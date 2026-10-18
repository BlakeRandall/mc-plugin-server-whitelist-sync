package dev.example.whitelistsync;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;

/**
 * Shared whitelist sync. All Bukkit whitelist reads/writes are performed on the
 * main server thread. MongoDB operations run on a dedicated single worker
 * thread.
 */
public final class WhitelistSyncPlugin extends JavaPlugin {

    private final AtomicBoolean cycleRunning = new AtomicBoolean(false);
    private final AtomicLong lastSuccessfulSync = new AtomicLong(0L);

    private volatile Set<UUID> lastAppliedSharedSnapshot;
    private volatile String lastError = "Not connected yet";
    private volatile MongoCollection<Document> collection;

    private MongoClient mongoClient;
    private ExecutorService databaseExecutor;
    private BukkitTask pollTask;
    private String serverId;
    private int intervalSeconds;
    private boolean verbose;
    private String startupMode;
    private boolean indexesReady;
    private int mongoBatchSize;
    private int applyBatchSize;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        loadSettings();
        databaseExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "WhitelistSync-MongoDB");
            thread.setDaemon(true);
            return thread;
        });
        createMongoClient();
        startPolling();
        getLogger().info("WhitelistSync enabled; initial sync will run shortly.");
    }

    @Override
    public void onDisable() {
        if (pollTask != null) {
            pollTask.cancel();
        }
        if (databaseExecutor != null) {
            databaseExecutor.shutdownNow();
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private String loadOrCreateServerId() {
        String serverId = getConfig().getString("server-id", "").trim();

        if (!serverId.isEmpty()) {
            try {
                return UUID.fromString(serverId).toString();
            } catch (IllegalArgumentException exception) {
                getLogger().warning(
                        "Invalid server-id in config.yml; generating a new ID."
                );
            }
        }

        serverId = UUID.randomUUID().toString();

        getConfig().set("server-id", serverId);
        saveConfig();

        getLogger().info("Generated and saved server ID: " + serverId);

        return serverId;
    }

    private void loadSettings() {
        FileConfiguration config = getConfig();
        serverId = loadOrCreateServerId();
        intervalSeconds = Math.max(2, config.getInt("sync-interval-seconds", 5));
        verbose = config.getBoolean("verbose-logging", true);
        startupMode = config.getString("startup-mode", "merge").trim().toLowerCase();
        mongoBatchSize = Math.max(100, getConfig().getInt("mongo-batch-size", 1000));
        applyBatchSize = Math.max(25, getConfig().getInt("apply-batch-size", 200));
        if (!startupMode.equals("merge")) {
            getLogger().warning("Unknown startup-mode '" + startupMode + "'; using merge.");
            startupMode = "merge";
        }
    }

    private void createMongoClient() {
        try {
            String uri = getConfig().getString("mongodb.uri", "").trim();
            if (uri.isEmpty() || uri.contains("CHANGE_ME")) {
                throw new IllegalArgumentException("Set mongodb.uri in plugins/WhitelistSync/config.yml first.");
            }
            if (mongoClient != null) {
                mongoClient.close();
            }
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(uri))
                    .applyToSocketSettings(builder -> builder.connectTimeout(
                            Math.max(1000, getConfig().getInt("mongodb.connect-timeout-ms", 5000)),
                            TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(builder -> builder.serverSelectionTimeout(
                            Math.max(1000, getConfig().getInt("mongodb.server-selection-timeout-ms", 5000)),
                            TimeUnit.MILLISECONDS))
                    .build();
            mongoClient = MongoClients.create(settings);
            MongoDatabase database = mongoClient.getDatabase(
                    getConfig().getString("mongodb.database", "minecraft"));
            collection = database.getCollection(
                    getConfig().getString("mongodb.collection", "shared_whitelist"));
            indexesReady = false;
            lastError = null;
        } catch (Exception exception) {
            collection = null;
            lastError = exception.getMessage();
            getLogger().log(Level.SEVERE, "MongoDB configuration error: " + exception.getMessage());
        }
    }

    private void startPolling() {
        if (pollTask != null) {
            pollTask.cancel();
        }
        long periodTicks = Math.max(40L, intervalSeconds * 20L);
        pollTask = Bukkit.getScheduler().runTaskTimer(this, this::beginSync, 40L, periodTicks);
    }

    private void beginSync() {
        if (!cycleRunning.compareAndSet(false, true)) {
            return;
        }
        if (collection == null) {
            createMongoClient();
            if (collection == null) {
                cycleRunning.set(false);
                return;
            }
        }

        Set<UUID> localSnapshot = currentWhitelist();
        Map<UUID, String> localUsernames = new HashMap<>();
        for (OfflinePlayer player : Bukkit.getWhitelistedPlayers()) {
            localUsernames.put(
                    player.getUniqueId(),
                    player.getName() == null ? "" : player.getName()
            );
        }

        Set<UUID> baseline = lastAppliedSharedSnapshot == null
                ? null : new HashSet<>(lastAppliedSharedSnapshot);

        databaseExecutor.execute(() -> {
            try {
                MongoCollection<Document> coll = collection;
                if (coll == null) {
                    throw new IllegalStateException("MongoDB collection is unavailable");
                }
                if (!indexesReady) {
                    coll.createIndex(Indexes.ascending("uuid"),
                            new IndexOptions().unique(true).name("unique_uuid"));
                    coll.createIndex(Indexes.ascending("active"), new IndexOptions().name("active_lookup"));
                    indexesReady = true;
                }
                coll.find().limit(1).first();

                if (baseline == null && startupMode.equals("merge")) {
                    seedLocalWhitelist(coll, localSnapshot, localUsernames);
                } else if (baseline != null) {
                    Set<UUID> added = new HashSet<>(localSnapshot);
                    added.removeAll(baseline);

                    Set<UUID> removed = new HashSet<>(baseline);
                    removed.removeAll(localSnapshot);

                    for (UUID uuid : added) {
                        publish(coll, uuid, true, "local-change");
                    }

                    for (UUID uuid : removed) {
                        publish(coll, uuid, false, "local-change");
                    }
                }

                Set<UUID> sharedActive = fetchActiveUuids(coll);
                Bukkit.getScheduler().runTask(this, ()
                        -> applySharedWhitelist(sharedActive, () -> {
                            lastAppliedSharedSnapshot = new HashSet<>(sharedActive);
                            lastSuccessfulSync.set(System.currentTimeMillis());
                            lastError = null;
                            cycleRunning.set(false);
                        })
                );
            } catch (Exception exception) {
                lastError = exception.getMessage();
                getLogger().log(Level.WARNING,
                        "MongoDB sync failed; local whitelist was not reconciled. Will retry. Cause: "
                        + exception.getMessage());
                cycleRunning.set(false);
            }
        });
    }

    private void publish(MongoCollection<Document> coll, UUID uuid, boolean active, String source) {
        coll.updateOne(eq("uuid", uuid.toString()), combine(
                set("active", active),
                set("changedBy", serverId),
                set("changeSource", source),
                currentDate("updatedAt"),
                inc("revision", 1L)),
                new UpdateOptions().upsert(true));
        if (verbose) {
            getLogger().info("Published " + (active ? "ADD " : "REMOVE ") + uuid + " to shared whitelist.");
        }
    }

    private Set<UUID> fetchActiveUuids(
            MongoCollection<Document> coll
    ) {
        Set<UUID> active = new HashSet<>();

        for (Document document : coll.find(eq("active", true))
                .projection(
                        com.mongodb.client.model.Projections
                                .include("uuid")
                )
                .batchSize(mongoBatchSize)) {

            String value = document.getString("uuid");

            if (value == null) {
                continue;
            }

            try {
                active.add(UUID.fromString(value));
            } catch (IllegalArgumentException exception) {
                getLogger().warning(
                        "Ignoring malformed UUID in MongoDB: " + value
                );
            }
        }

        return active;
    }

    private Set<UUID> currentWhitelist() {
        Set<UUID> result = new HashSet<>();
        for (OfflinePlayer player : Bukkit.getWhitelistedPlayers()) {
            result.add(player.getUniqueId());
        }
        return result;
    }

    private void applySharedWhitelist(
            Set<UUID> shared,
            Runnable onSuccess
    ) {
        Set<UUID> local = currentWhitelist();

        List<UUID> toAdd = new ArrayList<>();
        List<UUID> toRemove = new ArrayList<>();

        for (UUID uuid : shared) {
            if (!local.contains(uuid)) {
                toAdd.add(uuid);
            }
        }

        for (UUID uuid : local) {
            if (!shared.contains(uuid)) {
                toRemove.add(uuid);
            }
        }

        List<UUID> operations = new ArrayList<>(
                toAdd.size() + toRemove.size()
        );

        operations.addAll(toAdd);
        operations.addAll(toRemove);

        applyBatch(operations, 0, toAdd.size(), onSuccess);

        getLogger().info(
                "Whitelist reconciliation queued: add="
                + toAdd.size() + ", remove=" + toRemove.size()
        );
    }

    private void applyBatch(
            List<UUID> operations,
            int offset,
            int addCount,
            Runnable onSuccess
    ) {
        try {
            int end = Math.min(
                    offset + applyBatchSize,
                    operations.size()
            );

            for (int i = offset; i < end; i++) {
                UUID uuid = operations.get(i);

                boolean shouldWhitelist = i < addCount;

                Bukkit.getOfflinePlayer(uuid)
                        .setWhitelisted(shouldWhitelist);
            }

            if (end < operations.size()) {
                Bukkit.getScheduler().runTask(
                        this,
                        () -> applyBatch(
                                operations,
                                end,
                                addCount,
                                onSuccess
                        )
                );
            } else {
                onSuccess.run();
            }
        } catch (Exception exception) {
            lastError = exception.getMessage();
            cycleRunning.set(false);

            getLogger().log(
                    Level.WARNING,
                    "Whitelist batch failed; next sync will retry.",
                    exception
            );
        }
    }

    private void seedLocalWhitelist(
            MongoCollection<Document> coll,
            Set<UUID> localSnapshot,
            Map<UUID, String> usernames
    ) {
        List<WriteModel<Document>> batch
                = new ArrayList<>(mongoBatchSize);

        for (UUID uuid : localSnapshot) {
            Document seed = new Document("uuid", uuid.toString())
                    .append("active", true)
                    .append("username", usernames.getOrDefault(uuid, ""))
                    .append("changedBy", serverId)
                    .append("changeSource", "startup-merge")
                    .append("updatedAt", new java.util.Date())
                    .append("revision", 1L);

            batch.add(new UpdateOneModel<>(
                    eq("uuid", uuid.toString()),
                    new Document("$setOnInsert", seed),
                    new UpdateOptions().upsert(true)
            ));

            if (batch.size() >= mongoBatchSize) {
                coll.bulkWrite(
                        batch,
                        new com.mongodb.client.model.BulkWriteOptions()
                                .ordered(false)
                );
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            coll.bulkWrite(
                    batch,
                    new com.mongodb.client.model.BulkWriteOptions()
                            .ordered(false)
            );
        }

        getLogger().info(
                "Seeded local whitelist candidates: " + localSnapshot.size()
        );
    }

    public String statusText() {
        long last = lastSuccessfulSync.get();
        String age = last == 0L ? "never" : ((System.currentTimeMillis() - last) / 1000L) + "s ago";
        return "last-success=" + age + ", interval=" + intervalSeconds + "s, "
                + (lastError == null ? "database=ok" : "database/error=" + lastError);
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!command.getName().equalsIgnoreCase("whitelistsync")) {
            return false;
        }

        if (!sender.hasPermission("whitelistsync.admin")) {
            sender.sendMessage("You do not have permission to use this command.");
            return true;
        }

        String subcommand = args.length == 0 ? "status" : args[0].toLowerCase();

        switch (subcommand) {
            case "status" -> {
                sender.sendMessage("WhitelistSync: " + statusText());
            }

            case "sync" -> {
                if (cycleRunning.get()) {
                    sender.sendMessage("WhitelistSync: sync already running.");
                } else {
                    Bukkit.getScheduler().runTask(this, this::beginSync);
                    sender.sendMessage("WhitelistSync: sync queued.");
                }
            }

            case "reload" -> {
                if (cycleRunning.get()) {
                    sender.sendMessage(
                            "WhitelistSync: wait for the current sync to finish before reloading."
                    );
                    return true;
                }

                reloadConfig();
                loadSettings();
                createMongoClient();
                startPolling();

                sender.sendMessage(
                        "WhitelistSync: configuration reloaded; database configuration valid: "
                        + (collection != null)
                );
            }

            default ->
                sender.sendMessage(
                        "Usage: /whitelistsync <sync|status|reload>"
                );
        }

        return true;
    }
}
