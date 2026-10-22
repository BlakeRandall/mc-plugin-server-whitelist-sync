package dev.example.whitelistsync;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class WhitelistEntryTest {

    @Test
    void noArgumentConstructorInitializesDefaultValues() {
        WhitelistEntry entry = new WhitelistEntry();

        assertAll(
                () -> assertNull(entry.getUuid()),
                () -> assertFalse(entry.isActive()),
                () -> assertNull(entry.getUsername()),
                () -> assertNull(entry.getChangedBy()),
                () -> assertNull(entry.getChangeSource()),
                () -> assertNull(entry.getUpdatedAt()),
                () -> assertEquals(0L, entry.getRevision())
        );
    }

    @Test
    void gettersReturnValuesSetBySetters() {
        WhitelistEntry entry = new WhitelistEntry();
        Date updatedAt = new Date(1_700_000_000_000L);

        entry.setUuid("123e4567-e89b-12d3-a456-426614174000");
        entry.setActive(true);
        entry.setUsername("ExamplePlayer");
        entry.setChangedBy("server-1");
        entry.setChangeSource("local-change");
        entry.setUpdatedAt(updatedAt);
        entry.setRevision(42L);

        assertAll(
                () -> assertEquals(
                        "123e4567-e89b-12d3-a456-426614174000",
                        entry.getUuid()
                ),
                () -> assertTrue(entry.isActive()),
                () -> assertEquals("ExamplePlayer", entry.getUsername()),
                () -> assertEquals("server-1", entry.getChangedBy()),
                () -> assertEquals("local-change", entry.getChangeSource()),
                () -> assertEquals(updatedAt, entry.getUpdatedAt()),
                () -> assertEquals(42L, entry.getRevision())
        );
    }
}
