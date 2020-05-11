package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionLevelRangeTest {

    @Test
    public void testCreateFailDueToInvalidParams() {
        // min and max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(0, 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(-1, -1));
        // min can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(0, 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(-1, 1));
        // max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(1, 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(1, -1));
        // min can't be > max.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionLevelRange(2, 1));
    }

    @Test
    public void testSerializeDeserialize() {
        VersionLevelRange versionLevelRange = new VersionLevelRange(1, 2);
        assertEquals(1, versionLevelRange.min());
        assertEquals(2, versionLevelRange.max());

        Map<String, Long> serialized = versionLevelRange.serialize();
        assertEquals(
            new HashMap<String, Long>() {
                {
                    put("min_version_level", versionLevelRange.min());
                    put("max_version_level", versionLevelRange.max());
                }
            },
            serialized
        );

        VersionLevelRange deserialized = VersionLevelRange.deserialize(serialized);
        assertEquals(1, deserialized.min());
        assertEquals(2, deserialized.max());
        assertEquals(versionLevelRange, deserialized);
    }

    @Test
    public void testDeserializationFailureTest() {
        // min_version_level can't be < 1.
        Map<String, Long> invalidWithBadMinVersion = new HashMap<String, Long>() {
            {
                put("min_version_level", 0L);
                put("max_version_level", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithBadMinVersion));

        // max_version_level can't be < 1.
        Map<String, Long> invalidWithBadMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version_level", 1L);
                put("max_version_level", 0L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithBadMaxVersion));

        // min_version_level and max_version_level can't be < 1.
        Map<String, Long> invalidWithBadMinMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version_level", 0L);
                put("max_version_level", 0L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithBadMinMaxVersion));

        // min_version_level can't be > max_version_level.
        Map<String, Long> invalidWithLowerMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version_level", 2L);
                put("max_version_level", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithLowerMaxVersion));

        // min_version_level key missing.
        Map<String, Long> invalidWithMinKeyMissing = new HashMap<String, Long>() {
            {
                put("max_version_level", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithMinKeyMissing));

        // max_version_level key missing.
        Map<String, Long> invalidWithMaxKeyMissing = new HashMap<String, Long>() {
            {
                put("min_version_level", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionLevelRange.deserialize(invalidWithMaxKeyMissing));
    }

    @Test
    public void testToString() {
        assertEquals("VersionLevelRange[1, 1]", new VersionLevelRange(1, 1).toString());
        assertEquals("VersionLevelRange[1, 2]", new VersionLevelRange(1, 2).toString());
    }

    @Test
    public void testEquals() {
        assertTrue(new VersionLevelRange(1, 1).equals(new VersionLevelRange(1, 1)));
        assertFalse(new VersionLevelRange(1, 1).equals(new VersionLevelRange(1, 2)));
    }

    @Test
    public void testIsCompatibleWith() {
        assertTrue(new VersionLevelRange(1, 1).isCompatibleWith(new VersionRange(1, 1)));
        assertTrue(new VersionLevelRange(2, 3).isCompatibleWith(new VersionRange(1, 4)));
        assertTrue(new VersionLevelRange(1, 4).isCompatibleWith(new VersionRange(1, 4)));

        assertFalse(new VersionLevelRange(1, 4).isCompatibleWith(new VersionRange(2, 3)));
        assertFalse(new VersionLevelRange(1, 4).isCompatibleWith(new VersionRange(2, 4)));
        assertFalse(new VersionLevelRange(2, 4).isCompatibleWith(new VersionRange(2, 3)));
    }

    @Test
    public void testGetters() {
        assertEquals(1, new VersionLevelRange(1, 2).min());
        assertEquals(2, new VersionLevelRange(1, 2).max());
    }
}