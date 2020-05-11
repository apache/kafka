package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class VersionRangeTest {
    @Test
    public void testFailDueToInvalidParams() {
        // min and max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(0, 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(-1, -1));
        // min can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(0, 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(-1, 1));
        // max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(1, 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(1, -1));
        // min can't be > max.
        assertThrows(
            IllegalArgumentException.class,
            () -> new VersionRange(2, 1));
    }

    @Test
    public void testSerializeDeserializeTest() {
        VersionRange versionRange = new VersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Long> serialized = versionRange.serialize();
        assertEquals(
            new HashMap<String, Long>() {
                {
                    put("min_version", versionRange.min());
                    put("max_version", versionRange.max());
                }
            },
            serialized
        );

        VersionRange deserialized = VersionRange.deserialize(serialized);
        assertEquals(1, deserialized.min());
        assertEquals(2, deserialized.max());
        assertEquals(versionRange, deserialized);
    }

    @Test
    public void testDeserializationFailureTest() {
        // min_version can't be < 1.
        Map<String, Long> invalidWithBadMinVersion = new HashMap<String, Long>() {
            {
                put("min_version", 0L);
                put("max_version", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithBadMinVersion));

        // max_version can't be < 1.
        Map<String, Long> invalidWithBadMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version", 1L);
                put("max_version", 0L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithBadMaxVersion));

        // min_version and max_version can't be < 1.
        Map<String, Long> invalidWithBadMinMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version", 0L);
                put("max_version", 0L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithBadMinMaxVersion));

        // min_version can't be > max_version.
        Map<String, Long> invalidWithLowerMaxVersion = new HashMap<String, Long>() {
            {
                put("min_version", 2L);
                put("max_version", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithLowerMaxVersion));

        // min_version key missing.
        Map<String, Long> invalidWithMinKeyMissing = new HashMap<String, Long>() {
            {
                put("max_version", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithMinKeyMissing));

        // max_version key missing.
        Map<String, Long> invalidWithMaxKeyMissing = new HashMap<String, Long>() {
            {
                put("min_version", 1L);
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> VersionRange.deserialize(invalidWithMaxKeyMissing));
    }

    @Test
    public void testToString() {
        assertEquals("VersionRange[1, 1]", new VersionRange(1, 1).toString());
        assertEquals("VersionRange[1, 2]", new VersionRange(1, 2).toString());
    }

    @Test
    public void testEquals() {
        assertTrue(new VersionRange(1, 1).equals(new VersionRange(1, 1)));
        assertFalse(new VersionRange(1, 1).equals(new VersionRange(1, 2)));
    }

    @Test
    public void testGetters() {
        assertEquals(1, new VersionRange(1, 2).min());
        assertEquals(2, new VersionRange(1, 2).max());
    }
}
