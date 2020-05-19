package org.apache.kafka.common.feature;

import java.util.Map;

import org.junit.Test;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FinalizedVersionRangeTest {

    @Test
    public void testCreateFailDueToInvalidParams() {
        // min and max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new FinalizedVersionRange(0, 0));
        // min can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new FinalizedVersionRange(0, 1));
        // max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new FinalizedVersionRange(1, 0));
        // min can't be > max.
        assertThrows(
            IllegalArgumentException.class,
            () -> new FinalizedVersionRange(2, 1));
    }

    @Test
    public void testSerializeDeserialize() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Long> serialized = versionRange.serialize();
        assertEquals(
            mkMap(
                mkEntry("min_version_level", versionRange.min()),
                mkEntry("max_version_level", versionRange.max())),
            serialized
        );

        FinalizedVersionRange deserialized = FinalizedVersionRange.deserialize(serialized);
        assertEquals(1, deserialized.min());
        assertEquals(2, deserialized.max());
        assertEquals(versionRange, deserialized);
    }

    @Test
    public void testDeserializationFailureTest() {
        // min_version_level can't be < 1.
        Map<String, Long> invalidWithBadMinVersion =
            mkMap(mkEntry("min_version_level", 0L),
                mkEntry("max_version_level", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithBadMinVersion));

        // max_version_level can't be < 1.
        Map<String, Long> invalidWithBadMaxVersion =
            mkMap(mkEntry("min_version_level", 1L),
                mkEntry("max_version_level", 0L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithBadMaxVersion));

        // min_version_level and max_version_level can't be < 1.
        Map<String, Long> invalidWithBadMinMaxVersion =
            mkMap(mkEntry("min_version_level", 0L),
                mkEntry("max_version_level", 0L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithBadMinMaxVersion));

        // min_version_level can't be > max_version_level.
        Map<String, Long> invalidWithLowerMaxVersion =
            mkMap(mkEntry("min_version_level", 2L),
                mkEntry("max_version_level", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithLowerMaxVersion));

        // min_version_level key missing.
        Map<String, Long> invalidWithMinKeyMissing =
            mkMap(mkEntry("max_version_level", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithMinKeyMissing));

        // max_version_level key missing.
        Map<String, Long> invalidWithMaxKeyMissing =
            mkMap(mkEntry("min_version_level", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> FinalizedVersionRange.deserialize(invalidWithMaxKeyMissing));
    }

    @Test
    public void testToString() {
        assertEquals("FinalizedVersionRange[1, 1]", new FinalizedVersionRange(1, 1).toString());
        assertEquals("FinalizedVersionRange[1, 2]", new FinalizedVersionRange(1, 2).toString());
    }

    @Test
    public void testEquals() {
        assertTrue(new FinalizedVersionRange(1, 1).equals(new FinalizedVersionRange(1, 1)));
        assertFalse(new FinalizedVersionRange(1, 1).equals(new FinalizedVersionRange(1, 2)));
        assertFalse(new FinalizedVersionRange(1, 1).equals(null));
    }

    @Test
    public void testIsCompatibleWith() {
        assertFalse(new FinalizedVersionRange(1, 1).isIncompatibleWith(new SupportedVersionRange(1, 1)));
        assertFalse(new FinalizedVersionRange(2, 3).isIncompatibleWith(new SupportedVersionRange(1, 4)));
        assertFalse(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(1, 4)));

        assertTrue(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(2, 3)));
        assertTrue(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(2, 4)));
        assertTrue(new FinalizedVersionRange(2, 4).isIncompatibleWith(new SupportedVersionRange(2, 3)));
    }

    @Test
    public void testMinMax() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());
    }
}
