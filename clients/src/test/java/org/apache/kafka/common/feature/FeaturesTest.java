package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class FeaturesTest {

    @Test
    public void testEmptyFeatures() {
        Map<String, Map<String, Long>> emptyMap = new HashMap<>();

        Features<VersionLevelRange> emptyFinalizedFeatures = Features.emptyFinalizedFeatures();
        assertEquals(new HashMap<>(), emptyFinalizedFeatures.all());
        assertEquals(emptyMap, emptyFinalizedFeatures.serialize());
        assertEquals(emptyFinalizedFeatures, Features.deserializeFinalizedFeatures(emptyMap));

        Features<VersionRange> emptySupportedFeatures = Features.emptySupportedFeatures();
        assertEquals(new HashMap<>(), emptySupportedFeatures.all());
        assertEquals(
            new HashMap<String, HashMap<String, Long>>(),
            emptySupportedFeatures.serialize());
        assertEquals(emptySupportedFeatures, Features.deserializeSupportedFeatures(emptyMap));
    }

    @Test
    public void testAllAPI() {
        VersionRange v1 = new VersionRange(1, 2);
        VersionRange v2 = new VersionRange(3, 4);
        Map<String, VersionRange> allFeatures = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(allFeatures, features.all());
    }

    @Test
    public void testGetAPI() {
        VersionRange v1 = new VersionRange(1, 2);
        VersionRange v2 = new VersionRange(3, 4);
        Map<String, VersionRange> allFeatures = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionRange> features = Features.supportedFeatures(allFeatures);
        assertEquals(v1, features.get("feature_1"));
        assertEquals(v2, features.get("feature_2"));
        assertNull(features.get("nonexistent_feature"));
    }

    @Test
    public void testSerializeDeserializeSupportedFeatures() {
        VersionRange v1 = new VersionRange(1, 2);
        VersionRange v2 = new VersionRange(3, 4);
        Map<String, VersionRange> allFeatures = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionRange> features = Features.supportedFeatures(allFeatures);

        Map<String, Map<String, Long>> expected = new HashMap<String, Map<String, Long>>() {
            {
                put("feature_1", new HashMap<String, Long>() {
                    {
                        put("min_version", 1L);
                        put("max_version", 2L);
                    }
                });
                put("feature_2", new HashMap<String, Long>() {
                    {
                        put("min_version", 3L);
                        put("max_version", 4L);
                    }
                });
            }
        };
        assertEquals(expected, features.serialize());
        assertEquals(features, Features.deserializeSupportedFeatures(expected));
    }

    @Test
    public void testSerializeDeserializeFinalizedFeatures() {
        VersionLevelRange v1 = new VersionLevelRange(1, 2);
        VersionLevelRange v2 = new VersionLevelRange(3, 4);
        Map<String, VersionLevelRange> allFeatures = new HashMap<String, VersionLevelRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionLevelRange> features = Features.finalizedFeatures(allFeatures);

        Map<String, Map<String, Long>> expected = new HashMap<String, Map<String, Long>>() {
            {
                put("feature_1", new HashMap<String, Long>() {
                    {
                        put("min_version_level", 1L);
                        put("max_version_level", 2L);
                    }
                });
                put("feature_2", new HashMap<String, Long>() {
                    {
                        put("min_version_level", 3L);
                        put("max_version_level", 4L);
                    }
                });
            }
        };
        assertEquals(expected, features.serialize());
        assertEquals(features, Features.deserializeFinalizedFeatures(expected));
    }

    @Test
    public void testToStringFinalizedFeatures() {
        VersionLevelRange v1 = new VersionLevelRange(1, 2);
        VersionLevelRange v2 = new VersionLevelRange(3, 4);
        Map<String, VersionLevelRange> allFeatures = new HashMap<String, VersionLevelRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionLevelRange> features = Features.finalizedFeatures(allFeatures);

        assertEquals(
            "Features{(feature_2 -> VersionLevelRange[3, 4]), (feature_1 -> VersionLevelRange[1, 2])}",
            features.toString());
    }

    @Test
    public void testToStringSupportedFeatures() {
        VersionRange v1 = new VersionRange(1, 2);
        VersionRange v2 = new VersionRange(3, 4);
        Map<String, VersionRange> allFeatures = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v1);
                put("feature_2", v2);
            }
        };
        Features<VersionRange> features = Features.supportedFeatures(allFeatures);

        assertEquals(
            "Features{(feature_2 -> VersionRange[3, 4]), (feature_1 -> VersionRange[1, 2])}",
            features.toString());
    }

    @Test
    public void testDeserializationFailureSupportedFeatures() {
        Map<String, Map<String, Long>> invalidFeatures = new HashMap<String, Map<String, Long>>() {
            {
                // invalid feature with max_version key missing.
                put("feature_1", new HashMap<String, Long>() {
                    {
                        put("min_version", 1L);
                    }
                });
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.deserializeSupportedFeatures(invalidFeatures));
    }

    @Test
    public void testDeserializationFailureFinalizedFeatures() {
        Map<String, Map<String, Long>> invalidFeatures = new HashMap<String, Map<String, Long>>() {
            {
                // invalid feature with max_version_level key missing.
                put("feature_1", new HashMap<String, Long>() {
                    {
                        put("min_version_level", 1L);
                    }
                });
            }
        };
        assertThrows(
            IllegalArgumentException.class,
            () -> Features.deserializeFinalizedFeatures(invalidFeatures));
    }

    @Test
    public void testEquals() {
        VersionRange v1 = new VersionRange(1, 2);
        Map<String, VersionRange> allFeatures = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v1);
            }
        };
        Features<VersionRange> features = Features.supportedFeatures(allFeatures);
        Features<VersionRange> featuresClone = Features.supportedFeatures(allFeatures);
        assertTrue(features.equals(featuresClone));

        VersionRange v2 = new VersionRange(1, 3);
        Map<String, VersionRange> allFeaturesDifferent = new HashMap<String, VersionRange>() {
            {
                put("feature_1", v2);
            }
        };
        Features<VersionRange> featuresDifferent = Features.supportedFeatures(allFeaturesDifferent);
        assertFalse(features.equals(featuresDifferent));
    }
}
