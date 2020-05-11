package org.apache.kafka.common.feature;

import java.util.Map;

/**
 * A specialization of VersionRange representing a range of version levels. The main specialization
 * is that the class uses different serialization keys for min/max attributes.
 *
 * NOTE: This is the backing class used to define the min/max version levels for finalized features.
 */
public class VersionLevelRange extends VersionRange {
    // Label for the min version key, that's used only for serialization/deserialization purposes.
    private static final String MIN_VERSION_LEVEL_KEY_LABEL = "min_version_level";

    // Label for the max version key, that's used only for serialization/deserialization purposes.
    private static final String MAX_VERSION_LEVEL_KEY_LABEL = "max_version_level";

    public VersionLevelRange(long minVersionLevel, long maxVersionLevel) {
        super(MIN_VERSION_LEVEL_KEY_LABEL, minVersionLevel, MAX_VERSION_LEVEL_KEY_LABEL, maxVersionLevel);
    }

    public static VersionLevelRange deserialize(Map<String, Long> serialized) {
        return new VersionLevelRange(
            VersionRange.valueOrThrow(MIN_VERSION_LEVEL_KEY_LABEL, serialized),
            VersionRange.valueOrThrow(MAX_VERSION_LEVEL_KEY_LABEL, serialized));
    }

    /**
     * Checks if the [min, max] version level range of this object falls within the [min, max]
     * version range of the provided parameter.
     *
     * @param versionRange   the version range to be checked
     *
     * @return               true, if the version levels are compatible and false otherwise
     */
    public boolean isCompatibleWith(VersionRange versionRange) {
        return min() >= versionRange.min() && max() <= versionRange.max();
    }
}
