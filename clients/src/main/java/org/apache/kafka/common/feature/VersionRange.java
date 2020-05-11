package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an immutable basic version range using 2 attributes: min and max of type long.
 * The min and max attributes are expected to be >= 1, and with max >= min.
 *
 * The class also provides API to serialize/deserialize the version range to/from a map.
 * The class allows for configurable labels for the min/max attributes, which can be specialized by
 * sub-classes (if needed).
 *
 * NOTE: This is the backing class used to define the min/max versions for supported features.
 */
public class VersionRange {
    // Label for the min version key, that's used only for serialization/deserialization purposes.
    private static final String MIN_VERSION_KEY_LABEL = "min_version";

    // Label for the max version key, that's used only for serialization/deserialization purposes.
    private static final String MAX_VERSION_KEY_LABEL = "max_version";

    private final String minKeyLabel;

    private final long minValue;

    private final String maxKeyLabel;

    private final long maxValue;

    protected VersionRange(String minKey, long minValue, String maxKeyLabel, long maxValue) {
        if (minValue < 1 || maxValue < 1 || maxValue < minValue) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minValue > 1, maxValue > 1 and maxValue >= minValue, but received" +
                    " minValue: %d, maxValue: %d", minValue, maxValue));
        }
        this.minKeyLabel = minKey;
        this.minValue = minValue;
        this.maxKeyLabel = maxKeyLabel;
        this.maxValue = maxValue;
    }

    public VersionRange(long minVersion, long maxVersion) {
        this(MIN_VERSION_KEY_LABEL, minVersion, MAX_VERSION_KEY_LABEL, maxVersion);
    }

    public long min() {
        return minValue;
    }

    public long max() {
        return maxValue;
    }

    public String toString() {
        return String.format("%s[%d, %d]", this.getClass().getSimpleName(), min(), max());
    }

    public Map<String, Long> serialize() {
        return new HashMap<String, Long>() {
            {
                put(minKeyLabel, min());
                put(maxKeyLabel, max());
            }
        };
    }

    public static VersionRange deserialize(Map<String, Long> serialized) {
        return new VersionRange(
            valueOrThrow(MIN_VERSION_KEY_LABEL, serialized),
            valueOrThrow(MAX_VERSION_KEY_LABEL, serialized));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof VersionRange)) {
            return false;
        }

        final VersionRange that = (VersionRange) other;
        return Objects.equals(this.minKeyLabel, that.minKeyLabel) &&
            Objects.equals(this.minValue, that.minValue) &&
            Objects.equals(this.maxKeyLabel, that.maxKeyLabel) &&
            Objects.equals(this.maxValue, that.maxValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minKeyLabel, minValue, maxKeyLabel, maxValue);
    }

    public static long valueOrThrow(String key, Map<String, Long> serialized) {
        final Long value = serialized.get(key);
        if (value == null) {
            throw new IllegalArgumentException(key + " absent in " + serialized);
        }
        return value;
    }
}
