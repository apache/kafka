/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 */
class BaseVersionRange {
    private final String minKeyLabel;

    private final long minValue;

    private final String maxKeyLabel;

    private final long maxValue;

    /**
     * Raises an exception unless the following condition is met:
     * minValue >= 1 and maxValue >= 1 and minValue <= maxValue < minValue.
     *
     * @param minKeyLabel   Label for the min version key, that's used only for
     *                      serialization/deserialization purposes.
     * @param minValue      The minimum version value.
     * @param maxKeyLabel   Label for the max version key, that's used only for
     *                      serialization/deserialization purposes.
     * @param maxValue      The maximum version value.
     *
     * @throws IllegalArgumentException   If any of the following conditions are true:
     *                                     - (minValue < 1) OR (maxValue < 1) OR (maxValue < minValue).
     *                                     - minKeyLabel is empty, OR, minKeyLabel is empty.
     */
    protected BaseVersionRange(String minKeyLabel, long minValue, String maxKeyLabel, long maxValue) {
        if (minValue < 1 || maxValue < 1 || maxValue < minValue) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minValue > 1, maxValue > 1 and maxValue >= minValue, but received" +
                    " minValue: %d, maxValue: %d", minValue, maxValue));
        }
        if (minKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected minKeyLabel to be non-empty.");
        }
        if (maxKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected maxKeyLabel to be non-empty.");
        }
        this.minKeyLabel = minKeyLabel;
        this.minValue = minValue;
        this.maxKeyLabel = maxKeyLabel;
        this.maxValue = maxValue;
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || !(other instanceof BaseVersionRange)) {
            return false;
        }

        final BaseVersionRange that = (BaseVersionRange) other;
        return Objects.equals(this.minKeyLabel, that.minKeyLabel) &&
            this.minValue == that.minValue &&
            Objects.equals(this.maxKeyLabel, that.maxKeyLabel) &&
            this.maxValue == that.maxValue;
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
