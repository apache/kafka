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

import static java.util.stream.Collectors.joining;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.utils.Utils;

/**
 * Represents an immutable basic version range using 2 attributes: min and max, each of type short.
 * The min and max attributes need to satisfy 2 rules:
 *  - they are each expected to be >= 1, as we only consider positive version values to be valid.
 *  - max should be >= min.
 *
 * The class also provides API to convert the version range to a map.
 * The class allows for configurable labels for the min/max attributes, which can be specialized by
 * sub-classes (if needed).
 */
class BaseVersionRange {
    // Non-empty label for the min version key, that's used only to convert to/from a map.
    private final String minKeyLabel;

    // The value of the minimum version.
    private final short minValue;

    // Non-empty label for the max version key, that's used only to convert to/from a map.
    private final String maxKeyLabel;

    // The value of the maximum version.
    private final short maxValue;

    /**
     * Raises an exception unless the following condition is met:
     * minValue >= 1 and maxValue >= 1 and maxValue >= minValue.
     *
     * @param minKeyLabel   Label for the min version key, that's used only to convert to/from a map.
     * @param minValue      The minimum version value.
     * @param maxKeyLabel   Label for the max version key, that's used only to convert to/from a map.
     * @param maxValue      The maximum version value.
     *
     * @throws IllegalArgumentException   If any of the following conditions are true:
     *                                     - (minValue < 1) OR (maxValue < 1) OR (maxValue < minValue).
     *                                     - minKeyLabel is empty, OR, minKeyLabel is empty.
     */
    protected BaseVersionRange(String minKeyLabel, short minValue, String maxKeyLabel, short maxValue) {
        if (minValue < 1 || maxValue < 1 || maxValue < minValue) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minValue >= 1, maxValue >= 1 and maxValue >= minValue, but received" +
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

    public short min() {
        return minValue;
    }

    public short max() {
        return maxValue;
    }

    public String toString() {
        return String.format(
            "%s[%s]",
            this.getClass().getSimpleName(),
            mapToString(toMap()));
    }

    public Map<String, Short> toMap() {
        return Utils.mkMap(Utils.mkEntry(minKeyLabel, min()), Utils.mkEntry(maxKeyLabel, max()));
    }

    private static String mapToString(final Map<String, Short> map) {
        return map
            .entrySet()
            .stream()
            .map(entry -> String.format("%s:%d", entry.getKey(), entry.getValue()))
            .collect(joining(", "));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
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

    public static short valueOrThrow(String key, Map<String, Short> versionRangeMap) {
        final Short value = versionRangeMap.get(key);
        if (value == null) {
            throw new IllegalArgumentException(String.format("%s absent in [%s]", key, mapToString(versionRangeMap)));
        }
        return value;
    }
}
