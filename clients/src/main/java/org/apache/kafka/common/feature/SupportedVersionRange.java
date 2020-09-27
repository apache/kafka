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

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.utils.Utils;

/**
 * An extended {@link BaseVersionRange} representing the min, max and first active versions for a
 * supported feature:
 *  - minVersion: This is the minimum supported version for the feature.
 *  - maxVersion: This the maximum supported version for the feature.
 *  - firstActiveVersion: This is the first active version for the feature. Versions in the range
 *    [minVersion, firstActiveVersion - 1] are considered to be deprecated.
 */
public class SupportedVersionRange extends BaseVersionRange {
    // Label for the min version key, that's used only to convert to/from a map.
    private static final String MIN_VERSION_KEY_LABEL = "min_version";

    // Label for the max version key, that's used only to convert to/from a map.
    private static final String MAX_VERSION_KEY_LABEL = "max_version";

    // Label for the first active version key, that's used only to convert to/from a map.
    private static final String FIRST_ACTIVE_VERSION_KEY_LABEL = "first_active_version";

    private final short firstActiveVersionValue;

    public SupportedVersionRange(short minVersion, short firstActiveVersion, short maxVersion) {
        super(MIN_VERSION_KEY_LABEL, minVersion, MAX_VERSION_KEY_LABEL, maxVersion);
        if (firstActiveVersion < minVersion || firstActiveVersion > maxVersion) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected firstActiveVersion >= minVersion and" +
                    " firstActiveVersion <= maxVersion, but received" +
                    " minVersion:%d, firstActiveVersion:%d, maxVersion:%d",
                    minVersion,
                    firstActiveVersion,
                    maxVersion));
        }
        this.firstActiveVersionValue = firstActiveVersion;
    }

    public short firstActiveVersion() {
        return firstActiveVersionValue;
    }

    public Map<String, Short> toMap() {
        return Utils.mkMap(Utils.mkEntry(minKeyLabel(), min()),
                           Utils.mkEntry(FIRST_ACTIVE_VERSION_KEY_LABEL, firstActiveVersionValue),
                           Utils.mkEntry(maxKeyLabel(), max()));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (getClass() != other.getClass()) {
            return false;
        }

        final SupportedVersionRange that = (SupportedVersionRange) other;
        return super.equals(other) && this.firstActiveVersionValue == that.firstActiveVersionValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), firstActiveVersionValue);
    }

    public static SupportedVersionRange fromMap(Map<String, Short> versionRangeMap) {
        return new SupportedVersionRange(
            BaseVersionRange.valueOrThrow(MIN_VERSION_KEY_LABEL, versionRangeMap),
            BaseVersionRange.valueOrThrow(FIRST_ACTIVE_VERSION_KEY_LABEL, versionRangeMap),
            BaseVersionRange.valueOrThrow(MAX_VERSION_KEY_LABEL, versionRangeMap));
    }
}
