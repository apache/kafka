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

/**
 * A specialization of {@link BaseVersionRange} representing a range of version levels.
 * NOTE: This is the backing class used to define the min/max version levels for finalized features.
 */
public class FinalizedVersionRange extends BaseVersionRange {
    // Label for the min version key, that's used only for serialization/deserialization purposes.
    private static final String MIN_VERSION_LEVEL_KEY_LABEL = "min_version_level";

    // Label for the max version key, that's used only for serialization/deserialization purposes.
    private static final String MAX_VERSION_LEVEL_KEY_LABEL = "max_version_level";

    public FinalizedVersionRange(long minVersionLevel, long maxVersionLevel) {
        super(MIN_VERSION_LEVEL_KEY_LABEL, minVersionLevel, MAX_VERSION_LEVEL_KEY_LABEL, maxVersionLevel);
    }

    public static FinalizedVersionRange deserialize(Map<String, Long> serialized) {
        return new FinalizedVersionRange(
            BaseVersionRange.valueOrThrow(MIN_VERSION_LEVEL_KEY_LABEL, serialized),
            BaseVersionRange.valueOrThrow(MAX_VERSION_LEVEL_KEY_LABEL, serialized));
    }

    private boolean isCompatibleWith(BaseVersionRange versionRange) {
        return min() >= versionRange.min() && max() <= versionRange.max();
    }

    /**
     * Checks if the [min, max] version level range of this object does *NOT* fall within the
     * [min, max] version range of the provided SupportedVersionRange parameter.
     *
     * @param versionRange   the SupportedVersionRange to be checked
     *
     * @return               true, if the version levels are compatible and false otherwise
     */
    public boolean isIncompatibleWith(SupportedVersionRange versionRange) {
        return !isCompatibleWith(versionRange);
    }
}
