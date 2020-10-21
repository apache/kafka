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
 * An extended {@link BaseVersionRange} representing the min/max versions for a finalized feature.
 */
public class FinalizedVersionRange extends BaseVersionRange {
    // Label for the min version key, that's used only to convert to/from a map.
    private static final String MIN_VERSION_LEVEL_KEY_LABEL = "min_version_level";

    // Label for the max version key, that's used only to convert to/from a map.
    private static final String MAX_VERSION_LEVEL_KEY_LABEL = "max_version_level";

    public FinalizedVersionRange(short minVersionLevel, short maxVersionLevel) {
        super(MIN_VERSION_LEVEL_KEY_LABEL, minVersionLevel, MAX_VERSION_LEVEL_KEY_LABEL, maxVersionLevel);
    }

    public static FinalizedVersionRange fromMap(Map<String, Short> versionRangeMap) {
        return new FinalizedVersionRange(
            BaseVersionRange.valueOrThrow(MIN_VERSION_LEVEL_KEY_LABEL, versionRangeMap),
            BaseVersionRange.valueOrThrow(MAX_VERSION_LEVEL_KEY_LABEL, versionRangeMap));
    }

    /**
     * Checks if the [min, max] version level range of this object does *NOT* fall within the
     * [min, max] range of the provided SupportedVersionRange parameter.
     *
     * @param supportedVersionRange   the SupportedVersionRange to be checked
     *
     * @return                        - true, if the version levels are compatible
     *                                - false otherwise
     */
    public boolean isIncompatibleWith(SupportedVersionRange supportedVersionRange) {
        return min() < supportedVersionRange.min() || max() > supportedVersionRange.max();
    }
}
