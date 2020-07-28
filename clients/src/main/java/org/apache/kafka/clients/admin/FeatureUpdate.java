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
package org.apache.kafka.clients.admin;

import java.util.Map;

/**
 * Encapsulates details about an update to a finalized feature. This is particularly useful to
 * define each feature update in the {@link Admin#updateFeatures(Map, UpdateFeaturesOptions)} API.
 */
public class FeatureUpdate {
    private final short maxVersionLevel;
    private final boolean allowDowngrade;

    /**
     * @param maxVersionLevel   the new maximum version level for the finalized feature.
     *                          a value < 1 is special and indicates that the update is intended to
     *                          delete the finalized feature, and should be accompanied by setting
     *                          the allowDowngrade flag to true.
     * @param allowDowngrade    - true, if this feature update was meant to downgrade the existing
     *                            maximum version level of the finalized feature.
     *                          - false, otherwise.
     */
    public FeatureUpdate(final short maxVersionLevel, final boolean allowDowngrade) {
        if (maxVersionLevel < 1 && !allowDowngrade) {
            throw new IllegalArgumentException(String.format(
                "The allowDowngrade flag is not set when the provided maxVersionLevel:%d is < 1.",
                maxVersionLevel));
        }
        this.maxVersionLevel = maxVersionLevel;
        this.allowDowngrade = allowDowngrade;
    }

    public short maxVersionLevel() {
        return maxVersionLevel;
    }

    public boolean allowDowngrade() {
        return allowDowngrade;
    }
}