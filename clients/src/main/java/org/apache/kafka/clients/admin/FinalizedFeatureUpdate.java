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

import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.message.UpdateFinalizedFeaturesRequestData;

/**
 * Encapsulates details about an update to a finalized feature. This is particularly useful to
 * define each feature update in the
 * {@link Admin#updateFinalizedFeatures(Set, UpdateFinalizedFeaturesOptions)} API request.
 */
public class FinalizedFeatureUpdate {
    private final String featureName;
    private final short maxVersionLevel;
    private final boolean allowDowngrade;

    /**
     * @param featureName       the name of the finalized feature to be updated.
     * @param maxVersionLevel   the new maximum version level for the finalized feature.
     *                          a value < 1 is special and indicates that the update is intended to
     *                          delete the finalized feature, and should be accompanied by setting
     *                          the allowDowngrade flag to true.
     * @param allowDowngrade    - true, if this feature update was meant to downgrade the existing
     *                            maximum version level of the finalized feature.
     *                          - false, otherwise.
     */
    public FinalizedFeatureUpdate(
        final String featureName, final short maxVersionLevel, final boolean allowDowngrade) {
        Objects.requireNonNull(featureName, "Provided feature name can not be null.");
        if (maxVersionLevel < 1 && !allowDowngrade) {
            throw new IllegalArgumentException(
                String.format(
                    "For featureName: %s, the allowDowngrade flag is not set when the" +
                    " provided maxVersionLevel:%d is < 1.", featureName, maxVersionLevel));
        }
        this.featureName = featureName;
        this.maxVersionLevel = maxVersionLevel;
        this.allowDowngrade = allowDowngrade;
    }

    /**
     * @return   the name of the finalized feature to be updated.
     */
    public String featureName() {
        return featureName;
    }

    /**
     * @return   the new maximum version level for the finalized feature.
     */
    public short maxVersionLevel() {
        return maxVersionLevel;
    }

    /**
     * @return   - true, if this feature update was meant to downgrade the maximum version level of
     *             the finalized feature.
     *           - false, otherwise.
     */
    public boolean allowDowngrade() {
        return allowDowngrade;
    }

    /**
     * Helper function that creates {@link UpdateFinalizedFeaturesRequestData} from a set of
     * {@link FinalizedFeatureUpdate}.
     *
     * @param updates   the set of {@link FinalizedFeatureUpdate}
     *
     * @return          a newly constructed UpdateFinalizedFeaturesRequestData object
     */
    public static UpdateFinalizedFeaturesRequestData createRequest(Set<FinalizedFeatureUpdate> updates) {
        final UpdateFinalizedFeaturesRequestData.FinalizedFeatureUpdateKeyCollection items
            = new UpdateFinalizedFeaturesRequestData.FinalizedFeatureUpdateKeyCollection();
        for (FinalizedFeatureUpdate update : updates) {
            final UpdateFinalizedFeaturesRequestData.FinalizedFeatureUpdateKey item =
                new UpdateFinalizedFeaturesRequestData.FinalizedFeatureUpdateKey();
            item.setName(update.featureName());
            item.setMaxVersionLevel(update.maxVersionLevel());
            item.setAllowDowngrade(update.allowDowngrade());
            items.add(item);
        }
        final UpdateFinalizedFeaturesRequestData data = new UpdateFinalizedFeaturesRequestData();
        data.setFinalizedFeatureUpdates(items);
        return data;
    }
}
