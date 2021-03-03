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

package org.apache.kafka.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.FeatureMap;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;


public class FeatureControlManager {
    /**
     * The features supported by this controller's software.
     */
    private final Map<String, VersionRange> supportedFeatures;

    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, VersionRange> finalizedVersions;

    /**
     * The latest feature epoch.
     */
    private final TimelineHashSet<Long> epoch;

    FeatureControlManager(Map<String, VersionRange> supportedFeatures,
                          SnapshotRegistry snapshotRegistry) {
        this.supportedFeatures = supportedFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.epoch = new TimelineHashSet<>(snapshotRegistry, 0);
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
            Map<String, VersionRange> updates, Set<String> downgradeables,
            Map<Integer, Map<String, VersionRange>> brokerFeatures) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Entry<String, VersionRange> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(),
                downgradeables.contains(entry.getKey()), brokerFeatures, records));
        }
        return new ControllerResult<>(records, results);
    }

    private ApiError updateFeature(String featureName,
                                   VersionRange newRange,
                                   boolean downgradeable,
                                   Map<Integer, Map<String, VersionRange>> brokerFeatures,
                                   List<ApiMessageAndVersion> records) {
        if (newRange.min() <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The lower value for the new range cannot be less than 1.");
        }
        if (newRange.max() <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than 1.");
        }
        VersionRange localRange = supportedFeatures.get(featureName);
        if (localRange == null || !localRange.contains(newRange)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature range.");
        }
        for (Entry<Integer, Map<String, VersionRange>> brokerEntry :
            brokerFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null || !brokerRange.contains(newRange)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Broker " + brokerEntry.getKey() + " does not support the given " +
                        "feature range.");
            }
        }
        VersionRange currentRange = finalizedVersions.get(featureName);
        if (currentRange != null && currentRange.max() > newRange.max()) {
            if (!downgradeable) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without " +
                    "setting downgradable to true.");
            }
        }
        records.add(new ApiMessageAndVersion(
            new FeatureLevelRecord().setName(featureName).
                setMinFeatureLevel(newRange.min()).setMaxFeatureLevel(newRange.max()),
            (short) 0));
        return ApiError.NONE;
    }

    FeatureMapAndEpoch finalizedFeatures(long lastCommittedOffset) {
        Map<String, VersionRange> features = new HashMap<>();
        for (Entry<String, VersionRange> entry : finalizedVersions.entrySet(lastCommittedOffset)) {
            features.put(entry.getKey(), entry.getValue());
        }
        long currentEpoch = -1;
        Iterator<Long> iterator = epoch.iterator(lastCommittedOffset);
        if (iterator.hasNext()) {
            currentEpoch = iterator.next();
        }
        return new FeatureMapAndEpoch(new FeatureMap(features), currentEpoch);
    }

    void replay(FeatureLevelRecord record, long offset) {
        finalizedVersions.put(record.name(),
            new VersionRange(record.minFeatureLevel(), record.maxFeatureLevel()));
        epoch.clear();
        epoch.add(offset);
    }
}
