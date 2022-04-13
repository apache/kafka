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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


public class FeatureControlManager {
    private final Logger log;

    /**
     * An immutable map containing the features supported by this controller's software.
     */
    private final QuorumFeatures quorumFeatures;

    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, Short> finalizedVersions;


    FeatureControlManager(LogContext logContext,
                          QuorumFeatures quorumFeatures,
                          SnapshotRegistry snapshotRegistry) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
            Map<String, Short> updates,
            Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
            Map<Integer, Map<String, VersionRange>> brokerFeatures,
            boolean validateOnly) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Entry<String, Short> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(),
                upgradeTypes.getOrDefault(entry.getKey(), FeatureUpdate.UpgradeType.UPGRADE), brokerFeatures, records));
        }

        if (validateOnly) {
            return ControllerResult.of(Collections.emptyList(), results);
        } else {
            return ControllerResult.atomicOf(records, results);
        }
    }

    boolean canSupportVersion(String featureName, short versionRange) {
        return quorumFeatures.localSupportedFeature(featureName)
            .filter(localRange -> localRange.contains(versionRange))
            .isPresent();
    }

    boolean featureExists(String featureName) {
        return quorumFeatures.localSupportedFeature(featureName).isPresent();
    }

    private ApiError updateFeature(String featureName,
                                   short newVersion,
                                   FeatureUpdate.UpgradeType upgradeType,
                                   Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
                                   List<ApiMessageAndVersion> records) {
        if (!featureExists(featureName)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given feature.");
        }

        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given upgrade type.");
        }

        final Short currentVersion = finalizedVersions.get(featureName);

        if (newVersion <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than 1.");
        }

        if (!canSupportVersion(featureName, newVersion)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature range.");
        }

        for (Entry<Integer, Map<String, VersionRange>> brokerEntry : brokersAndFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null || !brokerRange.contains(newVersion)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Broker " + brokerEntry.getKey() + " does not support the given " +
                        "feature range.");
            }
        }

        if (currentVersion != null && newVersion < currentVersion) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without setting the upgrade type to safe or unsafe downgrade.");
            }
        }

        records.add(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(featureName)
                .setFeatureLevel(newVersion),
            FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        return ApiError.NONE;
    }

    FinalizedControllerFeatures finalizedFeatures(long lastCommittedOffset) {
        Map<String, Short> features = new HashMap<>();
        for (Entry<String, Short> entry : finalizedVersions.entrySet(lastCommittedOffset)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, lastCommittedOffset);
    }

    public void replay(FeatureLevelRecord record) {
        log.info("Setting feature {} to {}", record.name(), record.featureLevel());
        finalizedVersions.put(record.name(), record.featureLevel());
    }

    class FeatureControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final Iterator<Entry<String, Short>> iterator;

        FeatureControlIterator(long epoch) {
            this.iterator = finalizedVersions.entrySet(epoch).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry<String, Short> entry = iterator.next();
            return Collections.singletonList(new ApiMessageAndVersion(new FeatureLevelRecord()
                .setName(entry.getKey())
                .setFeatureLevel(entry.getValue()), FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        }
    }

    FeatureControlIterator iterator(long epoch) {
        return new FeatureControlIterator(epoch);
    }
}
