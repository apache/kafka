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
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.FeatureLevelListener;
import org.apache.kafka.metadata.MetadataVersionProvider;
import org.apache.kafka.metadata.MetadataVersion;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


public class FeatureControlManager {
    public enum DowngradeType {
        SAFE, UNSAFE, NONE;
    }

    /**
     * An immutable map containing the features supported by this controller's software.
     */
    private final QuorumFeatures quorumFeatures;

    private final MetadataVersionProvider metadataVersionProvider;

    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, Short> finalizedVersions;

    /**
     * Collection of listeners for when features change
     */
    private final Map<String, FeatureLevelListener> listeners;

    FeatureControlManager(QuorumFeatures quorumFeatures,
                          SnapshotRegistry snapshotRegistry,
                          MetadataVersionProvider metadataVersionProvider) {
        this.quorumFeatures = quorumFeatures;
        this.metadataVersionProvider = metadataVersionProvider;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.listeners = new HashMap<>();
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
            Map<String, Short> updates,
            Map<String, DowngradeType> downgrades,
            Map<Integer, Map<String, VersionRange>> brokerFeatures,
            boolean validateOnly) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Entry<String, Short> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(),
                downgrades.getOrDefault(entry.getKey(), DowngradeType.NONE), brokerFeatures, records));
        }

        if (validateOnly) {
            return ControllerResult.of(Collections.emptyList(), results);
        } else {
            return ControllerResult.atomicOf(records, results);
        }
    }

    ControllerResult<Map<String, ApiError>> initializeMetadataVersion(short initVersion) {
        if (finalizedVersions.containsKey(MetadataVersion.FEATURE_NAME)) {
            return ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    MetadataVersion.FEATURE_NAME,
                    new ApiError(Errors.INVALID_UPDATE_VERSION,
                        "Cannot initialize metadata.version since it has already been initialized.")
            ));
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ApiError result = updateMetadataVersion(initVersion, initVersion, records::add);
        return ControllerResult.atomicOf(records, Collections.singletonMap(MetadataVersion.FEATURE_NAME, result));
    }

    void register(String listenerName, FeatureLevelListener listener) {
        this.listeners.putIfAbsent(listenerName, listener);
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
                                   DowngradeType downgradeType,
                                   Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
                                   List<ApiMessageAndVersion> records) {
        if (!featureExists(featureName)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given feature.");
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
            if (downgradeType.equals(DowngradeType.NONE)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without setting the downgrade type.");
            }
        }

        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            // Perform additional checks if we're updating metadata.version
            return updateMetadataVersion(newVersion, metadataVersionProvider.activeVersion().version(), records::add);
        } else {
            records.add(new ApiMessageAndVersion(
                new FeatureLevelRecord()
                    .setName(featureName)
                    .setFeatureLevel(newVersion),
                metadataVersionProvider.activeVersion().recordVersion(FEATURE_LEVEL_RECORD)));
            return ApiError.NONE;
        }
    }

    /**
     * Perform some additional validation for metadata.version updates.
     */
    private ApiError updateMetadataVersion(short newVersion,
                                           short recordVersion,
                                           Consumer<ApiMessageAndVersion> recordConsumer) {
        Optional<VersionRange> quorumSupported = quorumFeatures.quorumSupportedFeature(MetadataVersion.FEATURE_NAME);
        if (!quorumSupported.isPresent()) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The quorum can not support metadata.version!");
        }

        if (newVersion <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "metadata.version cannot be less than 1");
        }

        if (!quorumSupported.get().contains(newVersion)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller quorum cannot support the given metadata.version " + newVersion);
        }

        Short currentVersion = finalizedVersions.get(MetadataVersion.FEATURE_NAME);
        if (currentVersion != null && currentVersion > newVersion) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Downgrading metadata.version is not yet supported.");
        }

        try {
            MetadataVersion.fromValue(newVersion);
        } catch (IllegalArgumentException e) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Unknown metadata.version " + newVersion);
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(newVersion), recordVersion));
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
        finalizedVersions.put(record.name(), record.featureLevel());
        listeners.values().forEach(listener ->
            listener.handle(record.name(), record.featureLevel()));
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
