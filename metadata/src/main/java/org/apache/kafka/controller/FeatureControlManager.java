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
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.MetadataVersionProvider;
import org.apache.kafka.metadata.MetadataVersions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.metadata.FeatureMap;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


public class FeatureControlManager {
    /**
     * An immutable map containing the features supported by this controller's software.
     */
    private final Map<String, VersionRange> supportedFeatures;

    private final MetadataVersionProvider metadataVersionProvider;
    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, VersionRange> finalizedVersions;

    private final Map<String, FeatureLevelListener> listeners;

    FeatureControlManager(Map<String, VersionRange> supportedFeatures,
                          SnapshotRegistry snapshotRegistry,
                          MetadataVersionProvider metadataVersionProvider) {
        this.supportedFeatures = supportedFeatures;
        this.metadataVersionProvider = metadataVersionProvider;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.listeners = new HashMap<>();
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
            Map<String, Short> updates, Set<String> downgradeables,
            Map<Integer, Map<String, VersionRange>> brokerFeatures) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Entry<String, Short> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(),
                downgradeables.contains(entry.getKey()), brokerFeatures, records));
        }

        return ControllerResult.atomicOf(records, results);
    }

    ControllerResult<Map<String, ApiError>> initializeMetadataVersion(short initVersion) {
        if (finalizedVersions.containsKey(MetadataVersions.FEATURE_NAME)) {
            return ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    MetadataVersions.FEATURE_NAME,
                    new ApiError(Errors.INVALID_UPDATE_VERSION,
                        "Cannot initialize metadata.version since it has already been initialized.")
            ));
        }
        final VersionRange supportedRange = supportedFeatures.get(MetadataVersions.FEATURE_NAME);
        final VersionRange initRange = VersionRange.of(supportedRange.min(), initVersion);
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ApiError result = updateMetadataVersion(initRange, records::add);
        return ControllerResult.atomicOf(records, Collections.singletonMap(MetadataVersions.FEATURE_NAME, result));
    }

    void register(String listenerName, FeatureLevelListener listener) {
        this.listeners.putIfAbsent(listenerName, listener);
    }

    boolean canSupportVersion(String featureName, VersionRange versionRange) {
        VersionRange localRange = supportedFeatures.get(featureName);
        return localRange != null && localRange.contains(versionRange);
    }

    private ApiError updateFeature(String featureName,
                                   short newVersion,
                                   boolean downgradeable,
                                   Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
                                   List<ApiMessageAndVersion> records) {
        final VersionRange currentRange = finalizedVersions.get(featureName);
        final VersionRange newRange;
        if (currentRange == null) {
            // Never seen this feature before. Initialize its min version to the max of supported broker mins
            Optional<Short> minFinalizedVersion = brokersAndFeatures.values().stream().map(brokerFeatures -> {
                VersionRange brokerFeatureVersion = brokerFeatures.get(featureName);
                if (brokerFeatureVersion != null) {
                    return brokerFeatureVersion.min();
                } else {
                    return (short) -1;
                }
            }).max(Short::compareTo);

            if (minFinalizedVersion.isPresent()) {
                newRange = VersionRange.of(minFinalizedVersion.get(), newVersion);
            } else {
                // No brokers know about this feature!
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Cannot finalized this feature flag since no alive brokers support it.");
            }
        } else {
            newRange = VersionRange.of(currentRange.min(), newVersion);
        }

        if (newRange.max() < newRange.min()) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than the lower value.");
        }

        if (newRange.max() <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than 1.");
        }

        if (!canSupportVersion(featureName, newRange)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature range.");
        }

        for (Entry<Integer, Map<String, VersionRange>> brokerEntry : brokersAndFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null || !brokerRange.contains(newRange)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Broker " + brokerEntry.getKey() + " does not support the given " +
                        "feature range.");
            }
        }

        if (currentRange != null && currentRange.max() > newRange.max()) {
            if (!downgradeable) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without " +
                    "setting downgradable to true.");
            }
        }

        if (featureName.equals(MetadataVersions.FEATURE_NAME)) {
            return updateMetadataVersion(newRange, records::add);
        } else {
            records.add(new ApiMessageAndVersion(
                new FeatureLevelRecord().setName(featureName).
                    setMinFeatureLevel(newRange.min()).setMaxFeatureLevel(newRange.max()),
                metadataVersionProvider.activeVersion().recordVersion(FEATURE_LEVEL_RECORD)));
            return ApiError.NONE;
        }
    }

    /**
     * Perform some additional validation for metadata.version updates.
     */
    private ApiError updateMetadataVersion(VersionRange newRange, Consumer<ApiMessageAndVersion> recordConsumer) {
        if (newRange.max() < newRange.min()) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than the lower value.");
        }

        if (newRange.max() <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than 1.");
        }

        if (!canSupportVersion(MetadataVersions.FEATURE_NAME, newRange)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature range.");
        }

        VersionRange currentRange = finalizedVersions.get(MetadataVersions.FEATURE_NAME);
        if (currentRange != null && currentRange.max() > newRange.max()) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Downgrading metadata.version is not yet supported.");
        }

        try {
            MetadataVersions.of(newRange.max());
        } catch (IllegalArgumentException e) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Unknown metadata.version " + newRange.max());
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord().setName(MetadataVersions.FEATURE_NAME).
                setMinFeatureLevel(newRange.min()).setMaxFeatureLevel(newRange.max()),
            metadataVersionProvider.activeVersion().recordVersion(FEATURE_LEVEL_RECORD)));
        return ApiError.NONE;

    }

    FeatureMapAndEpoch finalizedFeatures(long lastCommittedOffset) {
        Map<String, VersionRange> features = new HashMap<>();
        for (Entry<String, VersionRange> entry : finalizedVersions.entrySet(lastCommittedOffset)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FeatureMapAndEpoch(new FeatureMap(features), lastCommittedOffset);
    }

    public void replay(FeatureLevelRecord record) {
        finalizedVersions.put(record.name(),
                VersionRange.of(record.minFeatureLevel(), record.maxFeatureLevel()));
        listeners.values().forEach(listener ->
            listener.handle(record.name(), record.minFeatureLevel(), record.maxFeatureLevel()));
    }

    class FeatureControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final Iterator<Entry<String, VersionRange>> iterator;

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
            Entry<String, VersionRange> entry = iterator.next();
            VersionRange versions = entry.getValue();
            return Collections.singletonList(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(entry.getKey()).
                setMinFeatureLevel(versions.min()).
                setMaxFeatureLevel(versions.max()), FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        }
    }

    FeatureControlIterator iterator(long epoch) {
        return new FeatureControlIterator(epoch);
    }
}
