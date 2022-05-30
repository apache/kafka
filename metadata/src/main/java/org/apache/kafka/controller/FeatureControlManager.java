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

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;
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

    /**
     * The current metadata version
     */
    private final TimelineObject<MetadataVersion> metadataVersion;


    FeatureControlManager(LogContext logContext,
                          QuorumFeatures quorumFeatures,
                          SnapshotRegistry snapshotRegistry) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataVersion = new TimelineObject<>(snapshotRegistry, MetadataVersion.UNINITIALIZED);
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
        Map<String, Short> updates,
        Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
        Map<Integer, Map<String, VersionRange>> brokerFeatures,
        boolean validateOnly
    ) {
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

    ControllerResult<Map<String, ApiError>> initializeMetadataVersion(short initVersion) {
        if (!metadataVersion().equals(MetadataVersion.UNINITIALIZED)) {
            return ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    MetadataVersion.FEATURE_NAME,
                    new ApiError(Errors.INVALID_UPDATE_VERSION,
                        "Cannot initialize metadata.version to " + initVersion + " since it has already been " +
                            "initialized to " + metadataVersion().featureLevel() + ".")
            ));
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ApiError result = updateMetadataVersion(initVersion, false, records::add);
        return ControllerResult.atomicOf(records, Collections.singletonMap(MetadataVersion.FEATURE_NAME, result));
    }

    /**
     * Test if the quorum can support this feature and version
     */
    boolean canSupportVersion(String featureName, short version) {
        return quorumFeatures.quorumSupportedFeature(featureName)
            .filter(versionRange -> versionRange.contains(version))
            .isPresent();
    }

    boolean featureExists(String featureName) {
        return quorumFeatures.localSupportedFeature(featureName).isPresent();
    }

    MetadataVersion metadataVersion() {
        return metadataVersion.get();
    }

    private ApiError updateFeature(
        String featureName,
        short newVersion,
        FeatureUpdate.UpgradeType upgradeType,
        Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
        List<ApiMessageAndVersion> records
    ) {
        if (!featureExists(featureName)) {
            return invalidUpdateVersion(featureName, newVersion,
                "The controller does not support the given feature.");
        }

        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return invalidUpdateVersion(featureName, newVersion,
                "The controller does not support the given upgrade type.");
        }

        final Short currentVersion;
        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            currentVersion = metadataVersion.get().featureLevel();
        } else {
            currentVersion = finalizedVersions.get(featureName);
        }

        if (newVersion <= 0) {
            return invalidUpdateVersion(featureName, newVersion,
                "A feature version cannot be less than 1.");
        }

        if (!canSupportVersion(featureName, newVersion)) {
            return invalidUpdateVersion(featureName, newVersion,
                "The quorum does not support the given feature version.");
        }

        for (Entry<Integer, Map<String, VersionRange>> brokerEntry : brokersAndFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null) {
                return invalidUpdateVersion(featureName, newVersion,
                    "Broker " + brokerEntry.getKey() + " does not support this feature.");
            } else if (!brokerRange.contains(newVersion)) {
                return invalidUpdateVersion(featureName, newVersion,
                    "Broker " + brokerEntry.getKey() + " does not support the given " +
                    "version. It supports " + brokerRange.min() + " to " + brokerRange.max() + ".");
            }
        }

        if (currentVersion != null && newVersion < currentVersion) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return invalidUpdateVersion(featureName, newVersion,
                    "Can't downgrade the version of this feature without setting the " +
                    "upgrade type to either safe or unsafe downgrade.");
            }
        }

        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            // Perform additional checks if we're updating metadata.version
            return updateMetadataVersion(newVersion, upgradeType.equals(FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE), records::add);
        } else {
            records.add(new ApiMessageAndVersion(
                new FeatureLevelRecord()
                    .setName(featureName)
                    .setFeatureLevel(newVersion),
                FEATURE_LEVEL_RECORD.highestSupportedVersion()));
            return ApiError.NONE;
        }
    }

    private ApiError invalidUpdateVersion(String feature, short version, String message) {
        String errorMessage = String.format("Invalid update version %d for feature %s. %s", version, feature, message);
        log.debug(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    /**
     * Perform some additional validation for metadata.version updates.
     */
    private ApiError updateMetadataVersion(
        short newVersionLevel,
        boolean allowUnsafeDowngrade,
        Consumer<ApiMessageAndVersion> recordConsumer
    ) {
        Optional<VersionRange> quorumSupported = quorumFeatures.quorumSupportedFeature(MetadataVersion.FEATURE_NAME);
        if (!quorumSupported.isPresent()) {
            return invalidMetadataVersion(newVersionLevel, "The quorum does not support metadata.version.");
        }

        if (newVersionLevel <= 0) {
            return invalidMetadataVersion(newVersionLevel, "KRaft mode/the quorum does not support metadata.version values less than 1.");
        }

        if (!quorumSupported.get().contains(newVersionLevel)) {
            return invalidMetadataVersion(newVersionLevel, "The controller quorum does support this version.");
        }

        MetadataVersion currentVersion = metadataVersion();
        final MetadataVersion newVersion;
        try {
            newVersion = MetadataVersion.fromFeatureLevel(newVersionLevel);
        } catch (IllegalArgumentException e) {
            return invalidMetadataVersion(newVersionLevel, "Unknown metadata.version.");
        }

        if (!currentVersion.equals(MetadataVersion.UNINITIALIZED) && newVersion.isLessThan(currentVersion)) {
            // This is a downgrade
            boolean metadataChanged = MetadataVersion.checkIfMetadataChanged(currentVersion, newVersion);
            if (!metadataChanged) {
                log.info("Downgrading metadata.version from {} to {}.", currentVersion, newVersion);
            } else {
                return invalidMetadataVersion(newVersionLevel, "Unsafe metadata.version downgrades are not supported.");
            }
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(newVersionLevel), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));
        return ApiError.NONE;
    }

    private ApiError invalidMetadataVersion(short version, String message) {
        String errorMessage = String.format("Invalid metadata.version %d. %s", version, message);
        log.error(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    FinalizedControllerFeatures finalizedFeatures(long epoch) {
        Map<String, Short> features = new HashMap<>();
        if (!metadataVersion.get(epoch).equals(MetadataVersion.UNINITIALIZED)) {
            features.put(MetadataVersion.FEATURE_NAME, metadataVersion.get(epoch).featureLevel());
        }
        for (Entry<String, Short> entry : finalizedVersions.entrySet(epoch)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, epoch);
    }

    public void replay(FeatureLevelRecord record) {
        if (!canSupportVersion(record.name(), record.featureLevel())) {
            throw new RuntimeException("Controller cannot support feature " + record.name() +
                                       " at version " + record.featureLevel());
        }

        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            log.info("Setting metadata.version to {}", record.featureLevel());
            metadataVersion.set(MetadataVersion.fromFeatureLevel(record.featureLevel()));
        } else {
            log.info("Setting feature {} to {}", record.name(), record.featureLevel());
            finalizedVersions.put(record.name(), record.featureLevel());
        }
    }

    class FeatureControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final Iterator<Entry<String, Short>> iterator;
        private final MetadataVersion metadataVersion;
        private boolean wroteVersion = false;

        FeatureControlIterator(long epoch) {
            this.iterator = finalizedVersions.entrySet(epoch).iterator();
            this.metadataVersion = FeatureControlManager.this.metadataVersion.get(epoch);
            if (this.metadataVersion.equals(MetadataVersion.UNINITIALIZED)) {
                this.wroteVersion = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !wroteVersion || iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            // Write the metadata.version first
            if (!wroteVersion) {
                wroteVersion = true;
                return Collections.singletonList(new ApiMessageAndVersion(new FeatureLevelRecord()
                    .setName(MetadataVersion.FEATURE_NAME)
                    .setFeatureLevel(metadataVersion.featureLevel()), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));
            }
            // Then write the rest of the features
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
