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

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;


public class FeatureControlManager {
    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private QuorumFeatures quorumFeatures = null;
        private KRaftVersionAccessor kraftVersionAccessor = null;

        private ClusterFeatureSupportDescriber clusterSupportDescriber = new ClusterFeatureSupportDescriber() {
            @Override
            public Iterator<Entry<Integer, Map<String, VersionRange>>> brokerSupported() {
                return Collections.emptyIterator();
            }

            @Override
            public Iterator<Entry<Integer, Map<String, VersionRange>>> controllerSupported() {
                return Collections.emptyIterator();
            }
        };

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setQuorumFeatures(QuorumFeatures quorumFeatures) {
            this.quorumFeatures = quorumFeatures;
            return this;
        }

        Builder setClusterFeatureSupportDescriber(ClusterFeatureSupportDescriber clusterSupportDescriber) {
            this.clusterSupportDescriber = clusterSupportDescriber;
            return this;
        }

        Builder setKRaftVersionAccessor(KRaftVersionAccessor kraftVersionAccessor) {
            this.kraftVersionAccessor = kraftVersionAccessor;
            return this;
        }

        public FeatureControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (quorumFeatures == null) {
                Map<String, VersionRange> localSupportedFeatures = new HashMap<>();
                localSupportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
                        MetadataVersion.MINIMUM_VERSION.featureLevel(),
                        MetadataVersion.latestProduction().featureLevel()));
                quorumFeatures = new QuorumFeatures(0, localSupportedFeatures, List.of(0));
            }
            if (kraftVersionAccessor == null) {
                kraftVersionAccessor = new KRaftVersionAccessor() {
                    private KRaftVersion version = KRaftVersion.LATEST_PRODUCTION;

                    @Override
                    public KRaftVersion kraftVersion() {
                        return version;
                    }

                    @Override
                    public void upgradeKRaftVersion(int epoch, KRaftVersion version, boolean validateOnly) {
                        if (!validateOnly) {
                            this.version = version;
                        }
                    }
                };
            }

            return new FeatureControlManager(
                logContext,
                quorumFeatures,
                snapshotRegistry,
                clusterSupportDescriber,
                kraftVersionAccessor
            );
        }
    }

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
    private final TimelineObject<Optional<MetadataVersion>> metadataVersion;

    /**
     * Gives information about the supported versions in the cluster.
     */
    private final ClusterFeatureSupportDescriber clusterSupportDescriber;

    /**
     * The interface for reading and upgrading the kraft version.
     */
    private final KRaftVersionAccessor kraftVersionAccessor;

    private FeatureControlManager(
        LogContext logContext,
        QuorumFeatures quorumFeatures,
        SnapshotRegistry snapshotRegistry,
        ClusterFeatureSupportDescriber clusterSupportDescriber,
        KRaftVersionAccessor kraftVersionAccessor
    ) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataVersion = new TimelineObject<>(snapshotRegistry, Optional.empty());
        this.clusterSupportDescriber = clusterSupportDescriber;
        this.kraftVersionAccessor = kraftVersionAccessor;
    }

    ControllerResult<ApiError> updateFeatures(
        Map<String, Short> updates,
        Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
        boolean validateOnly,
        int currentClaimedEpoch
    ) {
        List<ApiMessageAndVersion> records =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);

        Map<String, Short> proposedUpdatedVersions = new HashMap<>(finalizedVersions);
        proposedUpdatedVersions.put(MetadataVersion.FEATURE_NAME, metadataVersionOrThrow().featureLevel());
        proposedUpdatedVersions.putAll(updates);

        for (Entry<String, Short> entry : updates.entrySet()) {
            ApiError error = updateFeature(
                entry.getKey(),
                entry.getValue(),
                upgradeTypes.getOrDefault(entry.getKey(), FeatureUpdate.UpgradeType.UPGRADE),
                records,
                proposedUpdatedVersions,
                validateOnly,
                currentClaimedEpoch
            );
            if (!error.error().equals(Errors.NONE)) {
                return ControllerResult.of(List.of(), error);
            }
        }

        if (validateOnly) {
            return ControllerResult.of(List.of(), ApiError.NONE);
        } else {
            return ControllerResult.atomicOf(records, ApiError.NONE);
        }
    }

    Optional<MetadataVersion> metadataVersion() {
        return metadataVersion.get();
    }

    MetadataVersion metadataVersionOrThrow() {
        return metadataVersionOrThrow(SnapshotRegistry.LATEST_EPOCH);
    }

    private MetadataVersion metadataVersionOrThrow(long epoch) {
        return metadataVersion.get(epoch).orElseThrow(() ->
            new IllegalStateException("Unknown metadata version for FeatureControlManager"));
    }

    @SuppressWarnings({ "CyclomaticComplexity" })
    private ApiError updateFeature(
        String featureName,
        short newVersion,
        FeatureUpdate.UpgradeType upgradeType,
        List<ApiMessageAndVersion> records,
        Map<String, Short> proposedUpdatedVersions,
        boolean validateOnly,
        int currentClaimedEpoch
    ) {
        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return invalidUpdateVersion(featureName, newVersion,
                "The controller does not support the given upgrade type.");
        }

        final short currentVersion;
        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            currentVersion = metadataVersionOrThrow().featureLevel();
        } else if (featureName.equals(KRaftVersion.FEATURE_NAME)) {
            currentVersion = kraftVersionAccessor.kraftVersion().featureLevel();
        } else {
            currentVersion = finalizedVersions.getOrDefault(featureName, (short) 0);
        }

        if (newVersion < 0) {
            return invalidUpdateVersion(featureName, newVersion,
                "A feature version cannot be less than 0.");
        }

        Optional<String> reasonNotSupported = reasonNotSupported(featureName, newVersion);
        if (reasonNotSupported.isPresent()) {
            return invalidUpdateVersion(featureName, newVersion, reasonNotSupported.get());
        }

        if (newVersion < currentVersion) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return invalidUpdateVersion(featureName, newVersion,
                    "Can't downgrade the version of this feature without setting the " +
                    "upgrade type to either safe or unsafe downgrade.");
            }
        } else if (newVersion > currentVersion) {
            if (!upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return invalidUpdateVersion(featureName, newVersion, "Can't downgrade to a newer version.");
            }
        }

        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            // Perform additional checks if we're updating metadata.version
            return updateMetadataVersion(newVersion, upgradeType.equals(FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE), records::add);
        } else if (featureName.equals(KRaftVersion.FEATURE_NAME)) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                try {
                    kraftVersionAccessor.upgradeKRaftVersion(
                        currentClaimedEpoch,
                        KRaftVersion.fromFeatureLevel(newVersion),
                        validateOnly
                    );
                    /* Add the noop record so that there is at least one offset to wait on to
                     * complete the upgrade RPC
                     */
                    records.add(new ApiMessageAndVersion(new NoOpRecord(), (short) 0));
                    return ApiError.NONE;
                } catch (ApiException e) {
                    return ApiError.fromThrowable(e);
                } catch (IllegalArgumentException e) {
                    return invalidUpdateVersion(featureName, newVersion, e.getMessage());
                }
            } else if (newVersion != currentVersion) {
                return invalidUpdateVersion(
                    featureName,
                    newVersion,
                    "Can't downgrade the version of this feature."
                );
            } else {
                // Version didn't change
                return ApiError.NONE;
            }
        } else {
            // Validate dependencies for features that are not metadata.version
            try {
                Feature.validateVersion(
                    // Allow unstable feature versions is true because the version range is already checked above.
                    Feature.featureFromName(featureName).fromFeatureLevel(newVersion, true),
                    proposedUpdatedVersions);
            } catch (IllegalArgumentException e) {
                return invalidUpdateVersion(featureName, newVersion, e.getMessage());
            }
            records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(featureName).
                setFeatureLevel(newVersion), (short) 0));
            return ApiError.NONE;
        }
    }

    private Optional<String> reasonNotSupported(
        String featureName,
        short newVersion
    ) {
        int numBrokersChecked = 0;
        int numControllersChecked = 0;
        Optional<String> reason = quorumFeatures.reasonNotLocallySupported(featureName, newVersion);
        if (reason.isPresent()) return reason;
        numControllersChecked++;
        for (Iterator<Entry<Integer, Map<String, VersionRange>>> iter =
            clusterSupportDescriber.brokerSupported();
                iter.hasNext(); ) {
            Entry<Integer, Map<String, VersionRange>> entry = iter.next();
            reason = QuorumFeatures.reasonNotSupported(newVersion,
                    "Broker " + entry.getKey(),
                    entry.getValue().getOrDefault(featureName, QuorumFeatures.DISABLED));
            if (reason.isPresent()) return reason;
            numBrokersChecked++;
        }
        String registrationSuffix = "";
        HashSet<Integer> foundControllers = new HashSet<>();
        foundControllers.add(quorumFeatures.nodeId());
        if (metadataVersionOrThrow().isControllerRegistrationSupported()) {
            for (Iterator<Entry<Integer, Map<String, VersionRange>>> iter =
                 clusterSupportDescriber.controllerSupported();
                 iter.hasNext(); ) {
                Entry<Integer, Map<String, VersionRange>> entry = iter.next();
                if (entry.getKey() == quorumFeatures.nodeId()) {
                    // No need to re-check the features supported by this controller, since we
                    // already checked that above.
                    continue;
                }
                reason = QuorumFeatures.reasonNotSupported(newVersion,
                        "Controller " + entry.getKey(),
                        entry.getValue().getOrDefault(featureName, QuorumFeatures.DISABLED));
                if (reason.isPresent()) return reason;
                foundControllers.add(entry.getKey());
                numControllersChecked++;
            }
            for (int id : quorumFeatures.quorumNodeIds()) {
                if (!foundControllers.contains(id)) {
                    return Optional.of("controller " + id + " has not registered, and may not " +
                        "support this feature");
                }
            }
        } else {
            registrationSuffix = " Note: unable to verify controller support in the current " +
                "MetadataVersion.";
        }
        log.info("Verified that {} broker(s) and {} controller(s) supported changing {} to " +
                "feature level {}.{}", numBrokersChecked, numControllersChecked, featureName,
                newVersion, registrationSuffix);
        return Optional.empty();
    }

    private ApiError invalidUpdateVersion(String feature, short version, String message) {
        String errorMessage = String.format("Invalid update version %d for feature %s. %s", version, feature, message);
        log.warn(errorMessage);
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
        MetadataVersion currentVersion = metadataVersionOrThrow();
        final MetadataVersion newVersion;
        try {
            newVersion = MetadataVersion.fromFeatureLevel(newVersionLevel);
        } catch (IllegalArgumentException e) {
            return invalidMetadataVersion(newVersionLevel, "Valid versions are from "
                + MetadataVersion.MINIMUM_VERSION.featureLevel() + " to " + MetadataVersion.latestTesting().featureLevel() + ".");
        }

        if (newVersion.isLessThan(currentVersion)) {
            // This is a downgrade
            boolean metadataChanged = MetadataVersion.checkIfMetadataChanged(currentVersion, newVersion);
            if (!metadataChanged) {
                log.warn("Downgrading metadata.version from {} to {}.", currentVersion, newVersion);
            } else if (allowUnsafeDowngrade) {
                return unsupportedMetadataDowngrade(currentVersion, newVersion, 
                        "Unsafe metadata downgrade is not supported in this version.");
            } else {
                // The phrase "Retry using UNSAFE_DOWNGRADE if you want to force the downgrade to proceed." has been removed
                // because unsafe metadata downgrades are not yet supported. We can add it back when implemented (KAFKA-13896).
                return unsupportedMetadataDowngrade(currentVersion, newVersion,
                        "Refusing to perform the requested downgrade because it might delete metadata information.");
            }
        } else {
            log.warn("Upgrading metadata.version from {} to {}.", currentVersion, newVersion);
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(newVersionLevel), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));

        return ApiError.NONE;
    }

    private ApiError invalidMetadataVersion(short version, String message) {
        String errorMessage = String.format("Invalid metadata.version %d. %s", version, message);
        log.warn(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    private ApiError unsupportedMetadataDowngrade(MetadataVersion currentVersion, MetadataVersion targetVersion, String message) {
        String errorMessage = String.format("Unsupported metadata.version downgrade from %s to %s. %s", 
                currentVersion.featureLevel(), targetVersion.featureLevel(), message);
        log.warn(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    FinalizedControllerFeatures finalizedFeatures(long epoch) {
        Map<String, Short> features = new HashMap<>();
        features.put(MetadataVersion.FEATURE_NAME, metadataVersionOrThrow(epoch).featureLevel());
        for (Entry<String, Short> entry : finalizedVersions.entrySet(epoch)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, epoch);
    }

    public void replay(FeatureLevelRecord record) {
        VersionRange range = quorumFeatures.localSupportedFeature(record.name());
        if (!range.contains(record.featureLevel())) {
            throw new RuntimeException("Tried to apply FeatureLevelRecord " + record + ", but this controller only " +
                "supports versions " + range);
        }
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            MetadataVersion mv = MetadataVersion.fromFeatureLevel(record.featureLevel());
            metadataVersion.set(Optional.of(mv));
            log.info("Replayed a FeatureLevelRecord setting metadata.version to {}", mv);
        } else if (record.name().equals(KRaftVersion.FEATURE_NAME)) {
            // KAFKA-18979 - Skip any feature level record for kraft.version. This has two benefits:
            // 1. It removes from snapshots any FeatureLevelRecord for kraft.version that was incorrectly written to the log
            // 2. Allows ApiVersions to report the correct finalized kraft.version
        } else {
            if (record.featureLevel() == 0) {
                finalizedVersions.remove(record.name());
                log.info("Replayed a FeatureLevelRecord removing feature {}", record.name());
            } else {
                finalizedVersions.put(record.name(), record.featureLevel());
                log.info("Replayed a FeatureLevelRecord setting feature {} to {}",
                        record.name(), record.featureLevel());
            }
        }
    }

    boolean isControllerId(int nodeId) {
        return quorumFeatures.isControllerId(nodeId);
    }

    boolean isElrFeatureEnabled() {
        return finalizedVersions.getOrDefault(EligibleLeaderReplicasVersion.FEATURE_NAME, (short) 0) >=
            EligibleLeaderReplicasVersion.ELRV_1.featureLevel();
    }
}
