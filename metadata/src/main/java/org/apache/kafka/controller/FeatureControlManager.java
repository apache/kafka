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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


public class FeatureControlManager {

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private QuorumFeatures quorumFeatures = null;
        private MetadataVersion metadataVersion = MetadataVersion.latest();
        private MetadataVersion minimumBootstrapVersion = MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

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

        Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        Builder setMinimumBootstrapVersion(MetadataVersion minimumBootstrapVersion) {
            this.minimumBootstrapVersion = minimumBootstrapVersion;
            return this;
        }

        public FeatureControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (quorumFeatures == null) {
                quorumFeatures = new QuorumFeatures(0, new ApiVersions(), QuorumFeatures.defaultFeatureMap(),
                        Collections.emptyList());
            }
            return new FeatureControlManager(
                logContext,
                quorumFeatures,
                snapshotRegistry,
                metadataVersion,
                minimumBootstrapVersion
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
    private final TimelineObject<MetadataVersion> metadataVersion;

    /**
     * The current ZK migration state
     */
    private final TimelineObject<ZkMigrationState> migrationControlState;

    /**
     * The minimum bootstrap version that we can't downgrade before.
     */
    private final MetadataVersion minimumBootstrapVersion;

    private FeatureControlManager(
        LogContext logContext,
        QuorumFeatures quorumFeatures,
        SnapshotRegistry snapshotRegistry,
        MetadataVersion metadataVersion,
        MetadataVersion minimumBootstrapVersion
    ) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataVersion = new TimelineObject<>(snapshotRegistry, metadataVersion);
        this.minimumBootstrapVersion = minimumBootstrapVersion;
        this.migrationControlState = new TimelineObject<>(snapshotRegistry, ZkMigrationState.NONE);
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

    MetadataVersion metadataVersion() {
        return metadataVersion.get();
    }

    ZkMigrationState zkMigrationState() {
        return migrationControlState.get();
    }

    private ApiError updateFeature(
        String featureName,
        short newVersion,
        FeatureUpdate.UpgradeType upgradeType,
        Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
        List<ApiMessageAndVersion> records
    ) {
        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return invalidUpdateVersion(featureName, newVersion,
                "The controller does not support the given upgrade type.");
        }

        final short currentVersion;
        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            currentVersion = metadataVersion.get().featureLevel();
        } else {
            currentVersion = finalizedVersions.getOrDefault(featureName, (short) 0);
        }

        if (newVersion < 0) {
            return invalidUpdateVersion(featureName, newVersion,
                "A feature version cannot be less than 0.");
        }

        Optional<String> reasonNotSupported = quorumFeatures.reasonNotSupported(featureName, newVersion);
        if (reasonNotSupported.isPresent()) {
            return invalidUpdateVersion(featureName, newVersion, reasonNotSupported.get());
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
        } else {
            records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(featureName).
                setFeatureLevel(newVersion), (short) 0));
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
        MetadataVersion currentVersion = metadataVersion();
        ZkMigrationState zkMigrationState = zkMigrationState();
        final MetadataVersion newVersion;
        try {
            newVersion = MetadataVersion.fromFeatureLevel(newVersionLevel);
        } catch (IllegalArgumentException e) {
            return invalidMetadataVersion(newVersionLevel, "Unknown metadata.version.");
        }

        // Don't allow metadata.version changes while we're migrating
        if (EnumSet.of(ZkMigrationState.PRE_MIGRATION, ZkMigrationState.MIGRATION).contains(zkMigrationState)) {
            return invalidMetadataVersion(newVersionLevel, "Unable to modify metadata.version while a " +
                "ZK migration is in progress.");
        }

        // We cannot set a version earlier than IBP_3_3_IV0, since that was the first version that contained
        // FeatureLevelRecord itself.
        if (newVersion.isLessThan(minimumBootstrapVersion)) {
            return invalidMetadataVersion(newVersionLevel, "Unable to set a metadata.version less than " +
                    minimumBootstrapVersion);
        }
        if (newVersion.isLessThan(currentVersion)) {
            // This is a downgrade
            boolean metadataChanged = MetadataVersion.checkIfMetadataChanged(currentVersion, newVersion);
            if (!metadataChanged) {
                log.info("Downgrading metadata.version from {} to {}.", currentVersion, newVersion);
            } else if (allowUnsafeDowngrade) {
                return invalidMetadataVersion(newVersionLevel, "Unsafe metadata downgrade is not supported " +
                        "in this version.");
            } else {
                return invalidMetadataVersion(newVersionLevel, "Refusing to perform the requested " +
                        "downgrade because it might delete metadata information. Retry using " +
                        "UNSAFE_DOWNGRADE if you want to force the downgrade to proceed.");
            }
        } else {
            log.info("Upgrading metadata.version from {} to {}.", currentVersion, newVersion);
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(newVersionLevel), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));

        // If we are moving to a version that supports migrations, we need to write the correct state into the log
        if (newVersion.isMigrationSupported()) {
            recordConsumer.accept(buildZkMigrationRecord(ZkMigrationState.NONE));
        }
        return ApiError.NONE;
    }

    private ApiError invalidMetadataVersion(short version, String message) {
        String errorMessage = String.format("Invalid metadata.version %d. %s", version, message);
        log.error(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    FinalizedControllerFeatures finalizedFeatures(long epoch) {
        Map<String, Short> features = new HashMap<>();
        features.put(MetadataVersion.FEATURE_NAME, metadataVersion.get(epoch).featureLevel());
        for (Entry<String, Short> entry : finalizedVersions.entrySet(epoch)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, epoch);
    }


    /**
     * Optionally provides a ZkMigrationStateRecord to bootstrap into the metadata log. In the case of
     * upgrades, the log will not be empty and this will return a NONE state record. For an empty log,
     * this will return either a NONE or PRE_MIGRATION depending on the configuration and metadata.version.
     * <p>
     * If the log is in PRE_MIGRATION, this will throw an error.
     *
     * @param metadataVersion       The current MetadataVersion of the log
     * @param isMetadataLogEmpty    True if the log is being initialized from empty
     * @param recordConsumer        A consumer for the ZkMigrationStateRecord

    public void generateZkMigrationRecord(
        MetadataVersion metadataVersion,
        boolean isMetadataLogEmpty,
        Consumer<ApiMessageAndVersion> recordConsumer
    ) {
        if (!metadataVersion.isMigrationSupported()) {
            return;
        }

        if (isMetadataLogEmpty) {
            // Initialize the log with a ZkMigrationState
            if (zkMigrationEnabled) {
                log.info("Writing ZkMigrationState of PRE_MIGRATION to the log, since migrations are enabled.");
                recordConsumer.accept(buildRecord(ZkMigrationState.PRE_MIGRATION));
            } else {
                log.debug("Writing ZkMigrationState of NONE to the log, since migrations are not enabled.");
                recordConsumer.accept(buildRecord(ZkMigrationState.NONE));
            }
        } else {
            // non-empty log
            String prefix = "During metadata log initialization,";
            ZkMigrationState state = migrationControlState.get();
            switch (state) {

                case NONE:
                    // This is a non-migrated KRaft cluster
                    log.debug("{} read a ZkMigrationState of NONE indicating this cluster was never migrated from ZK.", prefix);
                    if (zkMigrationEnabled) {
                        log.error("Should not have ZK migrations enabled on a cluster that was created in KRaft mode");
                    }
                    break;
                case POST_MIGRATION:
                    // This is a migrated KRaft cluster
                    log.debug("{} read a ZkMigrationState of POST_MIGRATION indicating this cluster was previously migrated from ZK.", prefix);
                    break;
                case MIGRATION:
                    // This cluster is migrated, but still in dual-write mode
                    if (zkMigrationEnabled) {
                        log.debug("{} read a ZkMigrationState of MIGRATION indicating this cluster is being migrated from ZK.", prefix);
                    } else {
                        throw new IllegalStateException(
                                prefix + " read a ZkMigrationState of MIGRATION indicating this cluster is being migrated " +
                                        "from ZK, but the controller does not have migrations enabled."
                        );
                    }
                    break;
                case PRE_MIGRATION:
                    if (!state.preMigrationSupported()) {
                        // Upgrade case from 3.4. The controller only wrote PRE_MIGRATION during migrations in that version,
                        // so this needs to complete that migration.
                        log.info("{} read a ZkMigrationState of PRE_MIGRATION from a ZK migration on Apache Kafka 3.4.", prefix);
                        if (zkMigrationEnabled) {
                            recordConsumer.accept(buildRecord(ZkMigrationState.MIGRATION));
                            log.info("Writing ZkMigrationState of MIGRATION since migration mode is still active on the controller.");
                        } else {
                            recordConsumer.accept(buildRecord(ZkMigrationState.MIGRATION));
                            recordConsumer.accept(buildRecord(ZkMigrationState.POST_MIGRATION));
                            log.info("Writing ZkMigrationState of POST_MIGRATION since migration mode is not active on the controller.");
                        }
                    } else {
                        log.error("{} read a ZkMigrationState of PRE_MIGRATION indicating this cluster failed during a ZK migration.", prefix);
                        throw new IllegalStateException("Detected an invalid migration state during startup, cannot continue.");
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported migration state " + state.zkMigrationState());
            }
        }
    }*/

    /**
     * Tests if the controller should be preventing metadata updates due to being in the PRE_MIGRATION
     * state. If the controller does not yet support migrations (before 3.4-IV0), then metadata updates
     * are allowed in any state. Once the controller has been upgraded to a version that supports
     * migrations, then this method checks if the controller is in the PRE_MIGRATION state.
     */
    boolean inPreMigrationMode(MetadataVersion metadataVersion) {
        ZkMigrationState state = migrationControlState.get();
        if (metadataVersion.isMigrationSupported()) {
            return state == ZkMigrationState.PRE_MIGRATION;
        } else {
            return false;
        }
    }

    static ApiMessageAndVersion buildZkMigrationRecord(ZkMigrationState state) {
        return new ApiMessageAndVersion(
            new ZkMigrationStateRecord().setZkMigrationState(state.value()),
            ZkMigrationStateRecord.HIGHEST_SUPPORTED_VERSION
        );
    }

    public void replay(FeatureLevelRecord record) {
        VersionRange range = quorumFeatures.localSupportedFeature(record.name());
        if (!range.contains(record.featureLevel())) {
            throw new RuntimeException("Tried to apply FeatureLevelRecord " + record + ", but this controller only " +
                "supports versions " + range);
        }
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            log.info("Setting metadata.version to {}", record.featureLevel());
            metadataVersion.set(MetadataVersion.fromFeatureLevel(record.featureLevel()));
        } else {
            if (record.featureLevel() == 0) {
                log.info("Removing feature {}", record.name());
                finalizedVersions.remove(record.name());
            } else {
                log.info("Setting feature {} to {}", record.name(), record.featureLevel());
                finalizedVersions.put(record.name(), record.featureLevel());
            }
        }
    }

    public void replay(ZkMigrationStateRecord record) {
        ZkMigrationState recordState = ZkMigrationState.of(record.zkMigrationState());
        ZkMigrationState currentState = migrationControlState.get();
        log.info("Transitioning ZK migration state from {} to {}", currentState, recordState);
        migrationControlState.set(recordState);
    }

    boolean isControllerId(int nodeId) {
        return quorumFeatures.isControllerId(nodeId);
    }
}
