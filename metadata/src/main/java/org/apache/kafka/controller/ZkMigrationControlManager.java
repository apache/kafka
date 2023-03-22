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

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import java.util.function.Consumer;


public class ZkMigrationControlManager implements ZkMigrationBootstrap {

    private static class ZkMigrationControlState {
        private final ZkMigrationState zkMigrationState;
        private final boolean preMigrationSupported;

        private ZkMigrationControlState(ZkMigrationState zkMigrationState, boolean preMigrationSupported) {
            this.zkMigrationState = zkMigrationState;
            this.preMigrationSupported = preMigrationSupported;
        }

        public ZkMigrationState zkMigrationState() {
            return zkMigrationState;
        }

        public boolean preMigrationSupported() {
            return preMigrationSupported;
        }
    }

    private final TimelineObject<ZkMigrationControlState> migrationControlState;

    private final boolean zkMigrationEnabled;

    private final Logger log;

    ZkMigrationControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        boolean zkMigrationEnabled
    ) {
        this.migrationControlState = new TimelineObject<>(snapshotRegistry,
            new ZkMigrationControlState(ZkMigrationState.UNINITIALIZED, false));
        this.log = logContext.logger(ZkMigrationControlManager.class);
        this.zkMigrationEnabled = zkMigrationEnabled;
    }

    // Visible for testing
    ZkMigrationState zkMigrationState() {
        return migrationControlState.get().zkMigrationState();
    }

    /**
     * Tests if the controller should be preventing metadata updates due to being in the PRE_MIGRATION
     * state. If the controller does not yet support migrations (before 3.4-IV0), then metadata updates
     * are allowed in any state. Once the controller has been upgraded to a version that supports
     * migrations, then this method checks if the controller is in the PRE_MIGRATION state.
     */
    boolean inPreMigrationMode(MetadataVersion metadataVersion) {
        ZkMigrationControlState state = migrationControlState.get();
        if (metadataVersion.isMigrationSupported() && state.preMigrationSupported()) {
            return state.zkMigrationState() == ZkMigrationState.PRE_MIGRATION;
        } else {
            return false;
        }
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
     */
    @Override
    public void bootstrapInitialMigrationState(
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
            ZkMigrationControlState state = migrationControlState.get();
            switch (state.zkMigrationState()) {
                case UNINITIALIZED:
                    // No ZkMigrationState record has been seen yet
                    log.debug("{} did not read any ZkMigrationState. Writing a ZkMigrationState of NONE to the log to " +
                        "indicate this cluster was not migrated from ZK.", prefix);
                    if (zkMigrationEnabled) {
                        log.error("Should not have ZK migrations enabled on a cluster that was created in KRaft mode");
                    }
                    recordConsumer.accept(buildRecord(ZkMigrationState.NONE));
                    break;
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
    }

    private ApiMessageAndVersion buildRecord(ZkMigrationState state) {
        return new ApiMessageAndVersion(
            new ZkMigrationStateRecord()
                .setZkMigrationState(state.value())
                .setPreMigrationSupported(true),
            ZkMigrationStateRecord.HIGHEST_SUPPORTED_VERSION
        );
    }

    /**
     * The state changes we allow are:
     * <li>UNINITIALIZED -> ANY</li>
     * <li>PRE_MIGRATION -> MIGRATION</li>
     * <li>MIGRATION -> POST_MIGRATION</li>
     */
    void replay(ZkMigrationStateRecord record) {
        ZkMigrationState recordState = ZkMigrationState.of(record.zkMigrationState());
        ZkMigrationControlState currentState = migrationControlState.get();
        if (currentState.zkMigrationState().equals(ZkMigrationState.UNINITIALIZED)) {
            log.info("Initializing ZK migration state as {}", recordState);
            migrationControlState.set(new ZkMigrationControlState(recordState, record.preMigrationSupported()));
        } else {
            switch (currentState.zkMigrationState()) {
                case NONE:
                    throw new IllegalStateException("Cannot ever change migration state away from NONE");
                case PRE_MIGRATION:
                    if (recordState.equals(ZkMigrationState.MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        migrationControlState.set(new ZkMigrationControlState(recordState, record.preMigrationSupported()));
                    } else {
                        throw new IllegalStateException("Cannot change migration state from PRE_MIGRATION to " + recordState);
                    }
                    break;
                case MIGRATION:
                    if (recordState.equals(ZkMigrationState.POST_MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        migrationControlState.set(new ZkMigrationControlState(recordState, record.preMigrationSupported()));
                    } else {
                        throw new IllegalStateException("Cannot change migration state from MIGRATION to " + recordState);
                    }
                    break;
                case POST_MIGRATION:
                    throw new IllegalStateException("Cannot ever change migration state from POST_MIGRATION");
            }
        }

    }
}
