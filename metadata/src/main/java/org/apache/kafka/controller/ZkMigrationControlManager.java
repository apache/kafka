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
    private final TimelineObject<ZkMigrationState> zkMigrationState;

    private final boolean zkMigrationEnabled;

    private final Logger log;

    ZkMigrationControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        boolean zkMigrationEnabled
    ) {
        this.zkMigrationState = new TimelineObject<>(snapshotRegistry, ZkMigrationState.UNINITIALIZED);
        this.log = logContext.logger(ZkMigrationControlManager.class);
        this.zkMigrationEnabled = zkMigrationEnabled;
    }

    // Visible for testing
    ZkMigrationState zkMigrationState() {
        return zkMigrationState.get();
    }

    /**
     * Tests if the controller should be preventing metadata updates due to being in the PRE_MIGRATION
     * state. If the controller does not yet support migrations (before 3.4-IV0), then metadata updates
     * are allowed in any state. Once the controller has been upgraded to a version that supports
     * migrations, then this method checks if the controller is in the PRE_MIGRATION state.
     */
    boolean inPreMigrationMode(MetadataVersion metadataVersion) {
        if (metadataVersion.isMigrationSupported()) {
            return zkMigrationState.get() == ZkMigrationState.PRE_MIGRATION;
        } else {
            return false;
        }
    }

    /**
     * Optionally provides a ZkMigrationStateRecord to bootstrap into the metadata log. In the case of
     * upgrades, the log will not be empty and this will return a NONE state record. For an empty log,
     * this will return either a NONE or PRE_MIGRATION depending on the configuration and metadata.version.
     *
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
                recordConsumer.accept(new ApiMessageAndVersion(
                    new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.PRE_MIGRATION.value()),
                    ZkMigrationStateRecord.LOWEST_SUPPORTED_VERSION
                ));
            } else {
                log.debug("Writing ZkMigrationState of NONE to the log, since migrations are not enabled.");
                recordConsumer.accept(new ApiMessageAndVersion(
                    new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.NONE.value()),
                    ZkMigrationStateRecord.LOWEST_SUPPORTED_VERSION
                ));
            }
        } else {
            // non-empty log
            switch (zkMigrationState()) {
                case UNINITIALIZED:
                    // No ZkMigrationState record seen, put a NONE in the log
                    log.debug("Writing a ZkMigrationState of NONE to the log to indicate this cluster was not migrated from ZK.");
                    if (zkMigrationEnabled) {
                        log.error("Should not have ZK migrations enabled on a cluster that was created in KRaft mode");
                    }
                    recordConsumer.accept(new ApiMessageAndVersion(
                        new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.NONE.value()),
                        ZkMigrationStateRecord.LOWEST_SUPPORTED_VERSION
                    ));
                    break;
                case NONE:
                    // This is a non-migrated KRaft cluster
                    log.debug("Read a ZkMigrationState of NONE from the log indicating this cluster was never migrated from ZK.");
                    if (zkMigrationEnabled) {
                        log.error("Should not have ZK migrations enabled on a cluster that was created in KRaft mode");
                    }
                    break;
                case POST_MIGRATION:
                    // This is a migrated KRaft cluster
                    log.debug("Read a ZkMigrationState of POST_MIGRATION from the log indicating this cluster was previously migrated from ZK.");
                    break;
                case MIGRATION:
                    // This cluster is migrated, but still in dual-write mode
                    log.debug("Read a ZkMigrationState of MIGRATION from the log indicating this cluster is being migrated from ZK.");
                    break;
                case PRE_MIGRATION:
                default:
                    // Once we have support for metadata transactions, this failure case will only exist for
                    // errant ZkMigrationStateRecord that are encountered outside a transaction.
                    throw new IllegalStateException("Detected an in-progress migration during startup, cannot continue.");
            }
        }
    }

    void replay(ZkMigrationStateRecord record) {
        ZkMigrationState recordState = ZkMigrationState.of(record.zkMigrationState());
        // From uninitialized, only allow PRE_MIGRATION
        if (zkMigrationState.get().equals(ZkMigrationState.UNINITIALIZED)) {
            if (recordState.equals(ZkMigrationState.PRE_MIGRATION) || recordState.equals(ZkMigrationState.NONE)) {
                log.info("Initializing ZK migration state as {}", recordState);
                zkMigrationState.set(recordState);
            } else {
                throw new IllegalStateException("The first migration state seen can only be PRE_MIGRATION or NONE, not " + recordState.name());
            }
        } else {
            switch (zkMigrationState.get()) {
                case NONE:
                    throw new IllegalStateException("Cannot ever change migration state from NONE");
                case PRE_MIGRATION:
                    if (recordState.equals(ZkMigrationState.MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        zkMigrationState.set(recordState);
                    } else {
                        throw new IllegalStateException("Cannot change migration state from PRE_MIGRATION to " + recordState);
                    }
                    break;
                case MIGRATION:
                    if (recordState.equals(ZkMigrationState.POST_MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        zkMigrationState.set(recordState);
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
