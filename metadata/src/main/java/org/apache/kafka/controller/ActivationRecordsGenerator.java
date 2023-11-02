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

import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ActivationRecordsGenerator {

    static ControllerResult<Void> recordsForEmptyLog(
        Consumer<String> activationMessageConsumer,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        BootstrapMetadata bootstrapMetadata,
        MetadataVersion metadataVersion
    ) {
        StringBuilder logMessageBuilder = new StringBuilder("Performing controller activation. ");
        List<ApiMessageAndVersion> records = new ArrayList<>();

        if (transactionStartOffset != -1L) {
            // In-flight bootstrap transaction
            if (!metadataVersion.isMetadataTransactionSupported()) {
                throw new RuntimeException("Detected partial bootstrap records transaction at " +
                    transactionStartOffset + ", but the metadata.version " + metadataVersion +
                    " does not support transactions. Cannot continue.");
            } else {
                logMessageBuilder
                    .append("Aborting partial bootstrap records transaction at offset ")
                    .append(transactionStartOffset)
                    .append(". Re-appending ")
                    .append(bootstrapMetadata.records().size())
                    .append(" bootstrap record(s) in new metadata transaction at metadata.version ")
                    .append(metadataVersion)
                    .append(" from bootstrap source '")
                    .append(bootstrapMetadata.source())
                    .append("'. ");
                records.add(new ApiMessageAndVersion(
                    new AbortTransactionRecord().setReason("Controller failover"), (short) 0));
                records.add(new ApiMessageAndVersion(
                    new BeginTransactionRecord().setName("Bootstrap records"), (short) 0));
            }
        } else {
            // No in-flight transaction
            logMessageBuilder
                .append("The metadata log appears to be empty. ")
                .append("Appending ")
                .append(bootstrapMetadata.records().size())
                .append(" bootstrap record(s) ");
            if (metadataVersion.isMetadataTransactionSupported()) {
                records.add(new ApiMessageAndVersion(
                    new BeginTransactionRecord().setName("Bootstrap records"), (short) 0));
                logMessageBuilder.append("in metadata transaction ");
            }
            logMessageBuilder
                .append("at metadata.version ")
                .append(metadataVersion)
                .append(" from bootstrap source '")
                .append(bootstrapMetadata.source())
                .append("'. ");
        }

        // If no records have been replayed, we need to write out the bootstrap records.
        // This will include the new metadata.version, as well as things like SCRAM
        // initialization, etc.
        records.addAll(bootstrapMetadata.records());

        if (metadataVersion.isMigrationSupported()) {
            if (zkMigrationEnabled) {
                logMessageBuilder.append("Putting the controller into pre-migration mode. No metadata updates " +
                    "will be allowed until the ZK metadata has been migrated. ");
                records.add(ZkMigrationState.PRE_MIGRATION.toRecord());
            } else {
                logMessageBuilder.append("Setting the ZK migration state to NONE since this is a de-novo " +
                    "KRaft cluster. ");
                records.add(ZkMigrationState.NONE.toRecord());
            }
        } else {
            if (zkMigrationEnabled) {
                throw new RuntimeException("The bootstrap metadata.version " + bootstrapMetadata.metadataVersion() +
                    " does not support ZK migrations. Cannot continue with ZK migrations enabled.");
            }
        }

        activationMessageConsumer.accept(logMessageBuilder.toString().trim());
        if (metadataVersion.isMetadataTransactionSupported()) {
            records.add(new ApiMessageAndVersion(new EndTransactionRecord(), (short) 0));
            return ControllerResult.of(records, null);
        } else {
            return ControllerResult.atomicOf(records, null);
        }
    }

    static ControllerResult<Void> recordsForNonEmptyLog(
        Consumer<String> activationMessageConsumer,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        FeatureControlManager featureControl,
        MetadataVersion metadataVersion
    ) {
        StringBuilder logMessageBuilder = new StringBuilder("Performing controller activation. ");

        // Logs have been replayed. We need to initialize some things here if upgrading from older KRaft versions
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check for in-flight transaction
        if (transactionStartOffset != -1L) {
            if (!metadataVersion.isMetadataTransactionSupported()) {
                throw new RuntimeException("Detected in-progress transaction at offset " + transactionStartOffset +
                    ", but the metadata.version " + metadataVersion +
                    " does not support transactions. Cannot continue.");
            } else {
                logMessageBuilder
                    .append("Aborting in-progress metadata transaction at offset ")
                    .append(transactionStartOffset)
                    .append(". ");
                records.add(new ApiMessageAndVersion(
                    new AbortTransactionRecord().setReason("Controller failover"), (short) 0));
            }
        }

        if (metadataVersion.equals(MetadataVersion.MINIMUM_KRAFT_VERSION)) {
            logMessageBuilder.append("No metadata.version feature level record was found in the log. ")
                .append("Treating the log as version ")
                .append(MetadataVersion.MINIMUM_KRAFT_VERSION)
                .append(". ");
        }

        if (zkMigrationEnabled && !metadataVersion.isMigrationSupported()) {
            throw new RuntimeException("Should not have ZK migrations enabled on a cluster running " +
                "metadata.version " + featureControl.metadataVersion());
        } else if (metadataVersion.isMigrationSupported()) {
            logMessageBuilder
                .append("Loaded ZK migration state of ")
                .append(featureControl.zkMigrationState())
                .append(". ");
            switch (featureControl.zkMigrationState()) {
                case NONE:
                    // Since this is the default state there may or may not be an actual NONE in the log. Regardless,
                    // it will eventually be persisted in a snapshot, so we don't need to explicitly write it here.
                    if (zkMigrationEnabled) {
                        throw new RuntimeException("Should not have ZK migrations enabled on a cluster that was " +
                            "created in KRaft mode.");
                    }
                    break;
                case PRE_MIGRATION:
                    if (!metadataVersion.isMetadataTransactionSupported()) {
                        logMessageBuilder
                            .append("Activating pre-migration controller without empty log. ")
                            .append("There may be a partial migration. ");
                    }
                    break;
                case MIGRATION:
                    if (!zkMigrationEnabled) {
                        // This can happen if controller leadership transfers to a controller with migrations enabled
                        // after another controller had finalized the migration. For example, during a rolling restart
                        // of the controller quorum during which the migration config is being set to false.
                        logMessageBuilder
                            .append("Completing the ZK migration since this controller was configured with ")
                            .append("'zookeeper.metadata.migration.enable' set to 'false'. ");
                        records.add(ZkMigrationState.POST_MIGRATION.toRecord());
                    } else {
                        // This log message is used in zookeeper_migration_test.py
                        logMessageBuilder
                            .append("Staying in ZK migration mode since 'zookeeper.metadata.migration.enable' ")
                            .append("is still 'true'. ");
                    }
                    break;
                case POST_MIGRATION:
                    if (zkMigrationEnabled) {
                        logMessageBuilder
                            .append("Ignoring 'zookeeper.metadata.migration.enable' value of 'true' since ")
                            .append("the ZK migration has been completed. ");
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported ZkMigrationState " + featureControl.zkMigrationState());
            }
        }

        activationMessageConsumer.accept(logMessageBuilder.toString().trim());
        return ControllerResult.atomicOf(records, null);
    }

    /**
     * Generate the set of activation records.
     * </p>
     * If the log is empty, write the bootstrap records. If the log is not empty, do some validation and
     * possibly write some records to put the log into a valid state. For bootstrap records, if KIP-868
     * metadata transactions are supported, ues them. Otherwise, write the bootstrap records as an
     * atomic batch. The single atomic batch can be problematic if the bootstrap records are too large
     * (e.g., lots of SCRAM credentials). If ZK migrations are enabled, the activation records will
     * include a ZkMigrationState record regardless of whether the log was empty or not.
     */
    static ControllerResult<Void> generate(
        Consumer<String> activationMessageConsumer,
        boolean isEmpty,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        BootstrapMetadata bootstrapMetadata,
        FeatureControlManager featureControl
    ) {
        if (isEmpty) {
            return recordsForEmptyLog(activationMessageConsumer, transactionStartOffset, zkMigrationEnabled,
                bootstrapMetadata, bootstrapMetadata.metadataVersion());
        } else {
            return recordsForNonEmptyLog(activationMessageConsumer, transactionStartOffset, zkMigrationEnabled,
                featureControl, featureControl.metadataVersion());
        }
    }
}
