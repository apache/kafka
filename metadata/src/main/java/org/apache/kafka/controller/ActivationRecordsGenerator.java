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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;


public class ActivationRecordsGenerator {

    static ControllerResult<Void> recordsForEmptyLog(
        Consumer<String> activationMessageConsumer,
        long transactionStartOffset,
        BootstrapMetadata bootstrapMetadata,
        MetadataVersion metadataVersion,
        int defaultMinInSyncReplicas
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

        // If ELR is enabled, we need to set a cluster-level min.insync.replicas.
        if (bootstrapMetadata.featureLevel(EligibleLeaderReplicasVersion.FEATURE_NAME) > 0) {
            records.add(new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(BROKER.id()).
                setResourceName("").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue(Integer.toString(defaultMinInSyncReplicas)), (short) 0));
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
        MetadataVersion curMetadataVersion
    ) {
        StringBuilder logMessageBuilder = new StringBuilder("Performing controller activation. ");

        // Logs have been replayed. We need to initialize some things here if upgrading from older KRaft versions
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check for in-flight transaction
        if (transactionStartOffset != -1L) {
            if (!curMetadataVersion.isMetadataTransactionSupported()) {
                throw new RuntimeException("Detected in-progress transaction at offset " + transactionStartOffset +
                    ", but the metadata.version " + curMetadataVersion +
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

        activationMessageConsumer.accept(logMessageBuilder.toString().trim());
        return ControllerResult.atomicOf(records, null);
    }

    /**
     * Generate the set of activation records.
     * </p>
     * If the log is empty, write the bootstrap records. If the log is not empty, do some validation and
     * possibly write some records to put the log into a valid state. For bootstrap records, if KIP-868
     * metadata transactions are supported, use them. Otherwise, write the bootstrap records as an
     * atomic batch. The single atomic batch can be problematic if the bootstrap records are too large
     * (e.g., lots of SCRAM credentials).
     */
    static ControllerResult<Void> generate(
        Consumer<String> activationMessageConsumer,
        boolean isEmpty,
        long transactionStartOffset,
        BootstrapMetadata bootstrapMetadata,
        MetadataVersion curMetadataVersion,
        int defaultMinInSyncReplicas
    ) {
        if (isEmpty) {
            return recordsForEmptyLog(activationMessageConsumer,
                    transactionStartOffset,
                    bootstrapMetadata,
                    bootstrapMetadata.metadataVersion(),
                    defaultMinInSyncReplicas);
        } else {
            return recordsForNonEmptyLog(activationMessageConsumer,
                    transactionStartOffset,
                    curMetadataVersion);
        }
    }
}
