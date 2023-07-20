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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests MM2 replication with exactly-once support enabled on the Connect clusters.
 */
@Tag("integration")
public class MirrorConnectorsIntegrationExactlyOnceTest extends MirrorConnectorsIntegrationBaseTest {

    @BeforeEach
    public void startClusters() throws Exception {
        mm2Props.put(
                PRIMARY_CLUSTER_ALIAS + "." + DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG,
                DistributedConfig.ExactlyOnceSourceSupport.ENABLED.toString()
        );
        mm2Props.put(
                BACKUP_CLUSTER_ALIAS + "." + DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG,
                DistributedConfig.ExactlyOnceSourceSupport.ENABLED.toString()
        );
        for (Properties brokerProps : Arrays.asList(primaryBrokerProps, backupBrokerProps)) {
            brokerProps.put("transaction.state.log.replication.factor", "1");
            brokerProps.put("transaction.state.log.min.isr", "1");
        }
        super.startClusters();
    }

    @Override
    @Test
    public void testReplication() throws Exception {
        super.testReplication();

        // Augment the base replication test case with some extra testing of the offset management
        // API introduced in KIP-875
        // We do this only when exactly-once support is enabled in order to avoid having to worry about
        // zombie tasks producing duplicate records and/or writing stale offsets to the offsets topic

        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        String backupTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS);

        stopMirrorMakerConnectors(backup, MirrorSourceConnector.class);
        // Explicitly move back to offset 0
        // Note that the connector treats the offset as the last-consumed offset,
        // so it will start reading the topic partition from offset 1 when it resumes
        alterMirrorMakerSourceConnectorOffsets(backup, n -> 0L, "test-topic-1");
        // Reset the offsets for test-topic-2
        resetSomeMirrorMakerSourceConnectorOffsets(backup, "test-topic-2");
        resumeMirrorMakerConnectors(backup, MirrorSourceConnector.class);

        int expectedRecordsTopic1 = NUM_RECORDS_PRODUCED + ((NUM_RECORDS_PER_PARTITION - 1) * NUM_PARTITIONS);
        assertEquals(expectedRecordsTopic1, backup.kafka().consume(expectedRecordsTopic1, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
                "Records were not re-replicated to backup cluster after altering offsets.");
        int expectedRecordsTopic2 = NUM_RECORDS_PER_PARTITION * 2;
        assertEquals(expectedRecordsTopic2, backup.kafka().consume(expectedRecordsTopic2, RECORD_TRANSFER_DURATION_MS, backupTopic2).count(),
                "New topic was not re-replicated to backup cluster after altering offsets.");

        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends Connector>[] connectorsToReset = CONNECTOR_LIST.toArray(new Class[0]);
        stopMirrorMakerConnectors(backup, connectorsToReset);
        // Resetting the offsets for the heartbeat and checkpoint connectors doesn't have any effect
        // on their behavior, but users may want to wipe offsets from them to prevent the offsets topic
        // from growing infinitely. So, we include them in the list of connectors to reset as a sanity check
        // to make sure that this action can be performed successfully
        resetAllMirrorMakerConnectorOffsets(backup, connectorsToReset);
        resumeMirrorMakerConnectors(backup, connectorsToReset);

        expectedRecordsTopic1 += NUM_RECORDS_PRODUCED;
        assertEquals(expectedRecordsTopic1, backup.kafka().consume(expectedRecordsTopic1, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
                "Records were not re-replicated to backup cluster after resetting offsets.");
        expectedRecordsTopic2 += NUM_RECORDS_PER_PARTITION;
        assertEquals(expectedRecordsTopic2, backup.kafka().consume(expectedRecordsTopic2, RECORD_TRANSFER_DURATION_MS, backupTopic2).count(),
                "New topic was not re-replicated to backup cluster after resetting offsets.");
    }

}
