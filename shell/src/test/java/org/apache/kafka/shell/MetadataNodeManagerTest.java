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

package org.apache.kafka.shell;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.PartitionRecordJsonConverter;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


public class MetadataNodeManagerTest {

    private MetadataNodeManager metadataNodeManager;

    @BeforeEach
    public void setup() throws Exception {
        metadataNodeManager = new MetadataNodeManager();
        metadataNodeManager.setup();
    }

    @AfterEach
    public void cleanup() throws Exception {
        metadataNodeManager.close();
    }

    @Test
    public void testRegisterBrokerRecordAndUnregisterBrokerRecord() {
        // Register broker
        RegisterBrokerRecord record = new RegisterBrokerRecord()
            .setBrokerId(1)
            .setBrokerEpoch(2);
        metadataNodeManager.handleMessage(record);

        assertEquals(record.toString(),
            metadataNodeManager.getData().root().directory("brokers", "1").file("registration").contents());
        assertEquals("true",
            metadataNodeManager.getData().root().directory("brokers", "1").file("isFenced").contents());

        // Unregister broker
        UnregisterBrokerRecord unregisterBrokerRecord = new UnregisterBrokerRecord()
            .setBrokerId(1);
        metadataNodeManager.handleMessage(unregisterBrokerRecord);
        assertFalse(metadataNodeManager.getData().root().directory("brokers").children().containsKey("1"));
    }

    @Test
    public void testTopicRecordAndRemoveTopicRecord() {
        // Add topic
        TopicRecord topicRecord = new TopicRecord()
            .setName("topicName")
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"));

        metadataNodeManager.handleMessage(topicRecord);

        assertEquals("topicName",
            metadataNodeManager.getData().root().directory("topics", "topicName").file("name").contents());
        assertEquals("GcaQDl2UTsCNs1p9s37XkQ",
            metadataNodeManager.getData().root().directory("topics", "topicName").file("id").contents());
        assertEquals("topicName",
            metadataNodeManager.getData().root().directory("topicIds", "GcaQDl2UTsCNs1p9s37XkQ").file("name").contents());
        assertEquals("GcaQDl2UTsCNs1p9s37XkQ",
            metadataNodeManager.getData().root().directory("topicIds", "GcaQDl2UTsCNs1p9s37XkQ").file("id").contents());

        // Remove topic
        RemoveTopicRecord removeTopicRecord = new RemoveTopicRecord()
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"));

        metadataNodeManager.handleMessage(removeTopicRecord);

        assertFalse(
            metadataNodeManager.getData().root().directory("topicIds").children().containsKey("GcaQDl2UTsCNs1p9s37XkQ"));
        assertFalse(
            metadataNodeManager.getData().root().directory("topics").children().containsKey("topicName"));
    }

    @Test
    public void testPartitionRecord() {
        PartitionRecord record = new PartitionRecord()
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"))
            .setPartitionId(0)
            .setLeaderEpoch(1)
            .setReplicas(Arrays.asList(1, 2, 3))
            .setIsr(Arrays.asList(1, 2, 3));

        metadataNodeManager.handleMessage(record);
        assertEquals(
            PartitionRecordJsonConverter.write(record, PartitionRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(),
            metadataNodeManager.getData().root().directory("topicIds", "GcaQDl2UTsCNs1p9s37XkQ", "0").file("data").contents());
    }

    @Test
    public void testValidConfigRecord() {
        checkValidConfigRecord(ConfigResource.Type.BROKER.id(), "broker");
        checkValidConfigRecord(ConfigResource.Type.TOPIC.id(), "topic");
    }

    private void checkValidConfigRecord(byte resourceType, String typeString) {
        ConfigRecord configRecord = new ConfigRecord()
            .setResourceType(resourceType)
            .setResourceName("0")
            .setName("name")
            .setValue("kraft");

        metadataNodeManager.handleMessage(configRecord);
        assertEquals("kraft",
            metadataNodeManager.getData().root().directory("configs", typeString, "0").file("name").contents());

        // null value indicates delete
        configRecord.setValue(null);
        metadataNodeManager.handleMessage(configRecord);
        assertFalse(
            metadataNodeManager.getData().root().directory("configs", typeString, "0").children().containsKey("name"));
    }

    @Test
    public void testInvalidConfigRecord() {
        checkInvalidConfigRecord(ConfigResource.Type.BROKER_LOGGER.id());
        checkInvalidConfigRecord(ConfigResource.Type.UNKNOWN.id());
    }

    private void checkInvalidConfigRecord(byte resourceType) {
        ConfigRecord configRecord = new ConfigRecord()
            .setResourceType(resourceType)
            .setResourceName("0")
            .setName("name")
            .setValue("kraft");
        metadataNodeManager.handleMessage(configRecord);
        assertFalse(metadataNodeManager.getData().root().children().containsKey("configs"));
    }

    @Test
    public void testPartitionChangeRecord() {
        PartitionRecord oldPartitionRecord = new PartitionRecord()
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"))
            .setPartitionId(0)
            .setPartitionEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(Arrays.asList(0, 1, 2))
            .setReplicas(Arrays.asList(0, 1, 2));

        PartitionChangeRecord partitionChangeRecord = new PartitionChangeRecord()
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"))
            .setPartitionId(0)
            .setLeader(NO_LEADER_CHANGE)
            .setReplicas(Arrays.asList(0, 1, 2));

        PartitionRecord newPartitionRecord = new PartitionRecord()
            .setTopicId(Uuid.fromString("GcaQDl2UTsCNs1p9s37XkQ"))
            .setPartitionId(0)
            .setPartitionEpoch(1)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(Arrays.asList(0, 1, 2))
            .setReplicas(Arrays.asList(0, 1, 2));

        // Change nothing
        checkPartitionChangeRecord(
            oldPartitionRecord,
            partitionChangeRecord,
            newPartitionRecord
        );

        // Change isr
        checkPartitionChangeRecord(
            oldPartitionRecord,
            partitionChangeRecord.duplicate().setIsr(Arrays.asList(0, 2)),
            newPartitionRecord.duplicate().setIsr(Arrays.asList(0, 2))
        );

        // Change leader
        checkPartitionChangeRecord(
            oldPartitionRecord,
            partitionChangeRecord.duplicate().setLeader(1),
            newPartitionRecord.duplicate().setLeader(1).setLeaderEpoch(1)
        );
    }

    private void checkPartitionChangeRecord(PartitionRecord oldPartitionRecord,
                                           PartitionChangeRecord partitionChangeRecord,
                                           PartitionRecord newPartitionRecord) {
        metadataNodeManager.handleMessage(oldPartitionRecord);
        metadataNodeManager.handleMessage(partitionChangeRecord);
        assertEquals(
            PartitionRecordJsonConverter.write(newPartitionRecord, PartitionRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(),
            metadataNodeManager.getData().root()
                .directory("topicIds", oldPartitionRecord.topicId().toString(), oldPartitionRecord.partitionId() + "")
                .file("data").contents()
        );
    }

    @Test
    public void testUnfenceBrokerRecordAndFenceBrokerRecord() {
        RegisterBrokerRecord record = new RegisterBrokerRecord()
            .setBrokerId(1)
            .setBrokerEpoch(2);
        metadataNodeManager.handleMessage(record);

        assertEquals("true",
            metadataNodeManager.getData().root().directory("brokers", "1").file("isFenced").contents());

        UnfenceBrokerRecord unfenceBrokerRecord = new UnfenceBrokerRecord()
            .setId(1)
            .setEpoch(2);
        metadataNodeManager.handleMessage(unfenceBrokerRecord);
        assertEquals("false",
            metadataNodeManager.getData().root().directory("brokers", "1").file("isFenced").contents());

        FenceBrokerRecord fenceBrokerRecord = new FenceBrokerRecord()
            .setId(1)
            .setEpoch(2);
        metadataNodeManager.handleMessage(fenceBrokerRecord);
        assertEquals("true",
            metadataNodeManager.getData().root().directory("brokers", "1").file("isFenced").contents());
    }

    @Test
    public void testClientQuotaRecord() {
        ClientQuotaRecord record = new ClientQuotaRecord()
            .setEntity(Arrays.asList(
                    new ClientQuotaRecord.EntityData()
                        .setEntityType("user")
                        .setEntityName("kraft"),
                    new ClientQuotaRecord.EntityData()
                        .setEntityType("client")
                        .setEntityName("kstream")
                ))
            .setKey("producer_byte_rate")
            .setValue(1000.0);

        metadataNodeManager.handleMessage(record);

        assertEquals("1000.0",
            metadataNodeManager.getData().root().directory("client-quotas",
                "client", "kstream",
                "user", "kraft").file("producer_byte_rate").contents());

        metadataNodeManager.handleMessage(record.setRemove(true));

        assertFalse(
            metadataNodeManager.getData().root().directory("client-quotas",
                "client", "kstream",
                "user", "kraft").children().containsKey("producer_byte_rate"));

        record = new ClientQuotaRecord()
            .setEntity(Collections.singletonList(
                new ClientQuotaRecord.EntityData()
                    .setEntityType("user")
                    .setEntityName(null)
            ))
            .setKey("producer_byte_rate")
            .setValue(2000.0);

        metadataNodeManager.handleMessage(record);

        assertEquals("2000.0",
            metadataNodeManager.getData().root().directory("client-quotas",
                "user", "<default>").file("producer_byte_rate").contents());
    }

    @Test
    public void testProducerIdsRecord() {
        // generate a producerId record
        ProducerIdsRecord record1 = new ProducerIdsRecord()
            .setBrokerId(0)
            .setBrokerEpoch(1)
            .setNextProducerId(10000);
        metadataNodeManager.handleMessage(record1);

        assertEquals(
            "0",
            metadataNodeManager.getData().root().directory("producerIds").file("lastBlockBrokerId").contents());
        assertEquals(
            "1",
            metadataNodeManager.getData().root().directory("producerIds").file("lastBlockBrokerEpoch").contents());
        assertEquals(
            10000 + "",
            metadataNodeManager.getData().root().directory("producerIds").file("nextBlockStartId").contents());

        // generate another producerId record
        ProducerIdsRecord record2 = new ProducerIdsRecord()
            .setBrokerId(1)
            .setBrokerEpoch(2)
            .setNextProducerId(11000);
        metadataNodeManager.handleMessage(record2);

        assertEquals(
            "1",
            metadataNodeManager.getData().root().directory("producerIds").file("lastBlockBrokerId").contents());
        assertEquals(
            "2",
            metadataNodeManager.getData().root().directory("producerIds").file("lastBlockBrokerEpoch").contents());
        assertEquals(
            11000 + "",
            metadataNodeManager.getData().root().directory("producerIds").file("nextBlockStartId").contents());
    }
}
