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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.AclsImageTest;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.ConfigurationsImageTest;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ProducerIdsImageTest;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicsImageTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.DELETE_BROKER_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.DELETE_TOPIC_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.UPDATE_BROKER_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.UPDATE_TOPIC_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class KRaftMigrationZkWriterTest {
    /**
     * If ZK is empty, ensure that the writer will sync all metadata from the MetadataImage to ZK
     */
    @Test
    public void testReconcileSnapshotEmptyZk() {

        // These test clients don't return any data in their iterates, so this simulates an empty ZK
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,        // Features are not used in ZK mode, so we don't migrate or dual-write them
            ClusterImage.EMPTY,         // Broker registrations are not dual-written
            TopicsImageTest.IMAGE1,
            ConfigurationsImageTest.IMAGE1,
            ClientQuotasImage.EMPTY,    // TODO KAFKA-15017
            ProducerIdsImageTest.IMAGE1,
            AclsImageTest.IMAGE1,
            ScramImage.EMPTY            // TODO KAFKA-15017
        );

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleSnapshot(image, consumer);
        assertEquals(2, opCounts.remove("CreateTopic"));
        assertEquals(2, opCounts.remove("UpdateBrokerConfig"));
        assertEquals(1, opCounts.remove("UpdateProducerId"));
        assertEquals(4, opCounts.remove("UpdateAcl"));
        assertEquals(0, opCounts.size());

        assertEquals(2, topicClient.createdTopics.size());
        assertTrue(topicClient.createdTopics.contains("foo"));
        assertTrue(topicClient.createdTopics.contains("bar"));
        assertEquals("bar", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")).get("foo"));
        assertEquals("quux", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")).get("baz"));
        assertEquals("foobaz", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "1")).get("foobar"));
        assertEquals(4, aclClient.updatedResources.size());
    }

    /**
     * Only return one of two topics in the ZK topic iterator, ensure that the topic client creates the missing topic
     */
    @Test
    public void testReconcileSnapshotTopics() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                assignments.put(0, Arrays.asList(2, 3, 4));
                assignments.put(1, Arrays.asList(3, 4, 5));
                assignments.put(2, Arrays.asList(2, 4, 5));
                visitor.visitTopic("foo", TopicsImageTest.FOO_UUID, assignments);
            }
        };

        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,
            ClusterImage.EMPTY,
            TopicsImageTest.IMAGE1,     // Two topics, foo and bar
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY
        );

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleSnapshot(image, consumer);
        assertEquals(1, opCounts.remove("CreateTopic"));
        assertEquals(0, opCounts.size());
        assertEquals("bar", topicClient.createdTopics.get(0));
    }

    @Test
    public void testBrokerConfigDelta() {
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);
        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("b0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("b0").setName("spam").setValue(null));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-1").setName("foo").setValue(null));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsDelta(image, delta, consumer);
        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "b0"))
        );
        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-0"))
        );
        assertTrue(
            configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, "topic-1"))
        );
    }

    @Test
    public void testBrokerConfigSnapshot() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient() {
            @Override
            public void iterateBrokerConfigs(BiConsumer<String, Map<String, String>> configConsumer) {
                Map<String, String> b0 = new HashMap<>();
                b0.put("foo", "bar");
                b0.put("spam", "eggs");
                configConsumer.accept("0", b0);
                configConsumer.accept("1", Collections.singletonMap("foo", "bar"));
                configConsumer.accept("3", Collections.singletonMap("foo", "bar"));
            }
        };
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
                .setBrokersInZk(0)
                .setTopicMigrationClient(topicClient)
                .setConfigMigrationClient(configClient)
                .setAclMigrationClient(aclClient)
                .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("1").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("2").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsSnapshot(image, consumer);

        assertTrue(configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.BROKER, "3")),
            "Broker 3 is not in the ConfigurationsImage, it should get deleted");

        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")),
            "Broker 0 only has foo=bar in image, should overwrite the ZK config");

        assertFalse(configClient.writtenConfigs.containsKey(new ConfigResource(ConfigResource.Type.BROKER, "1")),
            "Broker 1 config is the same in image, so no write should happen");

        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "2")),
            "Broker 2 not present in ZK, should see an update");

        assertEquals(2, opCounts.get(UPDATE_BROKER_CONFIG));
        assertEquals(1, opCounts.get(DELETE_BROKER_CONFIG));
    }

    @Test
    public void testTopicConfigSnapshot() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient() {
            @Override
            public void iterateTopicConfigs(BiConsumer<String, Map<String, String>> configConsumer) {
                Map<String, String> topic0 = new HashMap<>();
                topic0.put("foo", "bar");
                topic0.put("spam", "eggs");
                configConsumer.accept("topic-0", topic0);
                configConsumer.accept("topic-1", Collections.singletonMap("foo", "bar"));
                configConsumer.accept("topic-3", Collections.singletonMap("foo", "bar"));
            }
        };
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-1").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-2").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsSnapshot(image, consumer);

        assertTrue(configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, "topic-3")),
                "Topic topic-3 is not in the ConfigurationsImage, it should get deleted");

        assertEquals(
                Collections.singletonMap("foo", "bar"),
                configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-0")),
                "Topic topic-0 only has foo=bar in image, should overwrite the ZK config");

        assertFalse(configClient.writtenConfigs.containsKey(new ConfigResource(ConfigResource.Type.TOPIC, "topic-1")),
                "Topic topic-1 config is the same in image, so no write should happen");

        assertEquals(
                Collections.singletonMap("foo", "bar"),
                configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-2")),
                "Topic topic-2 not present in ZK, should see an update");

        assertEquals(2, opCounts.get(UPDATE_TOPIC_CONFIG));
        assertEquals(1, opCounts.get(DELETE_TOPIC_CONFIG));
    }

    @Test
    public void testInvalidConfigSnapshot() {
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);
        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType((byte) 99).setResourceName("resource").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        assertThrows(RuntimeException.class, () -> writer.handleConfigsSnapshot(image, consumer),
            "Should throw due to invalid resource in image");
    }
}
