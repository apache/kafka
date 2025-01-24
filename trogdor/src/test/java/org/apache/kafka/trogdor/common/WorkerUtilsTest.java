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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WorkerUtilsTest {

    private static final Logger log = LoggerFactory.getLogger(WorkerUtilsTest.class);

    private final Node broker1 = new Node(0, "testHost-1", 1234);
    private final Node broker2 = new Node(1, "testHost-2", 1234);
    private final Node broker3 = new Node(1, "testHost-3", 1234);
    private final List<Node> cluster = Arrays.asList(broker1, broker2, broker3);
    private final List<Node> singleReplica = Collections.singletonList(broker1);

    private static final String TEST_TOPIC = "test-topic-1";
    private static final short TEST_REPLICATION_FACTOR = 1;
    private static final int TEST_PARTITIONS = 1;
    private static final NewTopic NEW_TEST_TOPIC =
        new NewTopic(TEST_TOPIC, TEST_PARTITIONS, TEST_REPLICATION_FACTOR);

    private MockAdminClient adminClient;


    @BeforeEach
    public void setUp() {
        adminClient = new MockAdminClient(cluster, broker1);
    }

    @Test
    public void testCreateOneTopic() throws Throwable {
        Map<String, NewTopic> newTopics = Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC);

        WorkerUtils.createTopics(log, adminClient, newTopics, true);
        assertEquals(Collections.singleton(TEST_TOPIC), adminClient.listTopics().names().get());
        assertEquals(
            new TopicDescription(
                TEST_TOPIC, false,
                Collections.singletonList(
                    new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))),
            adminClient.describeTopics(
                Collections.singleton(TEST_TOPIC)).topicNameValues().get(TEST_TOPIC).get()
        );
    }

    @Test
    public void testCreateRetriesOnTimeout() throws Throwable {
        adminClient.timeoutNextRequest(1);

        WorkerUtils.createTopics(
            log, adminClient, Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC), true);

        assertEquals(
            new TopicDescription(
                TEST_TOPIC, false,
                Collections.singletonList(
                    new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))),
            adminClient.describeTopics(
                Collections.singleton(TEST_TOPIC)).topicNameValues().get(TEST_TOPIC).get()
        );
    }

    @Test
    public void testCreateZeroTopicsDoesNothing() throws Throwable {
        WorkerUtils.createTopics(log, adminClient, Collections.emptyMap(), true);
        assertEquals(0, adminClient.listTopics().names().get().size());
    }

    @Test
    public void testCreateTopicsFailsIfAtLeastOneTopicExists() {
        adminClient.addTopic(
            false,
            TEST_TOPIC,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList())),
            null);

        Map<String, NewTopic> newTopics = new HashMap<>();
        newTopics.put(TEST_TOPIC, NEW_TEST_TOPIC);
        newTopics.put("another-topic",
                      new NewTopic("another-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR));
        newTopics.put("one-more-topic",
                      new NewTopic("one-more-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR));

        assertThrows(TopicExistsException.class, () -> WorkerUtils.createTopics(log, adminClient, newTopics, true));
    }

    @Test
    public void testExistingTopicsMustHaveRequestedNumberOfPartitions() {
        List<TopicPartitionInfo> tpInfo = new ArrayList<>();
        tpInfo.add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
        tpInfo.add(new TopicPartitionInfo(1, broker2, singleReplica, Collections.emptyList()));
        adminClient.addTopic(
            false,
            TEST_TOPIC,
            tpInfo,
            null);

        assertThrows(RuntimeException.class, () -> WorkerUtils.createTopics(
            log, adminClient, Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC), false));
    }

    @Test
    public void testExistingTopicsNotCreated() throws Throwable {
        final String existingTopic = "existing-topic";
        List<TopicPartitionInfo> tpInfo = new ArrayList<>();
        tpInfo.add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
        tpInfo.add(new TopicPartitionInfo(1, broker2, singleReplica, Collections.emptyList()));
        tpInfo.add(new TopicPartitionInfo(2, broker3, singleReplica, Collections.emptyList()));
        adminClient.addTopic(
            false,
            existingTopic,
            tpInfo,
            null);

        WorkerUtils.createTopics(
            log, adminClient,
            Collections.singletonMap(
                existingTopic,
                new NewTopic(existingTopic, tpInfo.size(), TEST_REPLICATION_FACTOR)), false);

        assertEquals(Collections.singleton(existingTopic), adminClient.listTopics().names().get());
    }

    @Test
    public void testCreatesNotExistingTopics() throws Throwable {
        // should be no topics before the call
        assertEquals(0, adminClient.listTopics().names().get().size());

        WorkerUtils.createTopics(
            log, adminClient, Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC), false);

        assertEquals(Collections.singleton(TEST_TOPIC), adminClient.listTopics().names().get());
        assertEquals(
            new TopicDescription(
                TEST_TOPIC, false,
                Collections.singletonList(
                    new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))),
            adminClient.describeTopics(Collections.singleton(TEST_TOPIC)).topicNameValues().get(TEST_TOPIC).get()
        );
    }

    @Test
    public void testCreatesOneTopicVerifiesOneTopic() throws Throwable {
        final String existingTopic = "existing-topic";
        List<TopicPartitionInfo> tpInfo = new ArrayList<>();
        tpInfo.add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
        tpInfo.add(new TopicPartitionInfo(1, broker2, singleReplica, Collections.emptyList()));
        adminClient.addTopic(
            false,
            existingTopic,
            tpInfo,
            null);

        Map<String, NewTopic> topics = new HashMap<>();
        topics.put(existingTopic,
                   new NewTopic(existingTopic, tpInfo.size(), TEST_REPLICATION_FACTOR));
        topics.put(TEST_TOPIC, NEW_TEST_TOPIC);

        WorkerUtils.createTopics(log, adminClient, topics, false);

        assertEquals(Set.of(existingTopic, TEST_TOPIC), adminClient.listTopics().names().get());
    }

    @Test
    public void testCreateNonExistingTopicsWithZeroTopicsDoesNothing() throws Throwable {
        WorkerUtils.createTopics(
            log, adminClient, Collections.emptyMap(), false);
        assertEquals(0, adminClient.listTopics().names().get().size());
    }

    @Test
    public void testAddConfigsToPropertiesAddsAllConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Properties resultProps = new Properties();
        resultProps.putAll(props);
        resultProps.put(ProducerConfig.CLIENT_ID_CONFIG, "test-client");
        resultProps.put(ProducerConfig.LINGER_MS_CONFIG, "1000");

        WorkerUtils.addConfigsToProperties(
            props,
            Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, "test-client"),
            Collections.singletonMap(ProducerConfig.LINGER_MS_CONFIG, "1000"));
        assertEquals(resultProps, props);
    }

    @Test
    public void testCommonConfigOverwritesDefaultProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Properties resultProps = new Properties();
        resultProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        resultProps.put(ProducerConfig.ACKS_CONFIG, "1");
        resultProps.put(ProducerConfig.LINGER_MS_CONFIG, "1000");

        WorkerUtils.addConfigsToProperties(
            props,
            Collections.singletonMap(ProducerConfig.ACKS_CONFIG, "1"),
            Collections.singletonMap(ProducerConfig.LINGER_MS_CONFIG, "1000"));
        assertEquals(resultProps, props);
    }

    @Test
    public void testClientConfigOverwritesBothDefaultAndCommonConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Properties resultProps = new Properties();
        resultProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        resultProps.put(ProducerConfig.ACKS_CONFIG, "0");

        WorkerUtils.addConfigsToProperties(
            props,
            Collections.singletonMap(ProducerConfig.ACKS_CONFIG, "1"),
            Collections.singletonMap(ProducerConfig.ACKS_CONFIG, "0"));
        assertEquals(resultProps, props);
    }

    @Test
    public void testGetMatchingTopicPartitionsCorrectlyMatchesExactTopicName() throws Throwable {
        final String topic1 = "existing-topic";
        final String topic2 = "another-topic";
        makeExistingTopicWithOneReplica(topic1, 10);
        makeExistingTopicWithOneReplica(topic2, 20);

        Collection<TopicPartition> topicPartitions =
            WorkerUtils.getMatchingTopicPartitions(adminClient, topic2, 0, 2);
        assertEquals(
            Set.of(
                new TopicPartition(topic2, 0), new TopicPartition(topic2, 1),
                new TopicPartition(topic2, 2)
            ),
            new HashSet<>(topicPartitions)
        );
    }

    @Test
    public void testGetMatchingTopicPartitionsCorrectlyMatchesTopics() throws Throwable {
        final String topic1 = "test-topic";
        final String topic2 = "another-test-topic";
        final String topic3 = "one-more";
        makeExistingTopicWithOneReplica(topic1, 10);
        makeExistingTopicWithOneReplica(topic2, 20);
        makeExistingTopicWithOneReplica(topic3, 30);

        Collection<TopicPartition> topicPartitions =
            WorkerUtils.getMatchingTopicPartitions(adminClient, ".*-topic$", 0, 1);
        assertEquals(
            Set.of(
                new TopicPartition(topic1, 0), new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)
            ),
            new HashSet<>(topicPartitions)
        );
    }

    private void makeExistingTopicWithOneReplica(String topicName, int numPartitions) {
        List<TopicPartitionInfo> tpInfo = new ArrayList<>();
        int brokerIndex = 0;
        for (int i = 0; i < numPartitions; ++i) {
            Node broker = cluster.get(brokerIndex);
            tpInfo.add(new TopicPartitionInfo(
                i, broker, singleReplica, Collections.emptyList()));
            brokerIndex = (brokerIndex + 1) % cluster.size();
        }
        adminClient.addTopic(
            false,
            topicName,
            tpInfo,
            null);
    }

    @Test
    public void testVerifyTopics() throws Throwable {
        Map<String, NewTopic> newTopics = Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC);
        WorkerUtils.createTopics(log, adminClient, newTopics, true);
        adminClient.setFetchesRemainingUntilVisible(TEST_TOPIC, 2);
        WorkerUtils.verifyTopics(log, adminClient, Collections.singleton(TEST_TOPIC),
            Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC), 3, 1);
        adminClient.setFetchesRemainingUntilVisible(TEST_TOPIC, 100);
        assertThrows(UnknownTopicOrPartitionException.class, () ->
            WorkerUtils.verifyTopics(log, adminClient, Collections.singleton(TEST_TOPIC),
                Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC), 2, 1));
    }
}
