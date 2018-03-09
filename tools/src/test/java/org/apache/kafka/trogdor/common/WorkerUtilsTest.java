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


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Before;
import org.junit.Test;

import org.easymock.EasyMock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class WorkerUtilsTest {

    private static final Logger log = LoggerFactory.getLogger(WorkerUtilsTest.class);

    private static final String TEST_TOPIC = "test-topic-1";
    private static final short TEST_REPLICATION_FACTOR = 3;
    private static final int TEST_PARTITIONS = 5;
    private static final NewTopic NEW_TEST_TOPIC =
        new NewTopic(TEST_TOPIC, TEST_PARTITIONS, TEST_REPLICATION_FACTOR);

    private static final Node TEST_NODE = new Node(1, "localhost", 9092);
    private static final TopicPartitionInfo TEST_TOPIC_PARTITION =
        new TopicPartitionInfo(
            0, TEST_NODE, Collections.singletonList(TEST_NODE), Collections.singletonList(TEST_NODE));
    private static final TopicDescription TEST_TOPIC_DESCRIPTION =
        new TopicDescription(TEST_TOPIC, false, Collections.singletonList(TEST_TOPIC_PARTITION));

    private AdminClient adminClient;
    private CreateTopicsResult createTopicsResult;
    private ListTopicsResult listTopicsResult;
    private DescribeTopicsResult describeTopicsResult;

    @Before
    public void setUp() throws Exception {
        adminClient = EasyMock.mock(AdminClient.class);
        createTopicsResult = EasyMock.mock(CreateTopicsResult.class);
        listTopicsResult = EasyMock.mock(ListTopicsResult.class);
        describeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    }

    @Test
    public void testCreateOneTopic() throws Throwable {
        Set<NewTopic> newTopics = Collections.singleton(NEW_TEST_TOPIC);

        expectCreateTopics(newTopics, Collections.<String, Exception>emptyMap());

        EasyMock.replay(createTopicsResult);
        EasyMock.replay(adminClient);

        WorkerUtils.createTopics(log, adminClient, newTopics);

        EasyMock.verify(createTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test
    public void testCreateZeroTopicsDoesNothing() throws Throwable {
        WorkerUtils.createTopics(log, adminClient, Collections.<NewTopic>emptyList());
    }

    @Test(expected = UnknownServerException.class)
    public void testCreateTopicsFailsIfAtLeastOneTopicCreationFails() throws Throwable {
        List<NewTopic> newTopics = new ArrayList<>();

        newTopics.add(NEW_TEST_TOPIC);
        newTopics.add(new NewTopic("another-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR));
        newTopics.add(new NewTopic("one-more-topic", TEST_PARTITIONS, TEST_REPLICATION_FACTOR));

        expectCreateTopics(
            newTopics,
            Collections.<String, Exception>singletonMap(TEST_TOPIC, new UnknownServerException()));

        EasyMock.replay(createTopicsResult);
        EasyMock.replay(adminClient);

        WorkerUtils.createTopics(log, adminClient, newTopics);

        EasyMock.verify(createTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExistingTopicsMustHaveRequestedNumberOfPartitions() throws Throwable {
        Map<String, NewTopic> topics = Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC);

        expectListTopics(topics.keySet());
        expectDescribeTopics(topics.keySet(), 1);

        EasyMock.replay(listTopicsResult);
        EasyMock.replay(describeTopicsResult);
        EasyMock.replay(adminClient);

        WorkerUtils.verifyTopicsAndCreateNonExistingTopics(log, adminClient, topics);

        EasyMock.verify(listTopicsResult);
        EasyMock.verify(describeTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test
    public void testExistingTopicsNotCreated() throws Throwable {
        Map<String, NewTopic> topics = Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC);

        expectListTopics(topics.keySet());
        expectDescribeTopics(topics.keySet(), TEST_PARTITIONS);

        EasyMock.replay(listTopicsResult);
        EasyMock.replay(describeTopicsResult);
        EasyMock.replay(adminClient);

        WorkerUtils.verifyTopicsAndCreateNonExistingTopics(log, adminClient, topics);

        EasyMock.verify(listTopicsResult);
        EasyMock.verify(describeTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test
    public void testCreatesNotExistingTopics() throws Throwable {
        Map<String, NewTopic> topics = Collections.singletonMap(TEST_TOPIC, NEW_TEST_TOPIC);

        expectListTopics(Collections.<String>emptySet());
        expectCreateTopics(topics.values(), Collections.<String, Exception>emptyMap());

        EasyMock.replay(listTopicsResult);
        EasyMock.replay(createTopicsResult);
        EasyMock.replay(adminClient);

        WorkerUtils.verifyTopicsAndCreateNonExistingTopics(log, adminClient, topics);

        EasyMock.verify(listTopicsResult);
        EasyMock.verify(createTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test
    public void testCreatesOneTopicVerifiesOneTopic() throws Throwable {
        final String newTopicName = "new-topic";
        Map<String, NewTopic> existingTopics = Collections.singletonMap(
            TEST_TOPIC, NEW_TEST_TOPIC);

        NewTopic newTopic = new NewTopic(newTopicName, TEST_PARTITIONS, TEST_REPLICATION_FACTOR);

        expectListTopics(existingTopics.keySet());
        expectDescribeTopics(existingTopics.keySet(), TEST_PARTITIONS);
        expectCreateTopics(Collections.singleton(newTopic), Collections.<String, Exception>emptyMap());

        EasyMock.replay(listTopicsResult);
        EasyMock.replay(describeTopicsResult);
        EasyMock.replay(createTopicsResult);
        EasyMock.replay(adminClient);

        Map<String, NewTopic> allTopics = new HashMap<>();
        allTopics.putAll(existingTopics);
        allTopics.put(newTopicName, newTopic);
        WorkerUtils.verifyTopicsAndCreateNonExistingTopics(log, adminClient, allTopics);

        EasyMock.verify(listTopicsResult);
        EasyMock.verify(describeTopicsResult);
        EasyMock.verify(createTopicsResult);
        EasyMock.verify(adminClient);
    }

    @Test
    public void testCreateNonExistingTopicsWithZeroTopicsDoesNothing() throws Throwable {
        WorkerUtils.verifyTopicsAndCreateNonExistingTopics(
            log, adminClient, Collections.<String, NewTopic>emptyMap());
    }

    private void expectCreateTopics(
        Collection<NewTopic> newTopics, Map<String, Exception> exceptions) {
        Map<String, KafkaFuture<Void>> topicFutures = new HashMap<>(newTopics.size());
        for (NewTopic newTopic: newTopics) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            topicFutures.put(newTopic.name(), future);
            Exception res = exceptions.get(newTopic.name());
            if (res == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(res);
            }
        }
        EasyMock.expect(adminClient.createTopics(EasyMock.anyObject(Collection.class)))
            .andReturn(createTopicsResult).anyTimes();
        EasyMock.expect(createTopicsResult.values())
            .andReturn(topicFutures).anyTimes();
    }

    private void expectListTopics(Set<String> topics) {
        KafkaFutureImpl<Set<String>> future = new KafkaFutureImpl<>();
        future.complete(topics);

        EasyMock.expect(adminClient.listTopics(EasyMock.anyObject(ListTopicsOptions.class)))
            .andReturn(listTopicsResult).anyTimes();
        EasyMock.expect(listTopicsResult.names()).andReturn(future);
    }

    private void expectDescribeTopics(Set<String> topics, int partitions) {
        Map<String, TopicDescription> topicDescriptionMap = new HashMap<>();
        List<TopicPartitionInfo> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicPartitions.add(TEST_TOPIC_PARTITION);
        }

        KafkaFutureImpl<Map<String, TopicDescription>>  future = new KafkaFutureImpl<>();
        for (String topic: topics) {
            TopicDescription description = new TopicDescription(topic, false, topicPartitions);
            topicDescriptionMap.put(topic, description);
        }
        future.complete(topicDescriptionMap);
        EasyMock.expect(adminClient.describeTopics(
            EasyMock.anyObject(Collection.class), EasyMock.anyObject(DescribeTopicsOptions.class)))
            .andReturn(describeTopicsResult).anyTimes();
        EasyMock.expect(describeTopicsResult.all()).andReturn(future);
    }

}
