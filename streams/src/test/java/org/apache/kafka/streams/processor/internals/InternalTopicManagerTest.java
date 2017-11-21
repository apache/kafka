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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InternalTopicManagerTest {

    private final Node broker1 = new Node(0, "dummyHost-1", 1234);
    private final Node broker2 = new Node(1, "dummyHost-2", 1234);
    private final List<Node> cluster = new ArrayList<Node>(2) {
        {
            add(broker1);
            add(broker2);
        }
    };
    private final String topic = "test_topic";
    private final String topic2 = "test_topic_2";
    private final List<Node> singleReplica = Collections.singletonList(broker1);

    private MockAdminClient mockAdminClient;
    private InternalTopicManager internalTopicManager;

    private final Map<String, Object> config = new HashMap<String, Object>() {
        {
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.host() + ":" + broker1.port());
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            put(AdminClientConfig.RETRIES_CONFIG, 1);
            put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 1);
        }
    };

    @Before
    public void init() {
        mockAdminClient = new MockAdminClient(cluster);
        internalTopicManager = new InternalTopicManager(
            mockAdminClient,
            config);
    }

    @After
    public void shutdown() throws IOException {
        mockAdminClient.close();
    }

    @Test
    public void shouldReturnCorrectPartitionCounts() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList())),
            null);
        assertEquals(Collections.singletonMap(topic, 1), internalTopicManager.getNumPartitions(Collections.singleton(topic)));
    }

    @Test
    public void shouldFailWithUnknownTopicException() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList())),
            null);

        try {
            internalTopicManager.getNumPartitions(new HashSet<String>() {
                {
                    add(topic);
                    add(topic2);
                }
            });
            fail("Should have thrown UnknownTopicOrPartitionException.");
        } catch (final StreamsException expected) {
            assertTrue(expected.getCause() instanceof UnknownTopicOrPartitionException);
        }
    }

    @Test
    public void shouldExhaustRetriesOnTimeoutExceptionForGetNumPartitions() {
        mockAdminClient.timeoutNextRequest(2);

        try {
            internalTopicManager.getNumPartitions(Collections.singleton(topic));
            fail("Should have thrown StreamsException.");
        } catch (final StreamsException expected) {
            assertNull(expected.getCause());
            assertEquals("Could not get number of partitions from brokers. This can happen if the Kafka cluster is temporary not available. You can increase admin client config `retries` to be resilient against this error.", expected.getMessage());
        }
    }

    @Test
    public void shouldCreateRequiredTopics() throws Exception {
        final InternalTopicConfig topicConfig = new InternalTopicConfig(topic,  Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), Collections.<String, String>emptyMap());
        topicConfig.setNumberOfPartitions(1);
        internalTopicManager.makeReady(Collections.singletonMap(topic, topicConfig));

        assertEquals(Collections.singleton(topic), mockAdminClient.listTopics().names().get());
        assertEquals(new TopicDescription(topic, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get());
    }

    @Test
    public void shouldNotCreateTopicIfExistsWithDifferentPartitions() {
        mockAdminClient.addTopic(
            false,
            topic,
            new ArrayList<TopicPartitionInfo>() {
                {
                    add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
                    add(new TopicPartitionInfo(1, broker1, singleReplica, Collections.<Node>emptyList()));
                }
            },
            null);

        try {
            final InternalTopicConfig internalTopicConfig = new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.delete), Collections.<String, String>emptyMap());
            internalTopicConfig.setNumberOfPartitions(1);
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            fail("Should have thrown StreamsException");
        } catch (StreamsException expected) { /* pass */ }
    }

    @Test
    public void shouldNotThrowExceptionIfExistsWithDifferentReplication() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.<Node>emptyList())),
            null);

        // attempt to create it again with replication 1
        final InternalTopicManager internalTopicManager2 = new InternalTopicManager(
            mockAdminClient,
            config);

        final InternalTopicConfig internalTopicConfig = new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.delete), Collections.<String, String>emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        internalTopicManager2.makeReady(Collections.singletonMap(topic, internalTopicConfig));
    }

    @Test
    public void shouldNotThrowExceptionForEmptyTopicMap() {
        internalTopicManager.makeReady(Collections.<String, InternalTopicConfig>emptyMap());
    }

    @Test
    public void shouldExhaustRetriesOnTimeoutExceptionForMakeReady() {
        mockAdminClient.timeoutNextRequest(4);

        final InternalTopicConfig internalTopicConfig = new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.delete), Collections.<String, String>emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            fail("Should have thrown StreamsException.");
        } catch (final StreamsException expected) {
            assertNull(expected.getCause());
            assertEquals("Could not create topics. This can happen if the Kafka cluster is temporary not available. You can increase admin client config `retries` to be resilient against this error.", expected.getMessage());
        }
    }

}
