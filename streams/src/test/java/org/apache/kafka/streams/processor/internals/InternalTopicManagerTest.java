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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private final String topic3 = "test_topic_3";
    private final List<Node> singleReplica = Collections.singletonList(broker1);

    private MockAdminClient mockAdminClient;
    private InternalTopicManager internalTopicManager;

    private final Map<String, Object> config = new HashMap<String, Object>() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.host() + ":" + broker1.port());
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            put(StreamsConfig.adminClientPrefix(StreamsConfig.RETRIES_CONFIG), 1);
            put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 16384);
        }
    };

    @Before
    public void init() {
        mockAdminClient = new MockAdminClient(cluster, broker1);
        internalTopicManager = new InternalTopicManager(
            mockAdminClient,
            new StreamsConfig(config));
    }

    @After
    public void shutdown() {
        mockAdminClient.close();
    }

    @Test
    public void shouldReturnCorrectPartitionCounts() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList())),
            null);
        assertEquals(Collections.singletonMap(topic, 1), internalTopicManager.getNumPartitions(Collections.singleton(topic)));
    }

    @Test
    public void shouldCreateRequiredTopics() throws Exception {
        final InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig2 = new UnwindowedChangelogTopicConfig(topic2, Collections.emptyMap());
        topicConfig2.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig3 = new WindowedChangelogTopicConfig(topic3, Collections.emptyMap());
        topicConfig3.setNumberOfPartitions(1);

        internalTopicManager.makeReady(Collections.singletonMap(topic, topicConfig));
        internalTopicManager.makeReady(Collections.singletonMap(topic2, topicConfig2));
        internalTopicManager.makeReady(Collections.singletonMap(topic3, topicConfig3));

        assertEquals(Utils.mkSet(topic, topic2, topic3), mockAdminClient.listTopics().names().get());
        assertEquals(new TopicDescription(topic, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get());
        assertEquals(new TopicDescription(topic2, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic2)).values().get(topic2).get());
        assertEquals(new TopicDescription(topic3, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic3)).values().get(topic3).get());

        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final ConfigResource resource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
        final ConfigResource resource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);

        assertEquals(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource)).values().get(resource).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), mockAdminClient.describeConfigs(Collections.singleton(resource2)).values().get(resource2).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource3)).values().get(resource3).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));
    }

    @Test
    public void shouldCompleteTopicValidationOnRetry() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(admin, new StreamsConfig(config));
        final TopicPartitionInfo partitionInfo = new TopicPartitionInfo(0, broker1,
            Collections.singletonList(broker1), Collections.singletonList(broker1));

        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessFuture.complete(new TopicDescription(topic, false, Collections.singletonList(partitionInfo), Collections.emptySet()));
        topicDescriptionFailFuture.completeExceptionally(new UnknownTopicOrPartitionException("KABOOM!"));

        final KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> topicCreationFuture = new KafkaFutureImpl<>();
        topicCreationFuture.completeExceptionally(new TopicExistsException("KABOOM!"));

        // let the first describe succeed on topic, and fail on topic2, and then let creation throws topics-existed;
        // it should retry with just topic2 and then let it succeed
        EasyMock.expect(admin.describeTopics(Utils.mkSet(topic, topic2)))
            .andReturn(new MockDescribeTopicsResult(Utils.mkMap(
                Utils.mkEntry(topic, topicDescriptionSuccessFuture),
                Utils.mkEntry(topic2, topicDescriptionFailFuture)
            ))).once();
        EasyMock.expect(admin.createTopics(Collections.singleton(new NewTopic(topic2, Optional.of(1), Optional.of((short) 1))
            .configs(Utils.mkMap(Utils.mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                                 Utils.mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"))))))
            .andReturn(new MockCreateTopicsResult(Collections.singletonMap(topic2, topicCreationFuture))).once();
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic2)))
            .andReturn(new MockDescribeTopicsResult(Collections.singletonMap(topic2, topicDescriptionSuccessFuture)));

        EasyMock.replay(admin);

        final InternalTopicConfig topicConfig = new UnwindowedChangelogTopicConfig(topic, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topic2Config = new UnwindowedChangelogTopicConfig(topic2, Collections.emptyMap());
        topic2Config.setNumberOfPartitions(1);
        topicManager.makeReady(Utils.mkMap(
            Utils.mkEntry(topic, topicConfig),
            Utils.mkEntry(topic2, topic2Config)
        ));

        EasyMock.verify(admin);
    }

    @Test
    public void shouldNotCreateTopicIfExistsWithDifferentPartitions() {
        mockAdminClient.addTopic(
            false,
            topic,
            new ArrayList<TopicPartitionInfo>() {
                {
                    add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
                    add(new TopicPartitionInfo(1, broker1, singleReplica, Collections.emptyList()));
                }
            },
            null);

        try {
            final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
            internalTopicConfig.setNumberOfPartitions(1);
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* pass */ }
    }

    @Test
    public void shouldNotThrowExceptionIfExistsWithDifferentReplication() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);

        // attempt to create it again with replication 1
        final InternalTopicManager internalTopicManager2 = new InternalTopicManager(
            mockAdminClient,
            new StreamsConfig(config));

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        internalTopicManager2.makeReady(Collections.singletonMap(topic, internalTopicConfig));
    }

    @Test
    public void shouldNotThrowExceptionForEmptyTopicMap() {
        internalTopicManager.makeReady(Collections.emptyMap());
    }

    @Test
    public void shouldExhaustRetriesOnTimeoutExceptionForMakeReady() {
        mockAdminClient.timeoutNextRequest(1);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            fail("Should have thrown StreamsException.");
        } catch (final StreamsException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void shouldLogWhenTopicNotFoundAndNotThrowException() {
        LogCaptureAppender.setClassLoggerToDebug(InternalTopicManager.class);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);

        final InternalTopicConfig internalTopicConfigII = new RepartitionTopicConfig("internal-topic", Collections.emptyMap());
        internalTopicConfigII.setNumberOfPartitions(1);

        final Map<String, InternalTopicConfig> topicConfigMap = new HashMap<>();
        topicConfigMap.put(topic, internalTopicConfig);
        topicConfigMap.put("internal-topic", internalTopicConfigII);

        internalTopicManager.makeReady(topicConfigMap);
        boolean foundExpectedMessage = false;
        for (final String message : appender.getMessages()) {
            foundExpectedMessage |= message.contains("Topic internal-topic is unknown or not found, hence not existed yet");
        }
        assertTrue(foundExpectedMessage);

    }

    @Test
    public void shouldExhaustRetriesOnMarkedForDeletionTopic() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);
        mockAdminClient.markTopicForDeletion(topic);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            fail("Should have thrown StreamsException.");
        } catch (final StreamsException expected) {
            assertNull(expected.getCause());
            assertTrue(expected.getMessage().startsWith("Could not create topics after 1 retries"));
        }
    }

    private class MockCreateTopicsResult extends CreateTopicsResult {

        MockCreateTopicsResult(final Map<String, KafkaFuture<TopicMetadataAndConfig>> futures) {
            super(futures);
        }
    }

    private class MockDescribeTopicsResult extends DescribeTopicsResult {

        MockDescribeTopicsResult(final Map<String, KafkaFuture<TopicDescription>> futures) {
            super(futures);
        }
    }
}
