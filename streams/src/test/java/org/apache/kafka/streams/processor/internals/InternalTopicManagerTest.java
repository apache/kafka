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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.InternalTopicManager.ValidationResult;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
    private final String topic1 = "test_topic";
    private final String topic2 = "test_topic_2";
    private final String topic3 = "test_topic_3";
    private final String topic4 = "test_topic_4";
    private final String topic5 = "test_topic_5";
    private final List<Node> singleReplica = Collections.singletonList(broker1);

    private String threadName;

    private MockAdminClient mockAdminClient;
    private InternalTopicManager internalTopicManager;

    private final Map<String, Object> config = new HashMap<String, Object>() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.host() + ":" + broker1.port());
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 16384);
            put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 100);
            put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, 50);
        }
    };

    @Before
    public void init() {
        threadName = Thread.currentThread().getName();

        mockAdminClient = new MockAdminClient(cluster, broker1);
        internalTopicManager = new InternalTopicManager(
            Time.SYSTEM,
            mockAdminClient,
            new StreamsConfig(config)
        );
    }

    @After
    public void shutdown() {
        mockAdminClient.close();
    }

    @Test
    public void shouldReturnCorrectPartitionCounts() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList())),
            null);
        assertEquals(Collections.singletonMap(topic1, 1),
            internalTopicManager.getNumPartitions(Collections.singleton(topic1), Collections.emptySet()));
    }

    @Test
    public void shouldCreateRequiredTopics() throws Exception {
        final InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig2 = new UnwindowedChangelogTopicConfig(topic2, Collections.emptyMap());
        topicConfig2.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig3 = new WindowedChangelogTopicConfig(topic3, Collections.emptyMap());
        topicConfig3.setNumberOfPartitions(1);

        internalTopicManager.makeReady(Collections.singletonMap(topic1, topicConfig));
        internalTopicManager.makeReady(Collections.singletonMap(topic2, topicConfig2));
        internalTopicManager.makeReady(Collections.singletonMap(topic3, topicConfig3));

        assertEquals(mkSet(topic1, topic2, topic3), mockAdminClient.listTopics().names().get());
        assertEquals(new TopicDescription(topic1, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic1)).values().get(topic1).get());
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

        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
        final ConfigResource resource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
        final ConfigResource resource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);

        assertEquals(
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
            mockAdminClient.describeConfigs(Collections.singleton(resource)).values().get(resource).get().get(TopicConfig.CLEANUP_POLICY_CONFIG)
        );
        assertEquals(
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
            mockAdminClient.describeConfigs(Collections.singleton(resource2)).values().get(resource2).get().get(TopicConfig.CLEANUP_POLICY_CONFIG)
        );
        assertEquals(
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE),
            mockAdminClient.describeConfigs(Collections.singleton(resource3)).values().get(resource3).get().get(TopicConfig.CLEANUP_POLICY_CONFIG)
        );
    }

    @Test
    public void shouldCompleteTopicValidationOnRetry() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final TopicPartitionInfo partitionInfo = new TopicPartitionInfo(0, broker1,
            Collections.singletonList(broker1), Collections.singletonList(broker1));

        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessFuture.complete(
            new TopicDescription(topic1, false, Collections.singletonList(partitionInfo), Collections.emptySet())
        );
        topicDescriptionFailFuture.completeExceptionally(new UnknownTopicOrPartitionException("KABOOM!"));

        final KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> topicCreationFuture = new KafkaFutureImpl<>();
        topicCreationFuture.completeExceptionally(new TopicExistsException("KABOOM!"));

        // let the first describe succeed on topic, and fail on topic2, and then let creation throws topics-existed;
        // it should retry with just topic2 and then let it succeed
        EasyMock.expect(admin.describeTopics(mkSet(topic1, topic2)))
            .andReturn(new MockDescribeTopicsResult(mkMap(
                mkEntry(topic1, topicDescriptionSuccessFuture),
                mkEntry(topic2, topicDescriptionFailFuture)
            ))).once();
        EasyMock.expect(admin.createTopics(Collections.singleton(new NewTopic(topic2, Optional.of(1), Optional.of((short) 1))
            .configs(mkMap(mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                                 mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"))))))
            .andReturn(new MockCreateTopicsResult(Collections.singletonMap(topic2, topicCreationFuture))).once();
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic2)))
            .andReturn(new MockDescribeTopicsResult(Collections.singletonMap(topic2, topicDescriptionSuccessFuture)));

        EasyMock.replay(admin);

        final InternalTopicConfig topicConfig = new UnwindowedChangelogTopicConfig(topic1, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topic2Config = new UnwindowedChangelogTopicConfig(topic2, Collections.emptyMap());
        topic2Config.setNumberOfPartitions(1);
        topicManager.makeReady(mkMap(
            mkEntry(topic1, topicConfig),
            mkEntry(topic2, topic2Config)
        ));

        EasyMock.verify(admin);
    }

    @Test
    public void shouldNotCreateTopicIfExistsWithDifferentPartitions() {
        mockAdminClient.addTopic(
            false,
            topic1,
            new ArrayList<TopicPartitionInfo>() {
                {
                    add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList()));
                    add(new TopicPartitionInfo(1, broker1, singleReplica, Collections.emptyList()));
                }
            },
            null);

        try {
            final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
            internalTopicConfig.setNumberOfPartitions(1);
            internalTopicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
            fail("Should have thrown StreamsException");
        } catch (final StreamsException expected) { /* pass */ }
    }

    @Test
    public void shouldNotThrowExceptionIfExistsWithDifferentReplication() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);

        // attempt to create it again with replication 1
        final InternalTopicManager internalTopicManager2 = new InternalTopicManager(
            Time.SYSTEM,
            mockAdminClient,
            new StreamsConfig(config)
        );

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        internalTopicManager2.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
    }

    @Test
    public void shouldNotThrowExceptionForEmptyTopicMap() {
        internalTopicManager.makeReady(Collections.emptyMap());
    }

    @Test
    public void shouldExhaustRetriesOnTimeoutExceptionForMakeReady() {
        mockAdminClient.timeoutNextRequest(1);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
            fail("Should have thrown StreamsException.");
        } catch (final StreamsException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void shouldLogWhenTopicNotFoundAndNotThrowException() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);

        final InternalTopicConfig internalTopicConfigII =
            new RepartitionTopicConfig("internal-topic", Collections.emptyMap());
        internalTopicConfigII.setNumberOfPartitions(1);

        final Map<String, InternalTopicConfig> topicConfigMap = new HashMap<>();
        topicConfigMap.put(topic1, internalTopicConfig);
        topicConfigMap.put("internal-topic", internalTopicConfigII);

        LogCaptureAppender.setClassLoggerToDebug(InternalTopicManager.class);
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(InternalTopicManager.class)) {
            internalTopicManager.makeReady(topicConfigMap);

            assertThat(
                appender.getMessages(),
                hasItem("stream-thread [" + threadName + "] Topic internal-topic is unknown or not found, hence not existed yet.\n" +
                    "Error message was: org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Topic internal-topic not found.")
            );
        }
    }

    @Test
    public void shouldCreateTopicWhenTopicLeaderNotAvailableAndThenTopicNotFound() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );

        final KafkaFutureImpl<TopicDescription> topicDescriptionLeaderNotAvailableFuture = new KafkaFutureImpl<>();
        topicDescriptionLeaderNotAvailableFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<TopicDescription> topicDescriptionUnknownTopicFuture = new KafkaFutureImpl<>();
        topicDescriptionUnknownTopicFuture.completeExceptionally(new UnknownTopicOrPartitionException("Unknown Topic!"));
        final KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> topicCreationFuture = new KafkaFutureImpl<>();
        topicCreationFuture.complete(EasyMock.createNiceMock(CreateTopicsResult.TopicMetadataAndConfig.class));

        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionLeaderNotAvailableFuture)))
            .once();
        // return empty set for 1st time
        EasyMock.expect(admin.createTopics(Collections.emptySet()))
            .andReturn(new MockCreateTopicsResult(Collections.emptyMap())).once();
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionUnknownTopicFuture)))
            .once();
        EasyMock.expect(admin.createTopics(Collections.singleton(
                new NewTopic(topic1, Optional.of(1), Optional.of((short) 1))
            .configs(mkMap(mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"),
                mkEntry(TopicConfig.SEGMENT_BYTES_CONFIG, "52428800"),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, "-1"))))))
            .andReturn(new MockCreateTopicsResult(Collections.singletonMap(topic1, topicCreationFuture))).once();

        EasyMock.replay(admin);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));

        EasyMock.verify(admin);
    }

    @Test
    public void shouldCompleteValidateWhenTopicLeaderNotAvailableAndThenDescribeSuccess() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final TopicPartitionInfo partitionInfo = new TopicPartitionInfo(0, broker1,
                Collections.singletonList(broker1), Collections.singletonList(broker1));

        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessFuture.complete(
            new TopicDescription(topic1, false, Collections.singletonList(partitionInfo), Collections.emptySet())
        );

        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionFailFuture)))
            .once();
        EasyMock.expect(admin.createTopics(Collections.emptySet()))
            .andReturn(new MockCreateTopicsResult(Collections.emptyMap())).once();
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionSuccessFuture)))
            .once();

        EasyMock.replay(admin);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));

        EasyMock.verify(admin);
    }

    @Test
    public void shouldThrowExceptionWhenKeepsTopicLeaderNotAvailable() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );

        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));

        // simulate describeTopics got LeaderNotAvailableException
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionFailFuture)))
            .anyTimes();
        EasyMock.expect(admin.createTopics(Collections.emptySet()))
            .andReturn(new MockCreateTopicsResult(Collections.emptyMap())).anyTimes();

        EasyMock.replay(admin);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig))
        );
        assertNull(exception.getCause());
        assertThat(
            exception.getMessage(),
            equalTo("Could not create topics within 50 milliseconds." +
                " This can happen if the Kafka cluster is temporarily not available.")
        );

        EasyMock.verify(admin);
    }

    @Test
    public void shouldExhaustRetriesOnMarkedForDeletionTopic() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);
        mockAdminClient.markTopicForDeletion(topic1);

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> internalTopicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig))
        );
        assertNull(exception.getCause());
        assertThat(
            exception.getMessage(),
            equalTo("Could not create topics within 50 milliseconds." +
                " This can happen if the Kafka cluster is temporarily not available.")
        );
    }

    @Test
    public void shouldValidateSuccessfully() {
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        setupTopicInMockAdminClient(topic2, repartitionTopicConfig());
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2)
        ));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldValidateSuccessfullyWithEmptyInternalTopics() {
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());

        final ValidationResult validationResult = internalTopicManager.validate(Collections.emptyMap());

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldReportMissingTopics() {
        final String missingTopic1 = "missingTopic1";
        final String missingTopic2 = "missingTopic2";
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(missingTopic1, 1);
        final InternalTopicConfig internalTopicConfig3 = setupRepartitionTopicConfig(missingTopic2, 1);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(missingTopic1, internalTopicConfig2),
            mkEntry(missingTopic2, internalTopicConfig3)
        ));

        final Set<String> missingTopics = validationResult.missingTopics();
        assertThat(missingTopics.size(), is(2));
        assertThat(missingTopics, hasItem(missingTopic1));
        assertThat(missingTopics, hasItem(missingTopic2));
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldReportMisconfigurationsOfPartitionCount() {
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        setupTopicInMockAdminClient(topic2, repartitionTopicConfig());
        setupTopicInMockAdminClient(topic3, repartitionTopicConfig());
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 2);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 3);
        final InternalTopicConfig internalTopicConfig3 = setupRepartitionTopicConfig(topic3, 1);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2),
            mkEntry(topic3, internalTopicConfig3)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(2));
        assertThat(misconfigurationsForTopics, hasKey(topic1));
        assertThat(misconfigurationsForTopics.get(topic1).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic1).get(0),
            is("Internal topic " + topic1 + " requires 2 partitions, but the existing topic on the broker has 1 partitions.")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic2));
        assertThat(misconfigurationsForTopics.get(topic2).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic2).get(0),
            is("Internal topic " + topic2 + " requires 3 partitions, but the existing topic on the broker has 1 partitions.")
        );
        assertThat(misconfigurationsForTopics, not(hasKey(topic3)));
    }

    @Test
    public void shouldReportMisconfigurationsOfCleanupPolicyForUnwindowedChangelogTopics() {
        final Map<String, String> unwindowedChangelogConfigWithDeleteCleanupPolicy = unwindowedChangelogConfig();
        unwindowedChangelogConfigWithDeleteCleanupPolicy.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_DELETE
        );
        setupTopicInMockAdminClient(topic1, unwindowedChangelogConfigWithDeleteCleanupPolicy);
        final Map<String, String> unwindowedChangelogConfigWithDeleteCompactCleanupPolicy = unwindowedChangelogConfig();
        unwindowedChangelogConfigWithDeleteCompactCleanupPolicy.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
        );
        setupTopicInMockAdminClient(topic2, unwindowedChangelogConfigWithDeleteCompactCleanupPolicy);
        setupTopicInMockAdminClient(topic3, unwindowedChangelogConfig());
        final InternalTopicConfig internalTopicConfig1 = setupUnwindowedChangelogTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupUnwindowedChangelogTopicConfig(topic2, 1);
        final InternalTopicConfig internalTopicConfig3 = setupUnwindowedChangelogTopicConfig(topic3, 1);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2),
            mkEntry(topic3, internalTopicConfig3)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(2));
        assertThat(misconfigurationsForTopics, hasKey(topic1));
        assertThat(misconfigurationsForTopics.get(topic1).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic1).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic " + topic1 + " should not contain \""
                + TopicConfig.CLEANUP_POLICY_DELETE + "\".")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic2));
        assertThat(misconfigurationsForTopics.get(topic2).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic2).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic " + topic2 + " should not contain \""
                + TopicConfig.CLEANUP_POLICY_DELETE + "\".")
        );
        assertThat(misconfigurationsForTopics, not(hasKey(topic3)));
    }

    @Test
    public void shouldReportMisconfigurationsOfCleanupPolicyForWindowedChangelogTopics() {
        final long retentionMs = 1000;
        final long shorterRetentionMs = 900;
        setupTopicInMockAdminClient(topic1, windowedChangelogConfig(retentionMs));
        setupTopicInMockAdminClient(topic2, windowedChangelogConfig(shorterRetentionMs));
        final Map<String, String> windowedChangelogConfigOnlyCleanupPolicyCompact = windowedChangelogConfig(retentionMs);
        windowedChangelogConfigOnlyCleanupPolicyCompact.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        setupTopicInMockAdminClient(topic3, windowedChangelogConfigOnlyCleanupPolicyCompact);
        final Map<String, String> windowedChangelogConfigOnlyCleanupPolicyDelete = windowedChangelogConfig(shorterRetentionMs);
        windowedChangelogConfigOnlyCleanupPolicyDelete.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        setupTopicInMockAdminClient(topic4, windowedChangelogConfigOnlyCleanupPolicyDelete);
        final Map<String, String> windowedChangelogConfigWithRetentionBytes = windowedChangelogConfig(retentionMs);
        windowedChangelogConfigWithRetentionBytes.put(TopicConfig.RETENTION_BYTES_CONFIG, "1024");
        setupTopicInMockAdminClient(topic5, windowedChangelogConfigWithRetentionBytes);
        final InternalTopicConfig internalTopicConfig1 = setupWindowedChangelogTopicConfig(topic1, 1, retentionMs);
        final InternalTopicConfig internalTopicConfig2 = setupWindowedChangelogTopicConfig(topic2, 1, retentionMs);
        final InternalTopicConfig internalTopicConfig3 = setupWindowedChangelogTopicConfig(topic3, 1, retentionMs);
        final InternalTopicConfig internalTopicConfig4 = setupWindowedChangelogTopicConfig(topic4, 1, retentionMs);
        final InternalTopicConfig internalTopicConfig5 = setupWindowedChangelogTopicConfig(topic5, 1, retentionMs);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2),
            mkEntry(topic3, internalTopicConfig3),
            mkEntry(topic4, internalTopicConfig4),
            mkEntry(topic5, internalTopicConfig5)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(3));
        assertThat(misconfigurationsForTopics, hasKey(topic2));
        assertThat(misconfigurationsForTopics.get(topic2).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic2).get(0),
            is("Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic " +
                topic2 + " is " + shorterRetentionMs + " but should be " + retentionMs + " or larger.")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic4));
        assertThat(misconfigurationsForTopics.get(topic4).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic4).get(0),
            is("Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic " +
                topic4 + " is " + shorterRetentionMs + " but should be " + retentionMs + " or larger.")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic5));
        assertThat(misconfigurationsForTopics.get(topic5).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic5).get(0),
            is("Retention byte (" + TopicConfig.RETENTION_BYTES_CONFIG + ") of existing internal topic " +
                topic5 + " is set but it should be unset.")
        );
        assertThat(misconfigurationsForTopics, not(hasKey(topic1)));
        assertThat(misconfigurationsForTopics, not(hasKey(topic3)));
    }

    @Test
    public void shouldReportMisconfigurationsOfCleanupPolicyForRepartitionTopics() {
        final long retentionMs = 1000;
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(-1)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
            )
        );
        mockAdminClient.addTopic(
            false,
            topic2,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(-1)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
            )
        );
        mockAdminClient.addTopic(
            false,
            topic3,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(-1)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
            )
        );
        mockAdminClient.addTopic(
            false,
            topic4,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
            )
        );
        mockAdminClient.addTopic(
            false,
            topic5,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(-1)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, "1024")
            )
        );
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        final InternalTopicConfig internalTopicConfig3 = setupRepartitionTopicConfig(topic3, 1);
        final InternalTopicConfig internalTopicConfig4 = setupRepartitionTopicConfig(topic4, 1);
        final InternalTopicConfig internalTopicConfig5 = setupRepartitionTopicConfig(topic5, 1);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2),
            mkEntry(topic3, internalTopicConfig3),
            mkEntry(topic4, internalTopicConfig4),
            mkEntry(topic5, internalTopicConfig5)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(4));
        assertThat(misconfigurationsForTopics, hasKey(topic2));
        assertThat(misconfigurationsForTopics.get(topic2).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic2).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic "
                + topic2 + " should not contain \"" + TopicConfig.CLEANUP_POLICY_COMPACT + "\".")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic3));
        assertThat(misconfigurationsForTopics.get(topic3).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic3).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic "
                + topic3 + " should not contain \"" + TopicConfig.CLEANUP_POLICY_COMPACT + "\".")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic4));
        assertThat(misconfigurationsForTopics.get(topic4).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic4).get(0),
            is("Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic "
                + topic4 + " is " + retentionMs + " but should be -1.")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic5));
        assertThat(misconfigurationsForTopics.get(topic5).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic5).get(0),
            is("Retention byte (" + TopicConfig.RETENTION_BYTES_CONFIG + ") of existing internal topic "
                + topic5 + " is set but it should be unset.")
        );
    }

    @Test
    public void shouldReportMultipleMisconfigurationsForSameTopic() {
        final long retentionMs = 1000;
        final long shorterRetentionMs = 900;
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(shorterRetentionMs)),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, "1024")
            )
        );
        final InternalTopicConfig internalTopicConfig1 = setupWindowedChangelogTopicConfig(topic1, 1, retentionMs);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(1));
        assertThat(misconfigurationsForTopics, hasKey(topic1));
        assertThat(misconfigurationsForTopics.get(topic1).size(), is(2));
        assertThat(
            misconfigurationsForTopics.get(topic1).get(0),
            is("Retention time (" + TopicConfig.RETENTION_MS_CONFIG + ") of existing internal topic " +
                topic1 + " is " + shorterRetentionMs + " but should be " + retentionMs + " or larger.")
        );
        assertThat(
            misconfigurationsForTopics.get(topic1).get(1),
            is("Retention byte (" + TopicConfig.RETENTION_BYTES_CONFIG + ") of existing internal topic " +
                topic1 + " is set but it should be unset.")
        );
    }

    @Test
    public void shouldThrowWhenPartitionCountUnknown() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
        );
        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());

        assertThrows(
            IllegalStateException.class,
            () -> internalTopicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldRetryWhenCallsThrowTimeoutExceptionDuringValidation() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            mkMap(
                mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, "-1"),
                mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
            )
        );
        mockAdminClient.timeoutNextRequest(2);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = internalTopicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldOnlyRetryDescribeTopicsWhenDescribeTopicsThrowsLeaderNotAvailableExceptionDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))))
            .andReturn(new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andReturn(new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
        EasyMock.verify(admin);
    }

    @Test
    public void shouldOnlyRetryDescribeConfigsWhenDescribeConfigsThrowsLeaderNotAvailableExceptionDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andReturn(new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigsFailFuture = new KafkaFutureImpl<>();
        topicConfigsFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andReturn(new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigsFailFuture))))
            .andReturn(new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
        EasyMock.verify(admin);
    }

    @Test
    public void shouldThrowWhenDescribeTopicsThrowsUnexpectedExceptionDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(Throwable.class, () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig)));
    }

    @Test
    public void shouldThrowWhenDescribeConfigsThrowsUnexpectedExceptionDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<Config> configDescriptionFailFuture = new KafkaFutureImpl<>();
        configDescriptionFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, configDescriptionFailFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(Throwable.class, () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig)));
    }

    @Test
    public void shouldThrowWhenTopicDescriptionsDoNotContainTopicDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic2, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(new Config(Collections.emptySet()));
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainTopicDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(new Config(Collections.emptySet()));
        final ConfigResource topicResource1 = new ConfigResource(Type.TOPIC, topic1);
        final ConfigResource topicResource2 = new ConfigResource(Type.TOPIC, topic2);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource1)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource2, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotCleanupPolicyForUnwindowedConfigDuringValidation() {
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupUnwindowedChangelogTopicConfig(topic1, 1),
            configWithoutKey(unwindowedChangelogConfig(), TopicConfig.CLEANUP_POLICY_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainCleanupPolicyForWindowedConfigDuringValidation() {
        final long retentionMs = 1000;
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupWindowedChangelogTopicConfig(topic1, 1, retentionMs),
            configWithoutKey(windowedChangelogConfig(retentionMs), TopicConfig.CLEANUP_POLICY_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainRetentionMsForWindowedConfigDuringValidation() {
        final long retentionMs = 1000;
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupWindowedChangelogTopicConfig(topic1, 1, retentionMs),
            configWithoutKey(windowedChangelogConfig(retentionMs), TopicConfig.RETENTION_MS_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainRetentionBytesForWindowedConfigDuringValidation() {
        final long retentionMs = 1000;
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupWindowedChangelogTopicConfig(topic1, 1, retentionMs),
            configWithoutKey(windowedChangelogConfig(retentionMs), TopicConfig.RETENTION_BYTES_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainCleanupPolicyForRepartitionConfigDuringValidation() {
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupRepartitionTopicConfig(topic1, 1),
            configWithoutKey(repartitionTopicConfig(), TopicConfig.CLEANUP_POLICY_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainRetentionMsForRepartitionConfigDuringValidation() {
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupRepartitionTopicConfig(topic1, 1),
            configWithoutKey(repartitionTopicConfig(), TopicConfig.RETENTION_MS_CONFIG)
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainRetentionBytesForRepartitionConfigDuringValidation() {
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupRepartitionTopicConfig(topic1, 1),
            configWithoutKey(repartitionTopicConfig(), TopicConfig.RETENTION_BYTES_CONFIG)
        );
    }

    private Config configWithoutKey(final Map<String, String> config, final String key) {
        return new Config(config.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(key))
            .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet())
        );
    }

    private void shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(final InternalTopicConfig streamsSideTopicConfig,
                                                                                     final Config brokerSideTopicConfig) {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            Time.SYSTEM,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(brokerSideTopicConfig);
        final ConfigResource topicResource1 = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource1)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource1, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, streamsSideTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenTimeoutIsExceededDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new TimeoutException());
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            TimeoutException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenFuturesNeverCompleteDuringValidation() {
        final AdminClient admin = EasyMock.createNiceMock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFutureThatNeverCompletes = new KafkaFutureImpl<>();
        EasyMock.expect(admin.describeTopics(Collections.singleton(topic1)))
            .andStubAnswer(() -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFutureThatNeverCompletes))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        EasyMock.expect(admin.describeConfigs(Collections.singleton(topicResource)))
            .andStubAnswer(() -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        EasyMock.replay(admin);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            TimeoutException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    private Map<String, String> repartitionTopicConfig() {
        return mkMap(
            mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
            mkEntry(TopicConfig.RETENTION_MS_CONFIG, "-1"),
            mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
        );
    }

    private Map<String, String> unwindowedChangelogConfig() {
        return mkMap(
            mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        );
    }

    private Map<String, String> windowedChangelogConfig(final long retentionMs) {
        return mkMap(
            mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE),
            mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)),
            mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
        );
    }

    private void setupTopicInMockAdminClient(final String topic, final Map<String, String> topicConfig) {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            topicConfig
        );
    }

    private InternalTopicConfig setupUnwindowedChangelogTopicConfig(final String topicName,
                                                                    final int partitionCount) {
        final InternalTopicConfig internalTopicConfig =
            new UnwindowedChangelogTopicConfig(topicName, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(partitionCount);
        return internalTopicConfig;
    }

    private InternalTopicConfig setupWindowedChangelogTopicConfig(final String topicName,
                                                                  final int partitionCount,
                                                                  final long retentionMs) {
        final InternalTopicConfig internalTopicConfig = new WindowedChangelogTopicConfig(
            topicName,
            mkMap(mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)))
        );
        internalTopicConfig.setNumberOfPartitions(partitionCount);
        return internalTopicConfig;
    }

    private InternalTopicConfig setupRepartitionTopicConfig(final String topicName,
                                                            final int partitionCount) {
        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topicName, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(partitionCount);
        return internalTopicConfig;
    }

    private static class MockCreateTopicsResult extends CreateTopicsResult {

        MockCreateTopicsResult(final Map<String, KafkaFuture<TopicMetadataAndConfig>> futures) {
            super(futures);
        }
    }

    private static class MockDescribeTopicsResult extends DescribeTopicsResult {

        MockDescribeTopicsResult(final Map<String, KafkaFuture<TopicDescription>> futures) {
            super(futures);
        }
    }

    private static class MockDescribeConfigsResult extends DescribeConfigsResult {

        MockDescribeConfigsResult(final Map<ConfigResource, KafkaFuture<Config>> futures) {
            super(futures);
        }
    }
}
