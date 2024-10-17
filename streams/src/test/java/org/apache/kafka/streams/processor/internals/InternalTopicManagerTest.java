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
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.InternalTopicManager.ValidationResult;

import org.apache.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
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
    private final MockTime time = new MockTime(0);

    private final Map<String, Object> config = new HashMap<String, Object>() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.host() + ":" + broker1.port());
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 16384);
            put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 100);
            put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, 10);
        }
    };

    @BeforeEach
    public void init() {
        threadName = Thread.currentThread().getName();

        mockAdminClient = new MockAdminClient(cluster, broker1);
        internalTopicManager = new InternalTopicManager(
            time,
            mockAdminClient,
            new StreamsConfig(config)
        );
    }

    @AfterEach
    public void shutdown() {
        mockAdminClient.close();
    }

    @Test
    public void shouldCreateTopics() throws Exception {
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);

        internalTopicManager.setup(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2)
        ));

        final Set<String> newlyCreatedTopics = mockAdminClient.listTopics().names().get();
        assertThat(newlyCreatedTopics.size(), is(2));
        assertThat(newlyCreatedTopics, hasItem(topic1));
        assertThat(newlyCreatedTopics, hasItem(topic2));
    }

    @Test
    public void shouldNotCreateTopicsWithEmptyInput() throws Exception {

        internalTopicManager.setup(Collections.emptyMap());

        final Set<String> newlyCreatedTopics = mockAdminClient.listTopics().names().get();
        assertThat(newlyCreatedTopics, empty());
    }

    @Test
    public void shouldOnlyRetryNotSuccessfulFuturesDuringSetup() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture = new KafkaFutureImpl<>();
        createTopicFailFuture.completeExceptionally(new TopicExistsException("exists"));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicSuccessfulFuture = new KafkaFutureImpl<>();
        createTopicSuccessfulFuture.complete(
            new TopicMetadataAndConfig(Uuid.randomUuid(), 1, 1, new Config(Collections.emptyList()))
        );
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        final NewTopic newTopic1 = newTopic(topic1, internalTopicConfig1, streamsConfig);
        final NewTopic newTopic2 = newTopic(topic2, internalTopicConfig2, streamsConfig);
        when(admin.createTopics(Set.of(newTopic1, newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicSuccessfulFuture),
                mkEntry(topic2, createTopicFailFuture)
            )));
        when(admin.createTopics(Set.of(newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic2, createTopicSuccessfulFuture)
            )));

        topicManager.setup(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2)
        ));
    }

    @Test
    public void shouldRetryCreateTopicWhenCreationTimesOut() {
        shouldRetryCreateTopicWhenRetriableExceptionIsThrown(new TimeoutException("timed out"));
    }

    @Test
    public void shouldRetryCreateTopicWhenTopicNotYetDeleted() {
        shouldRetryCreateTopicWhenRetriableExceptionIsThrown(new TopicExistsException("exists"));
    }

    private void shouldRetryCreateTopicWhenRetriableExceptionIsThrown(final Exception retriableException) {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture = new KafkaFutureImpl<>();
        createTopicFailFuture.completeExceptionally(retriableException);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicSuccessfulFuture = new KafkaFutureImpl<>();
        createTopicSuccessfulFuture.complete(
            new TopicMetadataAndConfig(Uuid.randomUuid(), 1, 1, new Config(Collections.emptyList()))
        );
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final NewTopic newTopic = newTopic(topic1, internalTopicConfig, streamsConfig);
        when(admin.createTopics(Set.of(newTopic)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicSuccessfulFuture)
            )))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                    mkEntry(topic2, createTopicSuccessfulFuture)
            )));

        topicManager.setup(mkMap(
            mkEntry(topic1, internalTopicConfig)
        ));
    }

    @Test
    public void shouldThrowInformativeExceptionForOlderBrokers() {
        final AdminClient admin = new MockAdminClient() {
            @Override
            public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics,
                                                   final CreateTopicsOptions options) {
                final CreatableTopic topicToBeCreated = new CreatableTopic();
                topicToBeCreated.setAssignments(new CreatableReplicaAssignmentCollection());
                topicToBeCreated.setNumPartitions((short) 1);
                // set unsupported replication factor for older brokers
                topicToBeCreated.setReplicationFactor((short) -1);

                final CreatableTopicCollection topicsToBeCreated = new CreatableTopicCollection();
                topicsToBeCreated.add(topicToBeCreated);

                try {
                    new CreateTopicsRequest.Builder(
                        new CreateTopicsRequestData()
                            .setTopics(topicsToBeCreated)
                            .setTimeoutMs(0)
                            .setValidateOnly(options.shouldValidateOnly()))
                        .build((short) 3); // pass in old unsupported request version for old brokers

                    throw new IllegalStateException("Building CreateTopicRequest should have thrown.");
                } catch (final UnsupportedVersionException expected) {
                    final KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                    future.completeExceptionally(expected);

                    return new CreateTopicsResult(Collections.singletonMap(topic1, future)) { };
                }
            }
        };

        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);

        final InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);

        final StreamsException exception = assertThrows(
            StreamsException.class,
            () -> topicManager.makeReady(Collections.singletonMap(topic1, topicConfig))
        );
        assertThat(
            exception.getMessage(),
            equalTo("Could not create topic " + topic1 + ", because brokers don't support configuration replication.factor=-1."
                + " You can change the replication.factor config or upgrade your brokers to version 2.4 or newer to avoid this error."));
    }

    @Test
    public void shouldThrowTimeoutExceptionInGetPartitionInfo() {
        setupTopicInMockAdminClient(topic1, Collections.emptyMap());
        final MockTime time = new MockTime(5);
        mockAdminClient.timeoutNextRequest(Integer.MAX_VALUE);

        final InternalTopicManager internalTopicManager =
            new InternalTopicManager(time, mockAdminClient, new StreamsConfig(config));

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> internalTopicManager.getTopicPartitionInfo(Collections.singleton(topic1))
        );

        assertThat(
            exception.getMessage(),
            is("Could not create topics within 50 milliseconds. This can happen if the Kafka cluster is temporarily not available.")
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionIfTopicExistsDuringSetup() {
        setupTopicInMockAdminClient(topic1, Collections.emptyMap());
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 15
        );
        final InternalTopicManager internalTopicManager =
            new InternalTopicManager(time, mockAdminClient, new StreamsConfig(config));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final TimeoutException exception = assertThrows(
            TimeoutException.class,
            () -> internalTopicManager.setup(Collections.singletonMap(topic1, internalTopicConfig))
        );

        assertThat(
            exception.getMessage(),
            is("Could not create internal topics within " +
                    (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 2 +
                " milliseconds. This can happen if the Kafka cluster is temporarily not available or a topic is marked" +
                    " for deletion and the broker did not complete its deletion within the timeout." +
                    " The last errors seen per topic are:" +
                    " {" + topic1 + "=org.apache.kafka.common.errors.TopicExistsException: Topic test_topic exists already.}")
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionIfGetPartitionInfoHasTopicDescriptionTimeout() {
        mockAdminClient.timeoutNextRequest(1);

        final InternalTopicManager internalTopicManager =
                new InternalTopicManager(time, mockAdminClient, new StreamsConfig(config));
        try {
            final Set<String> topic1set = new HashSet<>(Collections.singletonList(topic1));
            internalTopicManager.getTopicPartitionInfo(topic1set, null);

        } catch (final TimeoutException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }

        mockAdminClient.timeoutNextRequest(1);

        try {
            final Set<String> topic2set = new HashSet<>(Collections.singletonList(topic2));
            internalTopicManager.getTopicPartitionInfo(topic2set, null);

        } catch (final TimeoutException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void shouldThrowTimeoutExceptionIfGetNumPartitionsHasTopicDescriptionTimeout() {
        mockAdminClient.timeoutNextRequest(1);

        final InternalTopicManager internalTopicManager =
                new InternalTopicManager(time, mockAdminClient, new StreamsConfig(config));
        try {
            final Set<String> topic1set = new HashSet<String>(Arrays.asList(topic1));
            final Set<String> topic2set = new HashSet<String>(Arrays.asList(topic2));

            internalTopicManager.getNumPartitions(topic1set, topic2set);

        } catch (final TimeoutException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }

        mockAdminClient.timeoutNextRequest(1);

        try {
            final Set<String> topic1set = new HashSet<String>(Arrays.asList(topic1));
            final Set<String> topic2set = new HashSet<String>(Arrays.asList(topic2));

            internalTopicManager.getNumPartitions(topic1set, topic2set);

        } catch (final TimeoutException expected) {
            assertEquals(TimeoutException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void shouldThrowWhenCreateTopicsThrowsUnexpectedException() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture = new KafkaFutureImpl<>();
        createTopicFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        final NewTopic newTopic = newTopic(topic1, internalTopicConfig, streamsConfig);
        when(admin.createTopics(Set.of(newTopic)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicFailFuture)
            )));

        assertThrows(StreamsException.class, () -> topicManager.setup(mkMap(
            mkEntry(topic1, internalTopicConfig)
        )));
    }

    @Test
    public void shouldThrowWhenCreateTopicsResultsDoNotContainTopic() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final NewTopic newTopic = newTopic(topic1, internalTopicConfig, streamsConfig);
        when(admin.createTopics(Set.of(newTopic)))
            .thenAnswer(answer -> new MockCreateTopicsResult(Collections.singletonMap(topic2, new KafkaFutureImpl<>())));

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.setup(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenCreateTopicExceedsTimeout() {
        final AdminClient admin = mock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture = new KafkaFutureImpl<>();
        createTopicFailFuture.completeExceptionally(new TimeoutException());
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final NewTopic newTopic = newTopic(topic1, internalTopicConfig, streamsConfig);
        when(admin.createTopics(Set.of(newTopic)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(mkEntry(topic1, createTopicFailFuture))));

        assertThrows(
            TimeoutException.class,
            () -> topicManager.setup(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenFuturesNeverCompleteDuringSetup() {
        final AdminClient admin = mock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFutureThatNeverCompletes = new KafkaFutureImpl<>();
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final NewTopic newTopic = newTopic(topic1, internalTopicConfig, streamsConfig);
        when(admin.createTopics(Set.of(newTopic)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(mkEntry(topic1, createTopicFutureThatNeverCompletes))));

        assertThrows(
            TimeoutException.class,
            () -> topicManager.setup(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldCleanUpWhenUnexpectedExceptionIsThrownDuringSetup() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        setupCleanUpScenario(admin, streamsConfig, internalTopicConfig1, internalTopicConfig2);
        final KafkaFutureImpl<Void> deleteTopicSuccessfulFuture = new KafkaFutureImpl<>();
        deleteTopicSuccessfulFuture.complete(null);
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicSuccessfulFuture))));

        assertThrows(
            StreamsException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    @Test
    public void shouldCleanUpWhenCreateTopicsResultsDoNotContainTopic() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture1 = new KafkaFutureImpl<>();
        createTopicFailFuture1.completeExceptionally(new TopicExistsException("exists"));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicSuccessfulFuture = new KafkaFutureImpl<>();
        createTopicSuccessfulFuture.complete(
            new TopicMetadataAndConfig(Uuid.randomUuid(), 1, 1, new Config(Collections.emptyList()))
        );
        final NewTopic newTopic1 = newTopic(topic1, internalTopicConfig1, streamsConfig);
        final NewTopic newTopic2 = newTopic(topic2, internalTopicConfig2, streamsConfig);
        when(admin.createTopics(Set.of(newTopic1, newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicSuccessfulFuture),
                mkEntry(topic2, createTopicFailFuture1)
            )));
        when(admin.createTopics(Set.of(newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic3, createTopicSuccessfulFuture)
            )));
        final KafkaFutureImpl<Void> deleteTopicSuccessfulFuture = new KafkaFutureImpl<>();
        deleteTopicSuccessfulFuture.complete(null);
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicSuccessfulFuture))));

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    @Test
    public void shouldCleanUpWhenCreateTopicsTimesOut() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture1 = new KafkaFutureImpl<>();
        createTopicFailFuture1.completeExceptionally(new TopicExistsException("exists"));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicSuccessfulFuture = new KafkaFutureImpl<>();
        createTopicSuccessfulFuture.complete(
            new TopicMetadataAndConfig(Uuid.randomUuid(), 1, 1, new Config(Collections.emptyList()))
        );
        final NewTopic newTopic1 = newTopic(topic1, internalTopicConfig1, streamsConfig);
        final NewTopic newTopic2 = newTopic(topic2, internalTopicConfig2, streamsConfig);
        when(admin.createTopics(Set.of(newTopic1, newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicSuccessfulFuture),
                mkEntry(topic2, createTopicFailFuture1)
            )));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFutureThatNeverCompletes = new KafkaFutureImpl<>();
        when(admin.createTopics(Set.of(newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(mkEntry(topic2, createTopicFutureThatNeverCompletes))));
        final KafkaFutureImpl<Void> deleteTopicSuccessfulFuture = new KafkaFutureImpl<>();
        deleteTopicSuccessfulFuture.complete(null);
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicSuccessfulFuture))));

        assertThrows(
            TimeoutException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    @Test
    public void shouldRetryDeleteTopicWhenTopicUnknown() {
        shouldRetryDeleteTopicWhenRetriableException(new UnknownTopicOrPartitionException());
    }

    @Test
    public void shouldRetryDeleteTopicWhenLeaderNotAvailable() {
        shouldRetryDeleteTopicWhenRetriableException(new LeaderNotAvailableException("leader not available"));
    }

    @Test
    public void shouldRetryDeleteTopicWhenFutureTimesOut() {
        shouldRetryDeleteTopicWhenRetriableException(new TimeoutException("timed out"));
    }

    private void shouldRetryDeleteTopicWhenRetriableException(final Exception retriableException) {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        setupCleanUpScenario(admin, streamsConfig, internalTopicConfig1, internalTopicConfig2);
        final KafkaFutureImpl<Void> deleteTopicFailFuture = new KafkaFutureImpl<>();
        deleteTopicFailFuture.completeExceptionally(retriableException);
        final KafkaFutureImpl<Void> deleteTopicSuccessfulFuture = new KafkaFutureImpl<>();
        deleteTopicSuccessfulFuture.complete(null);
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicFailFuture))))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicSuccessfulFuture))));

        assertThrows(
            StreamsException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenFuturesNeverCompleteDuringCleanUp() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        setupCleanUpScenario(admin, streamsConfig, internalTopicConfig1, internalTopicConfig2);
        final KafkaFutureImpl<Void> deleteTopicFutureThatNeverCompletes = new KafkaFutureImpl<>();
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicFutureThatNeverCompletes))));

        assertThrows(
            TimeoutException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    @Test
    public void shouldThrowWhenDeleteTopicsThrowsUnexpectedException() {
        final AdminClient admin = mock(AdminClient.class);
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        final InternalTopicManager topicManager = new InternalTopicManager(time, admin, streamsConfig);
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);
        setupCleanUpScenario(admin, streamsConfig, internalTopicConfig1, internalTopicConfig2);
        final KafkaFutureImpl<Void> deleteTopicFailFuture = new KafkaFutureImpl<>();
        deleteTopicFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        when(admin.deleteTopics(Set.of(topic1)))
            .thenAnswer(answer -> new MockDeleteTopicsResult(mkMap(mkEntry(topic1, deleteTopicFailFuture))));

        assertThrows(
            StreamsException.class,
            () -> topicManager.setup(mkMap(
                mkEntry(topic1, internalTopicConfig1),
                mkEntry(topic2, internalTopicConfig2)
            ))
        );
    }

    private void setupCleanUpScenario(final AdminClient admin, final StreamsConfig streamsConfig, final InternalTopicConfig internalTopicConfig1, final InternalTopicConfig internalTopicConfig2) {
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture1 = new KafkaFutureImpl<>();
        createTopicFailFuture1.completeExceptionally(new TopicExistsException("exists"));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicFailFuture2 = new KafkaFutureImpl<>();
        createTopicFailFuture2.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        final KafkaFutureImpl<TopicMetadataAndConfig> createTopicSuccessfulFuture = new KafkaFutureImpl<>();
        createTopicSuccessfulFuture.complete(
            new TopicMetadataAndConfig(Uuid.randomUuid(), 1, 1, new Config(Collections.emptyList()))
        );
        final NewTopic newTopic1 = newTopic(topic1, internalTopicConfig1, streamsConfig);
        final NewTopic newTopic2 = newTopic(topic2, internalTopicConfig2, streamsConfig);
        when(admin.createTopics(Set.of(newTopic1, newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic1, createTopicSuccessfulFuture),
                mkEntry(topic2, createTopicFailFuture1)
            )));
        when(admin.createTopics(Set.of(newTopic2)))
            .thenAnswer(answer -> new MockCreateTopicsResult(mkMap(
                mkEntry(topic2, createTopicFailFuture2)
            )));
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
    public void shouldReturnCorrectPartitionInfo() {
        final TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList());
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(topicPartitionInfo),
            null);

        final Map<String, List<TopicPartitionInfo>> ret = internalTopicManager.getTopicPartitionInfo(Collections.singleton(topic1));
        assertEquals(Collections.singletonMap(topic1, Collections.singletonList(topicPartitionInfo)), ret);
    }

    @Test
    public void shouldCreateRequiredTopics() throws Exception {
        final InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig2 = new UnwindowedUnversionedChangelogTopicConfig(topic2, Collections.emptyMap());
        topicConfig2.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig3 = new WindowedChangelogTopicConfig(topic3, Collections.emptyMap(), 10);
        topicConfig3.setNumberOfPartitions(1);
        final InternalTopicConfig topicConfig4 = new VersionedChangelogTopicConfig(topic4, Collections.emptyMap(), 12);
        topicConfig4.setNumberOfPartitions(1);

        internalTopicManager.makeReady(Collections.singletonMap(topic1, topicConfig));
        internalTopicManager.makeReady(Collections.singletonMap(topic2, topicConfig2));
        internalTopicManager.makeReady(Collections.singletonMap(topic3, topicConfig3));
        internalTopicManager.makeReady(Collections.singletonMap(topic4, topicConfig4));

        assertEquals(Set.of(topic1, topic2, topic3, topic4), mockAdminClient.listTopics().names().get());
        assertEquals(new TopicDescription(topic1, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic1)).topicNameValues().get(topic1).get());
        assertEquals(new TopicDescription(topic2, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic2)).topicNameValues().get(topic2).get());
        assertEquals(new TopicDescription(topic3, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic3)).topicNameValues().get(topic3).get());
        assertEquals(new TopicDescription(topic4, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic4)).topicNameValues().get(topic4).get());

        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
        final ConfigResource resource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
        final ConfigResource resource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);
        final ConfigResource resource4 = new ConfigResource(ConfigResource.Type.TOPIC, topic4);

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
        assertEquals(
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
            mockAdminClient.describeConfigs(Collections.singleton(resource4)).values().get(resource4).get().get(TopicConfig.CLEANUP_POLICY_CONFIG)
        );
    }

    @Test
    public void shouldCompleteTopicValidationOnRetry() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
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
        when(admin.describeTopics(Set.of(topic1, topic2)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(
                mkEntry(topic1, topicDescriptionSuccessFuture),
                mkEntry(topic2, topicDescriptionFailFuture)
            )));
        when(admin.createTopics(Collections.singleton(new NewTopic(topic2, Optional.of(1), Optional.of((short) 1))
            .configs(mkMap(mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                                 mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"))))))
            .thenAnswer(answer -> new MockCreateTopicsResult(Collections.singletonMap(topic2, topicCreationFuture)));
        when(admin.describeTopics(Collections.singleton(topic2)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(Collections.singletonMap(topic2, topicDescriptionSuccessFuture)));

        final InternalTopicConfig topicConfig = new UnwindowedUnversionedChangelogTopicConfig(topic1, Collections.emptyMap());
        topicConfig.setNumberOfPartitions(1);
        final InternalTopicConfig topic2Config = new UnwindowedUnversionedChangelogTopicConfig(topic2, Collections.emptyMap());
        topic2Config.setNumberOfPartitions(1);
        topicManager.makeReady(mkMap(
            mkEntry(topic1, topicConfig),
            mkEntry(topic2, topic2Config)
        ));
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
            time,
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
        mockAdminClient.timeoutNextRequest(5);

        final InternalTopicManager topicManager = new InternalTopicManager(
                new AutoAdvanceMockTime(time),
                mockAdminClient,
                new StreamsConfig(config)
        );

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
            fail("Should have thrown TimeoutException.");
        } catch (final TimeoutException expected) {
            assertThat(expected.getMessage(), is("Could not create topics within 50 milliseconds. " +
                    "This can happen if the Kafka cluster is temporarily not available."));
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

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(InternalTopicManager.class)) {
            appender.setClassLogger(InternalTopicManager.class, Level.DEBUG);
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
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );

        final KafkaFutureImpl<TopicDescription> topicDescriptionLeaderNotAvailableFuture = new KafkaFutureImpl<>();
        topicDescriptionLeaderNotAvailableFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<TopicDescription> topicDescriptionUnknownTopicFuture = new KafkaFutureImpl<>();
        topicDescriptionUnknownTopicFuture.completeExceptionally(new UnknownTopicOrPartitionException("Unknown Topic!"));
        final KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> topicCreationFuture = new KafkaFutureImpl<>();
        topicCreationFuture.complete(mock(CreateTopicsResult.TopicMetadataAndConfig.class));

        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionLeaderNotAvailableFuture)));
        // we would not need to call create-topics for the first time
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionUnknownTopicFuture)));
        when(admin.createTopics(Collections.singleton(
                new NewTopic(topic1, Optional.of(1), Optional.of((short) 1))
            .configs(mkMap(mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                mkEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime"),
                mkEntry(TopicConfig.SEGMENT_BYTES_CONFIG, "52428800"),
                mkEntry(TopicConfig.RETENTION_MS_CONFIG, "-1"))))))
            .thenAnswer(answer -> new MockCreateTopicsResult(Collections.singletonMap(topic1, topicCreationFuture)));

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
    }

    @Test
    public void shouldCompleteValidateWhenTopicLeaderNotAvailableAndThenDescribeSuccess() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
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

        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionFailFuture)));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionSuccessFuture)));

        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        topicManager.makeReady(Collections.singletonMap(topic1, internalTopicConfig));
    }

    @Test
    public void shouldThrowExceptionWhenKeepsTopicLeaderNotAvailable() {
        final AdminClient admin = mock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 15
        );
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );

        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));

        // simulate describeTopics got LeaderNotAvailableException
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(
                Collections.singletonMap(topic1, topicDescriptionFailFuture)));

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
    }

    @Test
    public void shouldExhaustRetriesOnMarkedForDeletionTopic() {
        mockAdminClient.addTopic(
            false,
            topic1,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);
        mockAdminClient.markTopicForDeletion(topic1);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 15
        );

        final InternalTopicManager internalTopicManager =
            new InternalTopicManager(time, mockAdminClient, new StreamsConfig(config));
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
    public void shouldReportMisconfigurationsOfCleanupPolicyForUnwindowedUnversionedChangelogTopics() {
        final Map<String, String> unwindowedUnversionedChangelogConfigWithDeleteCleanupPolicy = unwindowedUnversionedChangelogConfig();
        unwindowedUnversionedChangelogConfigWithDeleteCleanupPolicy.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_DELETE
        );
        setupTopicInMockAdminClient(topic1, unwindowedUnversionedChangelogConfigWithDeleteCleanupPolicy);
        final Map<String, String> unwindowedUnversionedChangelogConfigWithDeleteCompactCleanupPolicy = unwindowedUnversionedChangelogConfig();
        unwindowedUnversionedChangelogConfigWithDeleteCompactCleanupPolicy.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
        );
        setupTopicInMockAdminClient(topic2, unwindowedUnversionedChangelogConfigWithDeleteCompactCleanupPolicy);
        setupTopicInMockAdminClient(topic3, unwindowedUnversionedChangelogConfig());
        final InternalTopicConfig internalTopicConfig1 = setupUnwindowedUnversionedChangelogTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupUnwindowedUnversionedChangelogTopicConfig(topic2, 1);
        final InternalTopicConfig internalTopicConfig3 = setupUnwindowedUnversionedChangelogTopicConfig(topic3, 1);

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
    public void shouldReportMisconfigurationsOfCleanupPolicyForVersionedChangelogTopics() {
        final long compactionLagMs = 1000;
        final long shorterCompactionLagMs = 900;
        setupTopicInMockAdminClient(topic1, versionedChangelogConfig(compactionLagMs));
        setupTopicInMockAdminClient(topic2, versionedChangelogConfig(shorterCompactionLagMs));
        final Map<String, String> versionedChangelogConfigCleanupPolicyDelete = versionedChangelogConfig(compactionLagMs);
        versionedChangelogConfigCleanupPolicyDelete.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        setupTopicInMockAdminClient(topic3, versionedChangelogConfigCleanupPolicyDelete);
        final Map<String, String> versionedChangelogConfigCleanupPolicyCompactAndDelete = versionedChangelogConfig(compactionLagMs);
        versionedChangelogConfigCleanupPolicyCompactAndDelete.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + TopicConfig.CLEANUP_POLICY_DELETE);
        setupTopicInMockAdminClient(topic4, versionedChangelogConfigCleanupPolicyCompactAndDelete);
        final InternalTopicConfig internalTopicConfig1 = setupVersionedChangelogTopicConfig(topic1, 1, compactionLagMs);
        final InternalTopicConfig internalTopicConfig2 = setupVersionedChangelogTopicConfig(topic2, 1, compactionLagMs);
        final InternalTopicConfig internalTopicConfig3 = setupVersionedChangelogTopicConfig(topic3, 1, compactionLagMs);
        final InternalTopicConfig internalTopicConfig4 = setupVersionedChangelogTopicConfig(topic4, 1, compactionLagMs);

        final ValidationResult validationResult = internalTopicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2),
            mkEntry(topic3, internalTopicConfig3),
            mkEntry(topic4, internalTopicConfig4)
        ));

        final Map<String, List<String>> misconfigurationsForTopics = validationResult.misconfigurationsForTopics();
        assertThat(validationResult.missingTopics(), empty());
        assertThat(misconfigurationsForTopics.size(), is(3));
        assertThat(misconfigurationsForTopics, hasKey(topic2));
        assertThat(misconfigurationsForTopics.get(topic2).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic2).get(0),
            is("Min compaction lag (" + TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG + ") of existing internal topic " +
                topic2 + " is " + shorterCompactionLagMs + " but should be " + compactionLagMs + " or larger.")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic3));
        assertThat(misconfigurationsForTopics.get(topic3).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic3).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic " + topic3 + " should not contain \""
                + TopicConfig.CLEANUP_POLICY_DELETE + "\".")
        );
        assertThat(misconfigurationsForTopics, hasKey(topic4));
        assertThat(misconfigurationsForTopics.get(topic4).size(), is(1));
        assertThat(
            misconfigurationsForTopics.get(topic4).get(0),
            is("Cleanup policy (" + TopicConfig.CLEANUP_POLICY_CONFIG + ") of existing internal topic " + topic4 + " should not contain \""
                + TopicConfig.CLEANUP_POLICY_DELETE + "\".")
        );
        assertThat(misconfigurationsForTopics, not(hasKey(topic1)));
    }

    @Test
    public void shouldReportMisconfigurationsOfCleanupPolicyForRepartitionTopics() {
        final long retentionMs = 1000;
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        final Map<String, String> repartitionTopicConfigCleanupPolicyCompact = repartitionTopicConfig();
        repartitionTopicConfigCleanupPolicyCompact.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        setupTopicInMockAdminClient(topic2, repartitionTopicConfigCleanupPolicyCompact);
        final Map<String, String> repartitionTopicConfigCleanupPolicyCompactAndDelete = repartitionTopicConfig();
        repartitionTopicConfigCleanupPolicyCompactAndDelete.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
        );
        setupTopicInMockAdminClient(topic3, repartitionTopicConfigCleanupPolicyCompactAndDelete);
        final Map<String, String> repartitionTopicConfigWithFiniteRetentionMs = repartitionTopicConfig();
        repartitionTopicConfigWithFiniteRetentionMs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));
        setupTopicInMockAdminClient(topic4, repartitionTopicConfigWithFiniteRetentionMs);
        final Map<String, String> repartitionTopicConfigWithRetentionBytesSet = repartitionTopicConfig();
        repartitionTopicConfigWithRetentionBytesSet.put(TopicConfig.RETENTION_BYTES_CONFIG, "1024");
        setupTopicInMockAdminClient(topic5, repartitionTopicConfigWithRetentionBytesSet);
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
        final Map<String, String> windowedChangelogConfig = windowedChangelogConfig(shorterRetentionMs);
        windowedChangelogConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1024");
        setupTopicInMockAdminClient(topic1, windowedChangelogConfig);
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
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        final InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic1, Collections.emptyMap());

        assertThrows(
            IllegalStateException.class,
            () -> internalTopicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldNotThrowExceptionIfTopicExistsWithDifferentReplication() {
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        // attempt to create it again with replication 1
        final InternalTopicManager internalTopicManager2 = new InternalTopicManager(
            time,
            mockAdminClient,
            new StreamsConfig(config)
        );

        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);
        final ValidationResult validationResult =
            internalTopicManager2.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldRetryWhenCallsThrowTimeoutExceptionDuringValidation() {
        setupTopicInMockAdminClient(topic1, repartitionTopicConfig());
        mockAdminClient.timeoutNextRequest(2);
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = internalTopicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldOnlyRetryDescribeTopicsWhenDescribeTopicsThrowsLeaderNotAvailableExceptionDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
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
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldOnlyRetryDescribeConfigsWhenDescribeConfigsThrowsLeaderNotAvailableExceptionDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigsFailFuture = new KafkaFutureImpl<>();
        topicConfigsFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigsFailFuture))))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        final ValidationResult validationResult = topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldOnlyRetryNotSuccessfulFuturesDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new LeaderNotAvailableException("Leader Not Available!"));
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture1 = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture1.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture2 = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture2.complete(new TopicDescription(
            topic2,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        when(admin.describeTopics(Set.of(topic1, topic2)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(
                mkEntry(topic1, topicDescriptionSuccessfulFuture1),
                mkEntry(topic2, topicDescriptionFailFuture)
            )));
        when(admin.describeTopics(Set.of(topic2)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(
                mkEntry(topic2, topicDescriptionSuccessfulFuture2)
            )));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource1 = new ConfigResource(Type.TOPIC, topic1);
        final ConfigResource topicResource2 = new ConfigResource(Type.TOPIC, topic2);
        when(admin.describeConfigs(Set.of(topicResource1, topicResource2)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(
                mkEntry(topicResource1, topicConfigSuccessfulFuture),
                mkEntry(topicResource2, topicConfigSuccessfulFuture)
            )));
        final InternalTopicConfig internalTopicConfig1 = setupRepartitionTopicConfig(topic1, 1);
        final InternalTopicConfig internalTopicConfig2 = setupRepartitionTopicConfig(topic2, 1);

        final ValidationResult validationResult = topicManager.validate(mkMap(
            mkEntry(topic1, internalTopicConfig1),
            mkEntry(topic2, internalTopicConfig2)
        ));

        assertThat(validationResult.missingTopics(), empty());
        assertThat(validationResult.misconfigurationsForTopics(), anEmptyMap());
    }

    @Test
    public void shouldThrowWhenDescribeTopicsThrowsUnexpectedExceptionDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFailFuture = new KafkaFutureImpl<>();
        topicDescriptionFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(Throwable.class, () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig)));
    }

    @Test
    public void shouldThrowWhenDescribeConfigsThrowsUnexpectedExceptionDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap()));
        final KafkaFutureImpl<Config> configDescriptionFailFuture = new KafkaFutureImpl<>();
        configDescriptionFailFuture.completeExceptionally(new IllegalStateException("Nobody expects the Spanish inquisition"));
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, configDescriptionFailFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(Throwable.class, () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig)));
    }

    @Test
    public void shouldThrowWhenTopicDescriptionsDoNotContainTopicDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic2, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(new Config(Collections.emptySet()));
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotContainTopicDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(new Config(Collections.emptySet()));
        final ConfigResource topicResource1 = new ConfigResource(Type.TOPIC, topic1);
        final ConfigResource topicResource2 = new ConfigResource(Type.TOPIC, topic2);
        when(admin.describeConfigs(Collections.singleton(topicResource1)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource2, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowWhenConfigDescriptionsDoNotCleanupPolicyForUnwindowedUnversionedConfigDuringValidation() {
        shouldThrowWhenConfigDescriptionsDoNotContainConfigDuringValidation(
            setupUnwindowedUnversionedChangelogTopicConfig(topic1, 1),
            configWithoutKey(unwindowedUnversionedChangelogConfig(), TopicConfig.CLEANUP_POLICY_CONFIG)
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
        final AdminClient admin = mock(AdminClient.class);
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionSuccessfulFuture = new KafkaFutureImpl<>();
        topicDescriptionSuccessfulFuture.complete(new TopicDescription(
            topic1,
            false,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList()))
        ));
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionSuccessfulFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(brokerSideTopicConfig);
        final ConfigResource topicResource1 = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource1)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource1, topicConfigSuccessfulFuture))));

        assertThrows(
            IllegalStateException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, streamsSideTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenTimeoutIsExceededDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
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
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFailFuture))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            TimeoutException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenFuturesNeverCompleteDuringValidation() {
        final AdminClient admin = mock(AdminClient.class);
        final MockTime time = new MockTime(
            (Integer) config.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) / 3
        );
        final InternalTopicManager topicManager = new InternalTopicManager(
            time,
            admin,
            new StreamsConfig(config)
        );
        final KafkaFutureImpl<TopicDescription> topicDescriptionFutureThatNeverCompletes = new KafkaFutureImpl<>();
        when(admin.describeTopics(Collections.singleton(topic1)))
            .thenAnswer(answer -> new MockDescribeTopicsResult(mkMap(mkEntry(topic1, topicDescriptionFutureThatNeverCompletes))));
        final KafkaFutureImpl<Config> topicConfigSuccessfulFuture = new KafkaFutureImpl<>();
        topicConfigSuccessfulFuture.complete(
            new Config(repartitionTopicConfig().entrySet().stream()
                .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).collect(Collectors.toSet()))
        );
        final ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic1);
        when(admin.describeConfigs(Collections.singleton(topicResource)))
            .thenAnswer(answer -> new MockDescribeConfigsResult(mkMap(mkEntry(topicResource, topicConfigSuccessfulFuture))));
        final InternalTopicConfig internalTopicConfig = setupRepartitionTopicConfig(topic1, 1);

        assertThrows(
            TimeoutException.class,
            () -> topicManager.validate(Collections.singletonMap(topic1, internalTopicConfig))
        );
    }

    private NewTopic newTopic(final String topicName,
                              final InternalTopicConfig topicConfig,
                              final StreamsConfig streamsConfig) {
        return new NewTopic(
            topicName,
            topicConfig.numberOfPartitions(),
            Optional.of(streamsConfig.getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG).shortValue())
        ).configs(topicConfig.properties(
            Collections.emptyMap(),
            streamsConfig.getLong(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG))
        );
    }

    private Map<String, String> repartitionTopicConfig() {
        return mkMap(
            mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
            mkEntry(TopicConfig.RETENTION_MS_CONFIG, "-1"),
            mkEntry(TopicConfig.RETENTION_BYTES_CONFIG, null)
        );
    }

    private Map<String, String> unwindowedUnversionedChangelogConfig() {
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

    private Map<String, String> versionedChangelogConfig(final long compactionLagMs) {
        return mkMap(
            mkEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
            mkEntry(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, String.valueOf(compactionLagMs))
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

    private InternalTopicConfig setupUnwindowedUnversionedChangelogTopicConfig(final String topicName,
                                                                               final int partitionCount) {
        final InternalTopicConfig internalTopicConfig =
            new UnwindowedUnversionedChangelogTopicConfig(topicName, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(partitionCount);
        return internalTopicConfig;
    }

    private InternalTopicConfig setupWindowedChangelogTopicConfig(final String topicName,
                                                                  final int partitionCount,
                                                                  final long retentionMs) {
        final InternalTopicConfig internalTopicConfig = new WindowedChangelogTopicConfig(
            topicName,
            mkMap(mkEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))),
            10
        );
        internalTopicConfig.setNumberOfPartitions(partitionCount);
        return internalTopicConfig;
    }

    private InternalTopicConfig setupVersionedChangelogTopicConfig(final String topicName,
                                                                   final int partitionCount,
                                                                   final long compactionLagMs) {
        final InternalTopicConfig internalTopicConfig = new VersionedChangelogTopicConfig(
            topicName,
            mkMap(mkEntry(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, String.valueOf(compactionLagMs))),
            12
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

    private static class AutoAdvanceMockTime extends MockTime {
        private final MockTime time;

        private AutoAdvanceMockTime(final MockTime time) {
            this.time = time;
        }

        @Override
        public long milliseconds() {
            final long ms = time.milliseconds();
            time.sleep(10L);
            return ms;
        }
    }

    private static class MockCreateTopicsResult extends CreateTopicsResult {
        MockCreateTopicsResult(final Map<String, KafkaFuture<TopicMetadataAndConfig>> futures) {
            super(futures);
        }
    }

    private static class MockDeleteTopicsResult extends DeleteTopicsResult {
        MockDeleteTopicsResult(final Map<String, KafkaFuture<Void>> futures) {
            super(null, futures);
        }
    }

    private static class MockDescribeTopicsResult extends DescribeTopicsResult {
        MockDescribeTopicsResult(final Map<String, KafkaFuture<TopicDescription>> futures) {
            super(null, futures);
        }
    }

    private static class MockDescribeConfigsResult extends DescribeConfigsResult {
        MockDescribeConfigsResult(final Map<ConfigResource, KafkaFuture<Config>> futures) {
            super(futures);
        }
    }


}
