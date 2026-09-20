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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class KafkaStreamsStaticMemberIntegrationTest {

    private static MockTime mockTime;

    protected static final String INPUT_TOPIC = "inputTopic";
    protected static final String OUTPUT_TOPIC = "outputTopic";
    protected static final String GROUP_INSTANCE_ID = "someGroupInstance";

    protected Properties streamsConfig;
    protected static KafkaStreams streams;
    protected static Admin adminClient;
    protected Properties commonClientConfig;
    private Properties producerConfig;
    protected Properties resultConsumerConfig;
    private final File testFolder = TestUtils.tempDirectory();

    public static final EmbeddedKafkaCluster CLUSTER;

    static {
        final Properties brokerProps = new Properties();
        brokerProps.setProperty(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        Utils.closeQuietly(adminClient, "admin");
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testName) throws Exception {
        mockTime = CLUSTER.time;

        final String appID = safeUniqueTestName(testName);

        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        streamsConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_INSTANCE_ID);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // In this test, we set the SESSION_TIMEOUT_MS_CONFIG high in order to show that the call to
        // `close(CloseOptions)` can remove the application from the Consumer Groups successfully.
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        streamsConfig.putAll(commonClientConfig);

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.putAll(commonClientConfig);

        resultConsumerConfig = new Properties();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        resultConsumerConfig.putAll(commonClientConfig);

        if (adminClient == null) {
            adminClient = Admin.create(commonClientConfig);
        }

        CLUSTER.deleteAllTopics();
        CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, 1);

        add10InputElements();
    }

    @AfterEach
    public void after() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams = null;
        }
        Utils.delete(testFolder);
    }

    @Test
    public void testStaticMemberJoinRegistersThreadScopedInstanceIdsStreamsProtocol() throws Exception {
        final int numStreamThreads = 2;
        final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);

        streamsConfig.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        streamsConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_INSTANCE_ID);

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(streams);
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        TestUtils.waitForCondition(() -> {
            final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
            return groupDescription.members().size() == numStreamThreads &&
                groupDescription.members().stream().allMatch(member ->
                    member.instanceId()
                        .filter(instanceId -> instanceId.startsWith(GROUP_INSTANCE_ID + "-"))
                        .isPresent() &&
                        member.memberEpoch() >= 0);
        }, "Static streams members did not join with thread-scoped instance ids");
    }

    @Test
    public void testStaticMemberCloseWithLeaveGroupTriggersRebalanceStreamsProtocol() throws Exception {
        final File firstStateDir = TestUtils.tempDirectory();
        final File secondStateDir = TestUtils.tempDirectory();
        final String firstInstanceId = "instance-1";
        final String secondInstanceId = "instance-2";

        final AtomicInteger secondStreamsRebalances = new AtomicInteger(0);

        try (final KafkaStreams streams1 = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), staticStreamsConfig(firstInstanceId, firstStateDir));
             final KafkaStreams streams2 = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), staticStreamsConfig(secondInstanceId, secondStateDir))) {
            streams2.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.REBALANCING) {
                    secondStreamsRebalances.incrementAndGet();
                }
            });

            IntegrationTestUtils.startApplicationAndWaitUntilRunning(Arrays.asList(streams1, streams2));
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

            final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);

            TestUtils.waitForCondition(() -> {
                final StreamsGroupDescription groupDescription =
                    adminClient.describeStreamsGroups(Collections.singletonList(applicationId))
                        .describedGroups()
                        .get(applicationId)
                        .get();

                return groupDescription.members().size() == 2;
            }, "Streams group did not reach 2 static members");

            final int initialStreams2Rebalances = secondStreamsRebalances.get();

            streams1.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
                .withTimeout(Duration.ofSeconds(30)));

            TestUtils.waitForCondition(() -> {
                final StreamsGroupDescription groupDescription =
                    adminClient.describeStreamsGroups(Collections.singletonList(applicationId))
                        .describedGroups()
                        .get(applicationId)
                        .get();

                return groupDescription.members().size() == 1 &&
                    groupDescription.members().stream().allMatch(member ->
                        member.instanceId()
                            .filter(instanceId -> instanceId.startsWith("instance-2-"))
                            .isPresent());
            }, "Streams group did not retain only the remaining static member after LEAVE_GROUP close");

            TestUtils.waitForCondition(
                () -> secondStreamsRebalances.get() > initialStreams2Rebalances,
                "Remaining streams instance did not observe a rebalance after static member left the group"
            );
        } finally {
            Utils.delete(firstStateDir);
            Utils.delete(secondStateDir);
        }
    }

    @Test
    public void testStaticMemberCanRejoinAfterRemainInGroupStreamsProtocol() throws Exception {
        final File firstStateDir = TestUtils.tempDirectory();
        final File secondStateDir = TestUtils.tempDirectory();
        final File restartedStateDir = TestUtils.tempDirectory();
        final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final String firstInstanceId = "instance-1";
        final String secondInstanceId = "instance-2";

        try {
            try (final KafkaStreams firstStreams = new KafkaStreams(
                setupTopologyWithoutIntermediateUserTopic(),
                staticStreamsConfig(firstInstanceId, firstStateDir)
            );
                 final KafkaStreams secondStreams = new KafkaStreams(
                     setupTopologyWithoutIntermediateUserTopic(),
                     staticStreamsConfig(secondInstanceId, secondStateDir)
                 )) {
                IntegrationTestUtils.startApplicationAndWaitUntilRunning(Arrays.asList(firstStreams, secondStreams));
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Streams group did not reach 2 active static members");

                final StreamsGroupDescription initialGroupDescription = describeStreamsGroup(applicationId);
                final String firstMemberId = staticStreamsMemberId(initialGroupDescription, firstInstanceId).orElseThrow();

                firstStreams.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP)
                    .withTimeout(Duration.ofSeconds(30)));

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasStaticStreamsMemberWithMemberIdAndEpoch(
                            groupDescription,
                            firstInstanceId,
                            firstMemberId,
                            StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH
                        ) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Static streams member did not remain in group with static leave epoch");

                try (final KafkaStreams restartedStreams = new KafkaStreams(
                    setupTopologyWithoutIntermediateUserTopic(),
                    staticStreamsConfig(firstInstanceId, restartedStateDir)
                )) {
                    IntegrationTestUtils.startApplicationAndWaitUntilRunning(restartedStreams);

                    TestUtils.waitForCondition(() -> {
                        final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                        final Optional<String> rejoinedMemberId = staticStreamsMemberId(groupDescription, firstInstanceId);

                        return groupDescription.members().size() == 2 &&
                            rejoinedMemberId.isPresent() &&
                            !rejoinedMemberId.get().equals(firstMemberId) &&
                            hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                            hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                    }, "Static streams member did not rejoin with a new member id after REMAIN_IN_GROUP close");
                }
            }
        } finally {
            Utils.delete(firstStateDir);
            Utils.delete(secondStateDir);
            Utils.delete(restartedStateDir);
        }
    }

    @Test
    public void testStaticMemberCanRejoinAfterLeaveGroupStreamsProtocol() throws Exception {
        final File firstStateDir = TestUtils.tempDirectory();
        final File secondStateDir = TestUtils.tempDirectory();
        final File restartedStateDir = TestUtils.tempDirectory();
        final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final String firstInstanceId = "instance-1";
        final String secondInstanceId = "instance-2";

        try {
            try (final KafkaStreams firstStreams = new KafkaStreams(
                setupTopologyWithoutIntermediateUserTopic(),
                staticStreamsConfig(firstInstanceId, firstStateDir)
            );
                 final KafkaStreams secondStreams = new KafkaStreams(
                     setupTopologyWithoutIntermediateUserTopic(),
                     staticStreamsConfig(secondInstanceId, secondStateDir)
                 )) {
                IntegrationTestUtils.startApplicationAndWaitUntilRunning(Arrays.asList(firstStreams, secondStreams));
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Streams group did not reach 2 active static members");

                firstStreams.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
                    .withTimeout(Duration.ofSeconds(30)));

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 1 &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Static streams member did not leave the group with LEAVE_GROUP");

                try (final KafkaStreams restartedStreams = new KafkaStreams(
                    setupTopologyWithoutIntermediateUserTopic(),
                    staticStreamsConfig(firstInstanceId, restartedStateDir)
                )) {
                    IntegrationTestUtils.startApplicationAndWaitUntilRunning(restartedStreams);

                    TestUtils.waitForCondition(() -> {
                        final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                        return groupDescription.members().size() == 2 &&
                            hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                            hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                    }, "Static streams member did not rejoin after LEAVE_GROUP close");
                }
            }
        } finally {
            Utils.delete(firstStateDir);
            Utils.delete(secondStateDir);
            Utils.delete(restartedStateDir);
        }
    }

    @Test
    public void testStaticMemberRemainingInGroupExpiresAndTriggersRebalanceStreamsProtocol() throws Exception {
        final int sessionTimeoutMs = 1000;
        final int heartbeatIntervalMs = 100;
        final File firstStateDir = TestUtils.tempDirectory();
        final File secondStateDir = TestUtils.tempDirectory();
        final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final String firstInstanceId = "instance-1";
        final String secondInstanceId = "instance-2";

        CLUSTER.setGroupHeartbeatTimeout(applicationId, heartbeatIntervalMs);
        CLUSTER.setGroupSessionTimeout(applicationId, sessionTimeoutMs);

        try {
            try (final KafkaStreams firstStreams = new KafkaStreams(
                setupTopologyWithoutIntermediateUserTopic(),
                staticStreamsConfig(firstInstanceId, firstStateDir)
            );
                 final KafkaStreams secondStreams = new KafkaStreams(
                     setupTopologyWithoutIntermediateUserTopic(),
                     staticStreamsConfig(secondInstanceId, secondStateDir)
                 )) {
                IntegrationTestUtils.startApplicationAndWaitUntilRunning(Arrays.asList(firstStreams, secondStreams));
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Streams group did not reach 2 active static members");

                final StreamsGroupDescription initialGroupDescription = describeStreamsGroup(applicationId);
                final String firstMemberId = staticStreamsMemberId(initialGroupDescription, firstInstanceId).orElseThrow();

                firstStreams.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP)
                    .withTimeout(Duration.ofSeconds(30)));

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasStaticStreamsMemberWithMemberIdAndEpoch(
                            groupDescription,
                            firstInstanceId,
                            firstMemberId,
                            StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH
                        ) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Static streams member did not remain in group with static leave epoch");

                final int groupEpochAfterStaticLeave = describeStreamsGroup(applicationId).groupEpoch();

                TestUtils.waitForCondition(() -> {
                    mockTime.sleep(heartbeatIntervalMs);

                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 1 &&
                        groupDescription.groupEpoch() > groupEpochAfterStaticLeave &&
                        staticStreamsMemberId(groupDescription, firstInstanceId).isEmpty() &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Static streams member was not removed after session timeout");
            }
        } finally {
            Utils.delete(firstStateDir);
            Utils.delete(secondStateDir);
        }
    }

    @Test
    public void testDuplicateStaticInstanceIdDoesNotReplaceExistingStreamsMember() throws Exception {
        final File firstStateDir = TestUtils.tempDirectory();
        final File secondStateDir = TestUtils.tempDirectory();
        final File conflictStateDir = TestUtils.tempDirectory();
        final String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        final String firstInstanceId = "instance-1";
        final String secondInstanceId = "instance-2";
        final AtomicInteger firstStreamsRebalances = new AtomicInteger(0);
        final AtomicInteger secondStreamsRebalances = new AtomicInteger(0);

        try {
            try (final KafkaStreams firstStreams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), staticStreamsConfig(firstInstanceId, firstStateDir));
                 final KafkaStreams secondStreams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), staticStreamsConfig(secondInstanceId, secondStateDir));
                 final KafkaStreams conflictStreams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), staticStreamsConfig(firstInstanceId, conflictStateDir))) {
                firstStreams.setStateListener((newState, oldState) -> {
                    if (newState == KafkaStreams.State.REBALANCING) {
                        firstStreamsRebalances.incrementAndGet();
                    }
                });
                secondStreams.setStateListener((newState, oldState) -> {
                    if (newState == KafkaStreams.State.REBALANCING) {
                        secondStreamsRebalances.incrementAndGet();
                    }
                });

                IntegrationTestUtils.startApplicationAndWaitUntilRunning(Arrays.asList(firstStreams, secondStreams));
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                TestUtils.waitForCondition(() -> {
                    final StreamsGroupDescription groupDescription = describeStreamsGroup(applicationId);
                    return groupDescription.members().size() == 2 &&
                        hasActiveStaticStreamsMember(groupDescription, firstInstanceId) &&
                        hasActiveStaticStreamsMember(groupDescription, secondInstanceId);
                }, "Streams group did not reach 2 active static members");

                final StreamsGroupDescription initialGroupDescription = describeStreamsGroup(applicationId);
                final String firstMemberId = staticStreamsMemberId(initialGroupDescription, firstInstanceId).orElseThrow();
                final String secondMemberId = staticStreamsMemberId(initialGroupDescription, secondInstanceId).orElseThrow();
                final int initialGroupEpoch = initialGroupDescription.groupEpoch();
                final int firstRebalancesBeforeConflict = firstStreamsRebalances.get();
                final int secondRebalancesBeforeConflict = secondStreamsRebalances.get();

                conflictStreams.start();

                TestUtils.waitForCondition(
                    () -> conflictStreams.state() == KafkaStreams.State.ERROR,
                    "Conflicting static streams member did not transition to ERROR after duplicate static instance id"
                );

                final StreamsGroupDescription groupDescriptionAfterConflict = describeStreamsGroup(applicationId);

                assertEquals(2, groupDescriptionAfterConflict.members().size());
                assertEquals(initialGroupEpoch, groupDescriptionAfterConflict.groupEpoch());
                assertEquals(firstMemberId, staticStreamsMemberId(groupDescriptionAfterConflict, firstInstanceId).orElseThrow());
                assertEquals(secondMemberId, staticStreamsMemberId(groupDescriptionAfterConflict, secondInstanceId).orElseThrow());
                assertTrue(hasActiveStaticStreamsMember(groupDescriptionAfterConflict, firstInstanceId));
                assertTrue(hasActiveStaticStreamsMember(groupDescriptionAfterConflict, secondInstanceId));
                assertEquals(firstRebalancesBeforeConflict, firstStreamsRebalances.get());
                assertEquals(secondRebalancesBeforeConflict, secondStreamsRebalances.get());
            }
        } finally {
            Utils.delete(firstStateDir);
            Utils.delete(secondStateDir);
            Utils.delete(conflictStateDir);
        }
    }

    protected Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        input.to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));
        return builder.build();
    }

    private void add10InputElements() {
        final List<KeyValue<Long, String>> records = Arrays.asList(KeyValue.pair(0L, "aaa"),
            KeyValue.pair(1L, "bbb"),
            KeyValue.pair(0L, "ccc"),
            KeyValue.pair(1L, "ddd"),
            KeyValue.pair(0L, "eee"),
            KeyValue.pair(1L, "fff"),
            KeyValue.pair(0L, "ggg"),
            KeyValue.pair(1L, "hhh"),
            KeyValue.pair(0L, "iii"),
            KeyValue.pair(1L, "jjj"));

        for (final KeyValue<Long, String> record : records) {
            mockTime.sleep(10);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(record), producerConfig, mockTime.milliseconds());
        }
    }

    private Properties staticStreamsConfig(final String groupInstanceId, final File stateDir) {
        final Properties config = new Properties();
        config.putAll(streamsConfig);
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        return config;
    }

    private StreamsGroupDescription describeStreamsGroup(final String applicationId) throws Exception {
        return adminClient.describeStreamsGroups(Collections.singletonList(applicationId))
            .describedGroups()
            .get(applicationId)
            .get();
    }

    private boolean hasActiveStaticStreamsMember(
        final StreamsGroupDescription groupDescription,
        final String baseInstanceId
    ) {
        return groupDescription.members().stream().anyMatch(member ->
            member.instanceId()
                .filter(instanceId -> instanceId.startsWith(baseInstanceId + "-"))
                .isPresent() &&
                member.memberEpoch() >= 0
        );
    }

    private boolean hasStaticStreamsMemberWithMemberIdAndEpoch(
        final StreamsGroupDescription groupDescription,
        final String baseInstanceId,
        final String expectedMemberId,
        final int expectedEpoch
    ) {
        return groupDescription.members().stream().anyMatch(member ->
            member.instanceId()
                .filter(instanceId -> instanceId.startsWith(baseInstanceId + "-"))
                .isPresent() &&
                member.memberId().equals(expectedMemberId) &&
                member.memberEpoch() == expectedEpoch
        );
    }

    private Optional<String> staticStreamsMemberId(
        final StreamsGroupDescription groupDescription,
        final String baseInstanceId
    ) {
        return groupDescription.members().stream()
            .filter(member -> member.instanceId()
                .filter(instanceId -> instanceId.startsWith(baseInstanceId + "-"))
                .isPresent())
            .map(member -> member.memberId())
            .findFirst();
    }
    
}
