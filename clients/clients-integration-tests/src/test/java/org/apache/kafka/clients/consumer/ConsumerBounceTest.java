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
package org.apache.kafka.clients.consumer;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static  org.apache.kafka.test.TestUtils.SEEDED_RANDOM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the consumer that cover basic usage as well as server failures
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = ConsumerBounceTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"), // don't want to lose offset
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "10"), // set small enough session timeout
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),

        // Tests will run for CONSUMER and CLASSIC group protocol, so set the group max size property
        // required for each.
        @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, value = ConsumerBounceTest.MAX_GROUP_SIZE),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, value = ConsumerBounceTest.MAX_GROUP_SIZE),

        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, value = "1000"),
        @ClusterConfigProperty(key = ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG, value = "50"),

        @ClusterConfigProperty(key = KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, value = "50"),
        @ClusterConfigProperty(key = KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, value = "300")
    }
)
public class ConsumerBounceTest {

    private final Logger logger = new LogContext("ConsumerBounceTest").logger(this.getClass());

    public static final int BROKER_COUNT = 3;
    public static final String MAX_GROUP_SIZE = "5";

    private final Optional<Long> gracefulCloseTimeMs = Optional.of(1000L);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private final String topic = "topic";
    private final int partition = 0;
    private final int numPartitions = 3;
    private final short numReplica = 3;
    private final TopicPartition topicPartition = new TopicPartition(topic, partition);

    private final ClusterInstance clusterInstance;

    private final List<Consumer<byte[], byte[]>> consumers = new ArrayList<>();
    private final List<ConsumerAssignmentPoller> consumerPollers = new ArrayList<>();

    ConsumerBounceTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        clusterInstance.createTopic(topic, numPartitions, numReplica);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        consumerPollers.forEach(poller -> {
            try {
                poller.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        executor.shutdownNow();
        // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
        assertTrue(executor.awaitTermination(5000, TimeUnit.MILLISECONDS), "Executor did not terminate");
        consumers.forEach(Consumer::close);
    }

    @ClusterTest
    public void testClassicConsumerConsumptionWithBrokerFailures() throws Exception {
        consumeWithBrokerFailures(10, GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerConsumptionWithBrokerFailures() throws Exception {
        consumeWithBrokerFailures(10, GroupProtocol.CONSUMER);
    }

    /**
     * 1. Produce a bunch of messages
     * 2. Then consume the messages while killing and restarting brokers at random
     */
    private void consumeWithBrokerFailures(int numIters, GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 1000;
        ClientsTestUtils.sendRecords(clusterInstance, topicPartition, numRecords);

        AtomicInteger consumed = new AtomicInteger(0);
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {

            consumer.subscribe(List.of(topic));

            BounceBrokerScheduler scheduler = new BounceBrokerScheduler(numIters, clusterInstance);
            try {
                scheduler.start();

                while (scheduler.isRunning()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        assertEquals(consumed.get(), record.offset());
                        consumed.incrementAndGet();
                    });

                    if (!records.isEmpty()) {
                        consumer.commitSync();

                        long currentPosition = consumer.position(topicPartition);
                        long committedOffset = consumer.committed(Set.of(topicPartition)).get(topicPartition).offset();
                        assertEquals(currentPosition, committedOffset);

                        if (currentPosition == numRecords) {
                            consumer.seekToBeginning(List.of());
                            consumed.set(0);
                        }
                    }
                }
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @ClusterTest
    public void testClassicConsumerSeekAndCommitWithBrokerFailures() throws InterruptedException {
        seekAndCommitWithBrokerFailures(5, GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSeekAndCommitWithBrokerFailures() throws InterruptedException {
        seekAndCommitWithBrokerFailures(5, GroupProtocol.CONSUMER);
    }

    private void seekAndCommitWithBrokerFailures(int numIters, GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 1000;
        ClientsTestUtils.sendRecords(clusterInstance, topicPartition, numRecords);

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, 0);

            TestUtils.waitForCondition(() -> clusterInstance.brokers().values().stream().allMatch(broker ->
                    broker.replicaManager().localLog(topicPartition).get().highWatermark() == numRecords
            ), 30000, "Failed to update high watermark for followers after timeout.");

            BounceBrokerScheduler scheduler = new BounceBrokerScheduler(numIters, clusterInstance);
            try {
                scheduler.start();

                while (scheduler.isRunning()) {
                    int coin = SEEDED_RANDOM.nextInt(0, 3);

                    if (coin == 0) {
                        logger.info("Seeking to end of log.");
                        consumer.seekToEnd(List.of());
                        assertEquals(numRecords, consumer.position(topicPartition));
                    } else if (coin == 1) {
                        int pos = SEEDED_RANDOM.nextInt(numRecords);
                        logger.info("Seeking to {}", pos);
                        consumer.seek(topicPartition, pos);
                        assertEquals(pos, consumer.position(topicPartition));
                    } else {
                        logger.info("Committing offset.");
                        consumer.commitSync();
                        assertEquals(consumer.position(topicPartition), consumer.committed(Set.of(topicPartition)).get(topicPartition).offset());
                    }
                }
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @ClusterTest
    public void testClassicSubscribeWhenTopicUnavailable() throws InterruptedException {
        testSubscribeWhenTopicUnavailable(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncSubscribeWhenTopicUnavailable() throws InterruptedException {
        testSubscribeWhenTopicUnavailable(GroupProtocol.CONSUMER);
    }

    private void testSubscribeWhenTopicUnavailable(GroupProtocol groupProtocol) throws InterruptedException {
        String newTopic = "new-topic";
        TopicPartition newTopicPartition = new TopicPartition(newTopic, 0);
        int numRecords = 1000;

        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(
                Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 6000,
                       ConsumerConfig.METADATA_MAX_AGE_CONFIG, 100));
        consumers.add(consumer);
        consumer.subscribe(List.of(newTopic));
        consumer.poll(Duration.ZERO);
        // Schedule topic creation after 2 seconds
        executor.schedule(() -> assertDoesNotThrow(() -> clusterInstance.createTopic(newTopic, numPartitions, numReplica)),
                2, TimeUnit.SECONDS);

        // Start first poller
        ConsumerAssignmentPoller poller = new ConsumerAssignmentPoller(consumer, List.of(newTopic));
        consumerPollers.add(poller);
        poller.start();
        ClientsTestUtils.sendRecords(clusterInstance, newTopicPartition, numRecords);
        receiveExactRecords(poller, numRecords, 60000L);
        poller.shutdown();

        // Simulate broker failure and recovery
        clusterInstance.brokers().keySet().forEach(clusterInstance::shutdownBroker);
        Thread.sleep(500);
        clusterInstance.brokers().keySet().forEach(clusterInstance::startBroker);

        // Start second poller after recovery
        ConsumerAssignmentPoller poller2 = new ConsumerAssignmentPoller(consumer, List.of(newTopic));
        consumerPollers.add(poller2);
        poller2.start();

        ClientsTestUtils.sendRecords(clusterInstance, newTopicPartition, numRecords);
        receiveExactRecords(poller2, numRecords, 60000L);
    }


    @ClusterTest
    public void testClassicClose() throws Exception {
        testClose(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncClose() throws Exception {
        testClose(GroupProtocol.CONSUMER);
    }

    private void testClose(GroupProtocol groupProtocol) throws Exception {
        int numRecords = 10;
        ClientsTestUtils.sendRecords(clusterInstance, topicPartition, numRecords);

        checkCloseGoodPath(groupProtocol, numRecords, "group1");
        checkCloseWithCoordinatorFailure(groupProtocol, numRecords, "group2", "group3");
        checkCloseWithClusterFailure(groupProtocol, numRecords, "group4", "group5");
    }

    /**
     * Consumer is closed while cluster is healthy. Consumer should complete pending offset commits
     * and leave group. New consumer instance should be able to join group and start consuming from
     * last committed offset.
     */
    private void checkCloseGoodPath(GroupProtocol groupProtocol, int numRecords, String groupId) throws InterruptedException {
        Consumer<byte[], byte[]> consumer = createConsumerAndReceive(groupId, false, numRecords, Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name));
        assertDoesNotThrow(() -> submitCloseAndValidate(consumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get());
        checkClosedState(groupId, numRecords);
    }

    /**
     * Consumer closed while coordinator is unavailable. Close of consumers using group
     * management should complete after commit attempt even though commits fail due to rebalance.
     * Close of consumers using manual assignment should complete with successful commits since a
     * broker is available.
     */
    private void checkCloseWithCoordinatorFailure(GroupProtocol groupProtocol, int numRecords, String dynamicGroup, String manualGroup) throws Exception {
        Consumer<byte[], byte[]> dynamicConsumer = createConsumerAndReceive(dynamicGroup, false, numRecords, Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name));
        Consumer<byte[], byte[]> manualConsumer = createConsumerAndReceive(manualGroup, true, numRecords, Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name));

        findCoordinators(List.of(dynamicGroup, manualGroup)).forEach(clusterInstance::shutdownBroker);

        submitCloseAndValidate(dynamicConsumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get();
        submitCloseAndValidate(manualConsumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get();

        restartDeadBrokers();
        checkClosedState(dynamicGroup, 0);
        checkClosedState(manualGroup, numRecords);
    }

    /**
     * Consumer is closed while all brokers are unavailable. Cannot rebalance or commit offsets since
     * there is no coordinator, but close should timeout and return. If close is invoked with a very
     * large timeout, close should timeout after request timeout.
     */
    private void checkCloseWithClusterFailure(GroupProtocol groupProtocol, int numRecords, String group1, String group2) throws Exception {
        Consumer<byte[], byte[]> consumer1 = createConsumerAndReceive(group1, false, numRecords, Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name));
        Map<String, String> consumerConfig = new HashMap<>();

        long requestTimeout = 6000;
        if (groupProtocol.equals(GroupProtocol.CLASSIC)) {
            consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5000");
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        }
        consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(requestTimeout));
        consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name);

        Consumer<byte[], byte[]> consumer2 = createConsumerAndReceive(group2, true, numRecords, consumerConfig);

        clusterInstance.brokers().keySet().forEach(clusterInstance::shutdownBroker);

        long closeTimeout = 2000;
        submitCloseAndValidate(consumer1, closeTimeout, Optional.empty(), Optional.of(closeTimeout)).get();
        submitCloseAndValidate(consumer2, Long.MAX_VALUE, Optional.empty(), Optional.of(requestTimeout)).get();
    }

    private Set<Integer> findCoordinators(List<String> groups) throws Exception {
        FindCoordinatorRequest request = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                .setCoordinatorKeys(groups)).build();
        Set<Integer> nodes = new HashSet<>();
        TestUtils.waitForCondition(() -> {
            FindCoordinatorResponse response = null;
            try {
                response = IntegrationTestUtils.connectAndReceive(request, clusterInstance.brokerBoundPorts().get(0));
            } catch (IOException e) {
                return false;
            }

            if (response.hasError())
                return false;
            for (String group : groups)
                if (response.coordinatorByKey(group).isEmpty())
                    return false;
                else
                    nodes.add(response.coordinatorByKey(group).get().nodeId());
            return true;
        }, "Failed to find coordinator for group " + groups);
        return nodes;
    }

    @ClusterTest
    public void testClassicConsumerReceivesFatalExceptionWhenGroupPassesMaxSize() throws Exception {
        testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize() throws Exception {
        testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(GroupProtocol.CONSUMER);
    }

    private void testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(GroupProtocol groupProtocol) throws Exception {
        String group = "fatal-exception-test";
        String topic = "fatal-exception-test";

        Map<String, String> consumerConfig = new HashMap<>();
        int numPartition = Integer.parseInt(MAX_GROUP_SIZE);

        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        if (groupProtocol.equals(GroupProtocol.CLASSIC)) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        }
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        clusterInstance.createTopic(topic, Integer.parseInt(MAX_GROUP_SIZE), (short) BROKER_COUNT);
        Set<TopicPartition> partitions = new HashSet<>();
        for (int i = 0; i < Integer.parseInt(MAX_GROUP_SIZE); ++i)
            partitions.add(new TopicPartition(topic, i));

        addConsumersToGroupAndWaitForGroupAssignment(
                Integer.parseInt(MAX_GROUP_SIZE),
                List.of(topic),
                partitions,
                group,
                consumerConfig
        );

        addConsumersToGroup(
                1,
                List.of(topic),
                group,
                consumerConfig
        );

        ConsumerAssignmentPoller rejectedConsumer = consumerPollers.get(consumerPollers.size() - 1);
        consumerPollers.remove(consumerPollers.size() - 1);

        TestUtils.waitForCondition(
                () -> rejectedConsumer.getThrownException().isPresent(),
                "Extra consumer did not throw an exception"
        );

        assertInstanceOf(GroupMaxSizeReachedException.class, rejectedConsumer.getThrownException().get());

        // assert group continues to live and the records to be distributed across all partitions.
        var data = "data".getBytes(StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            IntStream.range(0, numPartition * 100).forEach(index ->
                    producer.send(new ProducerRecord<>(topic, index % numPartition, data, data)));
        }

        TestUtils.waitForCondition(
                () -> consumerPollers.stream().allMatch(p -> p.receivedMessages() >= 100),
                10000L, "The consumers in the group could not fetch the expected records"
        );
    }

    /**
     * Create 'numOfConsumersToAdd' consumers, add them to the consumer group, and create corresponding
     * pollers. Wait for partition re-assignment and validate.
     *
     * Assignment validation requires that total number of partitions is greater than or equal to
     * the resulting number of consumers in the group.
     *
     * @param numOfConsumersToAdd number of consumers to create and add to the consumer group
     * @param topicsToSubscribe topics to subscribe
     * @param subscriptions set of all topic partitions
     * @param group consumer group ID
     */
    private void addConsumersToGroupAndWaitForGroupAssignment(
            int numOfConsumersToAdd,
            List<String> topicsToSubscribe,
            Set<TopicPartition> subscriptions,
            String group,
            Map<String, String> consumerConfig
    ) throws InterruptedException {
        // Validation: number of consumers should not exceed number of partitions
        assertTrue(consumers.size() + numOfConsumersToAdd <= subscriptions.size(),
                "Total consumers exceed number of partitions");

        // Add consumers and pollers
        addConsumersToGroup(numOfConsumersToAdd, topicsToSubscribe, group, consumerConfig);

        // Validate that all pollers have assigned partitions
        validateGroupAssignment(consumerPollers, subscriptions);
    }

    /**
     * Check whether partition assignment is valid.
     * Assumes partition assignment is valid iff:
     * 1. Every consumer got assigned at least one partition
     * 2. Each partition is assigned to only one consumer
     * 3. Every partition is assigned to one of the consumers
     * 4. The assignment is the same as expected assignment (if provided)
     *
     * @param assignments        List of assignments, one set per consumer
     * @param partitions         All expected partitions
     * @param expectedAssignment Optional expected assignment
     * @return true if assignment is valid
     */
    private boolean isPartitionAssignmentValid(
            List<Set<TopicPartition>> assignments,
            Set<TopicPartition> partitions,
            List<Set<TopicPartition>> expectedAssignment
    ) {
        // 1. Check that every consumer has non-empty assignment
        boolean allNonEmpty = assignments.stream().noneMatch(Set::isEmpty);
        if (!allNonEmpty) return false;

        // 2. Check that total assigned partitions equals number of unique partitions
        Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        for (Set<TopicPartition> assignment : assignments) {
            allAssignedPartitions.addAll(assignment);
        }

        if (allAssignedPartitions.size() != partitions.size()) {
            // Either some partitions were assigned multiple times or some were not assigned
            return false;
        }

        // 3. Check that assigned partitions exactly match the expected set
        if (!allAssignedPartitions.equals(partitions)) {
            return false;
        }

        // 4. If expected assignment is given, check for exact match
        if (expectedAssignment != null && !expectedAssignment.isEmpty()) {
            if (assignments.size() != expectedAssignment.size()) return false;
            for (int i = 0; i < assignments.size(); i++) {
                if (!assignments.get(i).equals(expectedAssignment.get(i))) return false;
            }
        }

        return true;
    }

    /**
     * Wait for consumers to get partition assignment and validate it.
     *
     * @param consumerPollers       Consumer pollers corresponding to the consumer group being tested
     * @param subscriptions         Set of all topic partitions
     * @param msg                   Optional message to print if validation fails
     * @param waitTimeMs            Wait timeout in milliseconds
     * @param expectedAssignments   Expected assignments (optional)
     */
    private void validateGroupAssignment(
            List<ConsumerAssignmentPoller> consumerPollers,
            Set<TopicPartition> subscriptions,
            Optional<String> msg,
            long waitTimeMs,
            List<Set<TopicPartition>> expectedAssignments
    ) throws InterruptedException {
        List<Set<TopicPartition>> assignments = new ArrayList<>();

        TestUtils.waitForCondition(() -> {
            assignments.clear();
            consumerPollers.forEach(poller -> assignments.add(poller.consumerAssignment()));
            return isPartitionAssignmentValid(assignments, subscriptions, expectedAssignments);
        }, waitTimeMs, () -> msg.orElse("Did not get valid assignment for partitions " + subscriptions + ". Instead got: " + assignments));
    }

    // Overload for convenience (optional msg and expectedAssignments)
    private void validateGroupAssignment(
            List<ConsumerAssignmentPoller> consumerPollers,
            Set<TopicPartition> subscriptions
    ) throws InterruptedException {
        validateGroupAssignment(consumerPollers, subscriptions, Optional.empty(), 10000L, new ArrayList<>());
    }

    /**
     * Create 'numOfConsumersToAdd' consumers, add them to the consumer group, and create corresponding pollers.
     *
     * @param numOfConsumersToAdd number of consumers to create and add to the consumer group
     * @param topicsToSubscribe topics to which new consumers will subscribe
     * @param group consumer group ID
     */
    private void addConsumersToGroup(
            int numOfConsumersToAdd,
            List<String> topicsToSubscribe,
            String group,
            Map<String, String> consumerConfigs) {

        Map<String, Object> configs = new HashMap<>(consumerConfigs);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        for (int i = 0; i < numOfConsumersToAdd; i++) {
            Consumer<byte[], byte[]> consumer = clusterInstance.consumer(configs);
            consumers.add(consumer);

            ConsumerAssignmentPoller poller = new ConsumerAssignmentPoller(consumer, topicsToSubscribe);
            poller.start();
            consumerPollers.add(poller);
        }
    }

    @ClusterTest
    public void testClassicCloseDuringRebalance() throws Exception {
        testCloseDuringRebalance(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncCloseDuringRebalance() throws Exception {
        testCloseDuringRebalance(GroupProtocol.CONSUMER);
    }

    public void testCloseDuringRebalance(GroupProtocol groupProtocol) throws Exception {
        Map<String, String> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        if (groupProtocol.equals(GroupProtocol.CLASSIC)) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        }
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        checkCloseDuringRebalance(consumerConfig);
    }

    private void checkCloseDuringRebalance(Map<String, String> consumerConfig) throws Exception {
        Map<String, Object> configs = new HashMap<>(consumerConfig);
        String groupId = "group";
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        Consumer<byte[], byte[]> consumer1 = clusterInstance.consumer(configs);
        Future<?> f1 = subscribeAndPoll(consumer1, Optional.empty());
        waitForRebalance(2000, f1, null);

        Consumer<byte[], byte[]> consumer2 = clusterInstance.consumer(configs);
        Future<?> f2 = subscribeAndPoll(consumer2, Optional.empty());
        waitForRebalance(2000, f2, consumer1);

        Future<?> rebalanceFuture = createConsumerToRebalance(groupId);

        Future<?> closeFuture1 = submitCloseAndValidate(consumer1, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs);

        waitForRebalance(2000, rebalanceFuture, consumer2);

        createConsumerToRebalance(groupId); // one more time
        clusterInstance.brokers().values().forEach(KafkaBroker::shutdown);

        Future<?> closeFuture2 = submitCloseAndValidate(consumer2, Long.MAX_VALUE, Optional.empty(), Optional.of(0L));

        closeFuture1.get(2000, TimeUnit.MILLISECONDS);
        closeFuture2.get(2000, TimeUnit.MILLISECONDS);
    }

    Future<?> subscribeAndPoll(Consumer<byte[], byte[]> consumer, Optional<Semaphore> revokeSemaphore) {
        return executor.submit(() -> {
            consumer.subscribe(List.of(topic));
            revokeSemaphore.ifPresent(Semaphore::release);
            consumer.poll(Duration.ofMillis(500));
            return null;
        });
    }

    void waitForRebalance(long timeoutMs, Future<?> future, Consumer<byte[], byte[]> otherConsumers) {
        long startMs = System.currentTimeMillis();
        while (System.currentTimeMillis() < startMs + timeoutMs && !future.isDone()) {
            if (otherConsumers != null) {
                otherConsumers.poll(Duration.ofMillis(100));
            }
        }
        assertTrue(future.isDone(), "Rebalance did not complete in time");
    }

    Future<?> createConsumerToRebalance(String groupId) throws Exception {
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId));
        consumers.add(consumer);
        Semaphore rebalanceSemaphore = new Semaphore(0);
        Future<?> future = subscribeAndPoll(consumer, Optional.of(rebalanceSemaphore));
        assertTrue(rebalanceSemaphore.tryAcquire(2000, TimeUnit.MILLISECONDS), "Rebalance not triggered");
        assertFalse(future.isDone(), "Rebalance completed too early");
        return future;
    }



    private Consumer<byte[], byte[]> createConsumerAndReceive(String groupId, boolean manualAssign, int numRecords,
                                                              Map<String, String> consumerConfig) throws InterruptedException {
        Map<String, Object> configs = new HashMap<>(consumerConfig);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(configs);
        ConsumerAssignmentPoller poller;

        if (manualAssign) {
            poller = new ConsumerAssignmentPoller(consumer, Set.of(topicPartition));
        } else {
            poller = new ConsumerAssignmentPoller(consumer, List.of(topic));
        }
        poller.start();
        consumers.add(consumer);
        consumerPollers.add(poller);
        receiveExactRecords(poller, numRecords, 60000L);
        poller.shutdown();

        return consumer;
    }

    private void restartDeadBrokers() {
        clusterInstance.brokers().forEach((id, broker) -> {
            if (broker.isShutdown()) {
                broker.startup();
            }
        });
    }

    private void checkClosedState(String groupId, int committedRecords) throws InterruptedException {
        // Check that close was graceful with offsets committed and leave group sent.
        // New instance of consumer should be assigned partitions immediately and should see committed offsets.        Semaphore assignSemaphore = new Semaphore(0);

        Semaphore assignSemaphore = new Semaphore(0);
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId))) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assignSemaphore.release();
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Do nothing
                }
            });

            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100));
                return assignSemaphore.tryAcquire();
            }, "Assignment did not complete on time");

            if (committedRecords > 0) {
                Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.of(topicPartition));
                long offset = committed.get(topicPartition).offset();
                assertEquals(committedRecords, offset, "Committed offset does not match expected value.");
            }
        }
    }

    private Future<?> submitCloseAndValidate(
            Consumer<byte[], byte[]> consumer,
            long closeTimeoutMs,
            Optional<Long> minCloseTimeMs,
            Optional<Long> maxCloseTimeMs) {

        return executor.submit(() -> {
            final long closeGraceTimeMs = 2000;
            long startMs = System.currentTimeMillis();
            logger.info("Closing consumer with timeout {} ms.", closeTimeoutMs);

            consumer.close(CloseOptions.timeout(Duration.ofMillis(closeTimeoutMs)));
            long timeTakenMs = System.currentTimeMillis() - startMs;

            maxCloseTimeMs.ifPresent(ms ->
                assertTrue(timeTakenMs < ms + closeGraceTimeMs, "Close took too long " + timeTakenMs)
            );

            minCloseTimeMs.ifPresent(ms ->
                assertTrue(timeTakenMs >= ms, "Close finished too quickly " + timeTakenMs)
            );

            logger.info("consumer.close() completed in {} ms.", timeTakenMs);
        }, 0);
    }

    private void receiveExactRecords(ConsumerAssignmentPoller consumer, int numRecords, long timeoutMs) throws InterruptedException {
        TestUtils.waitForCondition(() -> consumer.receivedMessages() == numRecords, timeoutMs,
                () -> String.format("Consumer did not receive expected %d. It received %d", numRecords, consumer.receivedMessages()));
    }

    // A mock class to represent broker bouncing (simulate broker restart behavior)
    private static class BounceBrokerScheduler extends ShutdownableThread {
        private final int numIters;
        private int iter = 0;

        final ClusterInstance clusterInstance;

        public BounceBrokerScheduler(int numIters, ClusterInstance clusterInstance) {
            super("daemon-bounce-broker", false);
            this.numIters = numIters;
            this.clusterInstance = clusterInstance;
        }

        private void killRandomBroker() {
            this.clusterInstance.shutdownBroker(TestUtils.randomSelect(clusterInstance.brokerIds()));
        }

        private void restartDeadBrokers() {
            clusterInstance.brokers().forEach((id, broker) -> {
                if (broker.isShutdown()) {
                    broker.startup();
                }
            });
        }

        @Override
        public void doWork() {
            killRandomBroker();
            assertDoesNotThrow(() -> Thread.sleep(500));
            restartDeadBrokers();

            iter++;
            if (iter == numIters) {
                initiateShutdown();
            } else {
                assertDoesNotThrow(() -> Thread.sleep(500));
            }
        }
    }
}
