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

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.ShutdownableThread;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.common.test.TestUtils.SEEDED_RANDOM;
import static org.apache.kafka.common.test.TestUtils.randomSelect;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
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

    private List<Consumer<byte[], byte[]>> consumers;
    private List<ConsumerAssignmentPoller> consumerPollers;

    ConsumerBounceTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        consumerPollers = new ArrayList<>();
        consumers = new ArrayList<>();
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
        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {

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

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000"))) {
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
                        logger.info("Seeking to " + pos);
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

        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name));
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
        receiveExactRecords(poller, numRecords, 10000L);
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
        receiveExactRecords(poller2, numRecords, 10000L);
    }

    @ClusterTest
    public void testClose() throws Exception {
        int numRecords = 10;
        ClientsTestUtils.sendRecords(clusterInstance, topicPartition, numRecords);

        checkCloseGoodPath(numRecords, "group1");
        checkCloseWithCoordinatorFailure(numRecords, "group2", "group3");
    }

    private Consumer<byte[], byte[]> createConsumerAndReceive(String groupId, boolean manualAssign, int numRecords) throws InterruptedException {
        Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_ID_CONFIG, groupId));
        ConsumerAssignmentPoller poller;

        if (manualAssign) {
            consumer.assign(List.of(topicPartition));
            poller = new ConsumerAssignmentPoller(consumer, Set.of(topicPartition));
        } else {
            consumer.subscribe(List.of(topic));
            poller = new ConsumerAssignmentPoller(consumer, List.of(groupId));
        }

        poller.start();
        consumers.add(consumer);
        consumerPollers.add(poller);
        receiveExactRecords(poller, numRecords, 60000L);
        poller.shutdown();

        return consumer;
    }

    /**
     * Consumer is closed while cluster is healthy. Consumer should complete pending offset commits
     * and leave group. New consumer instance should be able to join group and start consuming from
     * last committed offset.
     */
    private void checkCloseGoodPath(int numRecords, String groupId) throws InterruptedException {
        Consumer<byte[], byte[]> consumer = createConsumerAndReceive(groupId, false, numRecords);
        assertDoesNotThrow(() -> submitCloseAndValidate(consumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get());
        checkClosedState(groupId, numRecords);
    }

    private void checkCloseWithCoordinatorFailure(int numRecords, String dynamicGroup, String manualGroup) throws Exception {
        Consumer<byte[], byte[]> dynamicConsumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, dynamicGroup));
        Consumer<byte[], byte[]> manualConsumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_ID_CONFIG, manualGroup));
        dynamicConsumer.subscribe(List.of(topic));
        manualConsumer.assign(List.of(topicPartition));
        ConsumerAssignmentPoller dynamicConsumerAssignmentPoller = new ConsumerAssignmentPoller(dynamicConsumer, List.of(topic));
        ConsumerAssignmentPoller manualConsumerAssignmentPoller = new ConsumerAssignmentPoller(manualConsumer, Set.of(topicPartition));
        dynamicConsumerAssignmentPoller.start();
        manualConsumerAssignmentPoller.start();
        consumerPollers.add(dynamicConsumerAssignmentPoller);
        consumerPollers.add(manualConsumerAssignmentPoller);

        clusterInstance.shutdownBroker(findCoordinator(dynamicGroup));
        clusterInstance.shutdownBroker(findCoordinator(manualGroup));

        submitCloseAndValidate(dynamicConsumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get();
        submitCloseAndValidate(manualConsumer, Long.MAX_VALUE, Optional.empty(), gracefulCloseTimeMs).get();

        restartDeadBrokers();
        checkClosedState(dynamicGroup, 0);
        checkClosedState(manualGroup, numRecords);
    }

    private int findCoordinator(String group) throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            TestUtils.waitForCondition(() -> {
                try {
                    DescribeConsumerGroupsResult result = admin.describeConsumerGroups(List.of(group));
                    return result.all().get().containsKey(group);
                } catch (Exception ignore) {
                    return false;
                }
            }, "Coordinator does not found.");
            return admin.describeConsumerGroups(List.of(group)).all().get().get(group).coordinator().id();
        }
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
            logger.info("Closing consumer with timeout " + closeTimeoutMs + " ms.");

            consumer.close(CloseOptions.timeout(Duration.ofMillis(closeTimeoutMs)));
            long timeTakenMs = System.currentTimeMillis() - startMs;

            maxCloseTimeMs.ifPresent(ms -> {
                assertTrue(timeTakenMs < ms + closeGraceTimeMs, "Close took too long " + timeTakenMs);
            });

            minCloseTimeMs.ifPresent(ms -> {
                assertTrue(timeTakenMs >= ms, "Close finished too quickly " + timeTakenMs);
            });

            logger.info("consumer.close() completed in {} ms.", timeTakenMs);
        }, 0);
    }

    private void receiveExactRecords(ConsumerAssignmentPoller consumer, int numRecords, long timeoutMs) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            System.err.println("ZZZ " + numRecords + " consumer.receivedMessages() " + consumer.receivedMessages());
            return consumer.receivedMessages() == numRecords;
        }, timeoutMs, String.format("Consumer did not receive expected %d. It received %d", numRecords, consumer.receivedMessages()));
//        TestUtils.waitForCondition(() -> consumer.receivedMessages() == numRecords, timeoutMs,
//             String.format("Consumer did not receive expected %d. It received %d", numRecords, consumer.receivedMessages()));
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
            this.clusterInstance.shutdownBroker(randomSelect(clusterInstance.brokerIds()));
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
