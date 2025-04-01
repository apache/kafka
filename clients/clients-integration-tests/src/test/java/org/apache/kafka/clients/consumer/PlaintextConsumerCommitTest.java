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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3
)
public class PlaintextConsumerCommitTest {

    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);

    public PlaintextConsumerCommitTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 2, (short) 3);
    }
    
    @ClusterTest
    public void testClassicConsumerAutoCommitOnClose() throws InterruptedException {
        testAutoCommitOnClose(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitOnClose() throws InterruptedException {
        testAutoCommitOnClose(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitOnClose(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            var numRecords = 10000;
            sendRecords(numRecords);

            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp, tp1));
            // should auto-commit sought positions before closing
            consumer.seek(tp, 300);
            consumer.seek(tp1, 500);
        }

        // now we should see the committed positions from another consumer
        try (var anotherConsumer = createConsumer(groupProtocol, true)) {
            assertEquals(300, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(500, anotherConsumer.committed(Set.of(tp1)).get(tp1).offset());
        }
    }
    
    @ClusterTest
    public void testClassicConsumerAutoCommitOnCloseAfterWakeup() throws InterruptedException {
        testAutoCommitOnCloseAfterWakeup(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitOnCloseAfterWakeup() throws InterruptedException {
        testAutoCommitOnCloseAfterWakeup(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitOnCloseAfterWakeup(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            var numRecords = 10000;
            sendRecords(numRecords);

            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp, tp1));

            // should auto-commit sought positions before closing
            consumer.seek(tp, 300);
            consumer.seek(tp1, 500);

            // wakeup the consumer before closing to simulate trying to break a poll
            // loop from another thread
            consumer.wakeup();
        }

        // now we should see the committed positions from another consumer
        try (var anotherConsumer = createConsumer(groupProtocol, true)) {
            assertEquals(300, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(500, anotherConsumer.committed(Set.of(tp1)).get(tp1).offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerCommitMetadata() throws InterruptedException {
        testCommitMetadata(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerCommitMetadata() throws InterruptedException {
        testCommitMetadata(GroupProtocol.CONSUMER);
    }

    private void testCommitMetadata(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            consumer.assign(List.of(tp));
            // sync commit
            var syncMetadata = new OffsetAndMetadata(5, Optional.of(15), "foo");
            consumer.commitSync(Map.of(tp, syncMetadata));
            assertEquals(syncMetadata, consumer.committed(Set.of(tp)).get(tp));

            // async commit
            var asyncMetadata = new OffsetAndMetadata(10, "bar");
            sendAndAwaitAsyncCommit(consumer, Optional.of(Map.of(tp, asyncMetadata)));
            assertEquals(asyncMetadata, consumer.committed(Set.of(tp)).get(tp));

            // handle null metadata
            var nullMetadata = new OffsetAndMetadata(5, null);
            consumer.commitSync(Map.of(tp, nullMetadata));
            assertEquals(nullMetadata, consumer.committed(Set.of(tp)).get(tp));
        }
    }

    @ClusterTest
    public void testClassicConsumerAsyncCommit() throws InterruptedException {
        testAsyncCommit(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAsyncCommit() throws InterruptedException {
        testAsyncCommit(GroupProtocol.CONSUMER);
    }

    private void testAsyncCommit(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, false)) {
            consumer.assign(List.of(tp));

            var callback = new CountConsumerCommitCallback();
            var count = 5;
            for (var i = 1; i <= count; i++) 
                consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(i)), callback);

            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100));
                return callback.successCount >= count || callback.lastError.isPresent();
            }, "Failed to observe commit callback before timeout");

            assertEquals(Optional.empty(), callback.lastError);
            assertEquals(count, callback.successCount);
            assertEquals(new OffsetAndMetadata(count), consumer.committed(Set.of(tp)).get(tp));
        }
    }

    @ClusterTest
    public void testClassicConsumerAutoCommitIntercept() throws InterruptedException {
        testAutoCommitIntercept(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitIntercept() throws InterruptedException {
        testAutoCommitIntercept(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitIntercept(GroupProtocol groupProtocol) throws InterruptedException {
        String topic2 = "topic2";
        cluster.createTopic(topic2, 2, (short) 3);
        int numRecords = 100;
        try (var producer = cluster.producer();
             // create consumer with interceptor
             Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
                 GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
                 ENABLE_AUTO_COMMIT_CONFIG, "true",
                 INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor"
             ))
        ) {
            // produce records
            for (var i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), ("key " + i).getBytes(), ("value " + i).getBytes()));
            }

            var rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // keep partitions paused in this test so that we can verify the commits based on specific seeks
                    consumer.pause(partitions);
                }
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // No-op
                }
            };

            changeConsumerSubscriptionAndValidateAssignment(
                consumer, 
                List.of(topic), 
                Set.of(tp, tp1),
                rebalanceListener
            );
            consumer.seek(tp, 10);
            consumer.seek(tp1, 20);

            // change subscription to trigger rebalance
            var commitCountBeforeRebalance = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue();
            var expectedAssignment = Set.of(tp, tp1, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1)); 
            changeConsumerSubscriptionAndValidateAssignment(
                consumer, 
                List.of(topic, topic2), 
                expectedAssignment, 
                rebalanceListener
            );

            // after rebalancing, we should have reset to the committed positions
            var committed1 = consumer.committed(Set.of(tp));
            assertEquals(10, committed1.get(tp).offset());
            var committed2 = consumer.committed(Set.of(tp1));
            assertEquals(20, committed2.get(tp1).offset());
            assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeRebalance);

            // In both CLASSIC and CONSUMER protocols, interceptors are executed in poll and close.
            // However, in the CONSUMER protocol, the assignment may be changed outside of a poll, so
            // we need to poll once to ensure the interceptor is called.
            if (groupProtocol == GroupProtocol.CONSUMER) {
                consumer.poll(Duration.ZERO);
            }

            // verify commits are intercepted on close
            var commitCountBeforeClose = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue();
            consumer.close();
            assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeClose);
            producer.close();
            // cleanup
            MockConsumerInterceptor.resetCounters();
        }
    }

    private Consumer<byte[], byte[]> createConsumer(GroupProtocol protocol, boolean enableAutoCommit) {
        return cluster.consumer(Map.of(
            GROUP_ID_CONFIG, "test-group",
            MAX_POLL_INTERVAL_MS_CONFIG, 600,
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit
        ));
    }

    private void sendRecords(int numRecords) {
        long startingTimestamp = System.currentTimeMillis();
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int i = 0; i < numRecords; i++) {
                long timestamp = startingTimestamp + i;
                var record = new ProducerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    timestamp,
                    ("key " + i).getBytes(),
                    ("value " + i).getBytes()
                );
                producer.send(record);
            }
            producer.flush();
        }
    }

    private void awaitAssignment(
        Consumer<byte[], byte[]> consumer,
        Set<TopicPartition> expectedAssignment
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return consumer.assignment().equals(expectedAssignment);
        }, "Timed out while awaiting expected assignment " + expectedAssignment + ". " +
            "The current assignment is " + consumer.assignment()
        );
    }

    private void sendAndAwaitAsyncCommit(
        Consumer<byte[], byte[]> consumer,
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
    ) throws InterruptedException {
        RetryCommitCallback commitCallback = new RetryCommitCallback(consumer, offsetsOpt);

        commitCallback.sendAsyncCommit();
        TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100));
                return commitCallback.isComplete;
            }, "Failed to observe commit callback before timeout"
        );

        assertEquals(Optional.empty(), commitCallback.error);
    }

    private class RetryCommitCallback implements OffsetCommitCallback {
        private boolean isComplete = false;
        private Optional<Exception> error = Optional.empty();
        
        private final Consumer<byte[], byte[]> consumer;
        private final Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt;
        
        public RetryCommitCallback(
            Consumer<byte[], byte[]> consumer,
            Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
        ) {
            this.consumer = consumer;
            this.offsetsOpt = offsetsOpt;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception instanceof RetriableCommitFailedException) {
                sendAsyncCommit();
            } else {
                isComplete = true;
                error = Optional.ofNullable(exception);
            }
        }

        void sendAsyncCommit() {
            if (offsetsOpt.isPresent()) {
                consumer.commitAsync(offsetsOpt.get(), this);
            } else {
                consumer.commitAsync(this);
            }
        }
    }

    private class CountConsumerCommitCallback implements OffsetCommitCallback {
        private int successCount = 0;
        private int failCount = 0;
        private Optional<Exception> lastError = Optional.empty();

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception == null) {
                successCount += 1;
            } else {
                failCount += 1;
                lastError = Optional.of(exception);
            }
        }
    }

    private void changeConsumerSubscriptionAndValidateAssignment(
        Consumer<byte[], byte[]> consumer, 
        List<String> topicsToSubscribe, 
        Set<TopicPartition> expectedAssignment, 
        ConsumerRebalanceListener rebalanceListener
    ) throws InterruptedException {
        consumer.subscribe(topicsToSubscribe, rebalanceListener);
        awaitAssignment(consumer, expectedAssignment);
    }
}
