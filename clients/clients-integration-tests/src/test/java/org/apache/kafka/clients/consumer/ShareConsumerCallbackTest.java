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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(1200)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1")
    }
)
public class ShareConsumerCallbackTest extends ShareConsumerTestBase {

    public ShareConsumerCallbackTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testAcknowledgementSentOnSubscriptionChange() throws ExecutionException, InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp2.topic(), tp2.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record2).get();
            producer.flush();
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));

            shareConsumer.subscribe(Set.of(tp.topic()));
            assertEquals(Optional.empty(), shareConsumer.acquisitionLockTimeoutMs());

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());
            shareConsumer.subscribe(Set.of(tp2.topic()));
            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());

            // Waiting for heartbeat to propagate the subscription change.
            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(500));
                return partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.containsKey(tp2);
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records from the updated subscription");

            // Verifying if the callback was invoked without exceptions for the partitions for both topics.
            assertFalse(partitionExceptionMap.containsKey(tp));
            assertFalse(partitionExceptionMap.containsKey(tp2));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackSuccessfulAcknowledgement() throws Exception {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Set.of(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            // The callback should be called before the return of the poll, even when there are no more records.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
            assertEquals(0, records.count());
            assertTrue(partitionOffsetsMap.containsKey(tp));

            // We expect no exception as the acknowledgement error code is null.
            assertFalse(partitionExceptionMap.containsKey(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackSuccessfulAcknowledgementOnCommitSync() throws Exception {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Set.of(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            // The acknowledgement commit callback should be called before the commitSync returns
            // once the records have been confirmed to have been acknowledged.
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            assertTrue(partitionOffsetsMap.containsKey(tp));

            // We expect no exception as the acknowledgement error code is null.
            assertFalse(partitionExceptionMap.containsKey(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackOnClose() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());

            // Now in the second poll, we implicitly acknowledge the record received in the first poll.
            // We get back the acknowledgement error code asynchronously after the second poll.
            // The acknowledgement commit callback is invoked in close.
            shareConsumer.poll(Duration.ofMillis(1000));
            shareConsumer.close();

            // We expect no exception as the acknowledgement error code is null.
            assertFalse(partitionExceptionMap.containsKey(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackInvalidRecordStateException() throws Exception {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());

            // Waiting until the acquisition lock expires.
            Thread.sleep(20000);

            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(500));
                return partitionExceptionMap.containsKey(tp) && partitionExceptionMap.get(tp) instanceof InvalidRecordStateException;
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to be notified by InvalidRecordStateException");
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback cannot invoke methods of ShareConsumer.
     * The exception thrown is verified in {@link TestableAcknowledgementCommitCallbackWithShareConsumer}
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackCallsShareConsumerDisallowed() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackWithShareConsumer<>(shareConsumer));
            shareConsumer.subscribe(Set.of(tp.topic()));

            // The acknowledgement commit callback will try to call a method of ShareConsumer
            shareConsumer.poll(Duration.ofMillis(5000));
            // The second poll sends the acknowledgements implicitly.
            // The acknowledgement commit callback will be called and the exception is thrown.
            // This is verified inside the onComplete() method implementation.
            shareConsumer.poll(Duration.ofMillis(500));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    private class TestableAcknowledgementCommitCallbackWithShareConsumer<K, V> implements AcknowledgementCommitCallback {
        private final ShareConsumer<K, V> shareConsumer;

        TestableAcknowledgementCommitCallbackWithShareConsumer(ShareConsumer<K, V> shareConsumer) {
            this.shareConsumer = shareConsumer;
        }

        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            // Accessing methods of ShareConsumer should throw an exception.
            assertThrows(IllegalStateException.class, shareConsumer::close);
            assertThrows(IllegalStateException.class, () -> shareConsumer.subscribe(Set.of(tp.topic())));
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(5000)));
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback can invoke ShareConsumer.wakeup() and it
     * wakes up the enclosing poll.
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackCallsShareConsumerWakeup() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            // The acknowledgement commit callback will try to call a method of ShareConsumer
            shareConsumer.setAcknowledgementCommitCallback((__, ___) -> shareConsumer.wakeup());
            shareConsumer.subscribe(Set.of(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            // The second poll sends the acknowledgements implicitly.
            shareConsumer.poll(Duration.ofMillis(2000));

            // Till now acknowledgement commit callback has not been called, so no exception thrown yet.
            // On 3rd poll, the acknowledgement commit callback will be called and the exception is thrown.
            AtomicBoolean exceptionThrown = new AtomicBoolean(false);
            TestUtils.waitForCondition(() -> {
                try {
                    shareConsumer.poll(Duration.ofMillis(500));
                } catch (WakeupException e) {
                    exceptionThrown.set(true);
                }
                return exceptionThrown.get();
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to receive expected exception");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback can throw an exception, and it is not propagated
     * to the caller of poll().
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackThrowsException() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            AtomicBoolean callbackCalled = new AtomicBoolean(false);
            shareConsumer.setAcknowledgementCommitCallback((__, ___) -> {
                callbackCalled.set(true);
                throw new org.apache.kafka.common.errors.OutOfOrderSequenceException("Exception thrown in AcknowledgementCommitCallback.onComplete");
            });
            shareConsumer.subscribe(Set.of(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            TestUtils.waitForCondition(() -> {
                try {
                    shareConsumer.poll(Duration.ofMillis(500));
                } catch (Exception e) {
                    throw new NoRetryException(e);
                }
                return callbackCalled.get();
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Received unexpected exception or callback not called");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }
}