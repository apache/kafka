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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
public class ShareConsumerTest extends ShareConsumerTestBase {

    public ShareConsumerTest(ClusterInstance cluster) {
        super(cluster);
    }

    @ClusterTest
    public void testPollNoSubscribeFails() {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            assertEquals(Set.of(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        }
    }

    @ClusterTest
    public void testSubscribeAndPollNoRecords() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Set.of(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscribePollUnsubscribe() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Set.of(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.unsubscribe();
            assertEquals(Set.of(), shareConsumer.subscription());
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscribePollSubscribe() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Set.of(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscribeUnsubscribePollFails() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Set.of(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.unsubscribe();
            assertEquals(Set.of(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();   // due to leader epoch in read
        }
    }

    @ClusterTest
    public void testSubscribeSubscribeEmptyPollFails() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Set.of(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.subscribe(Set.of());
            assertEquals(Set.of(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();   // due to leader epoch in read
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testSubscriptionAndPoll() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Set.of(tp.topic()));
            assertEquals(Optional.empty(), shareConsumer.acquisitionLockTimeoutMs());
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testSubscriptionAndPollMultiple() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Set.of(tp.topic()));
            assertEquals(Optional.empty(), shareConsumer.acquisitionLockTimeoutMs());
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());
            producer.send(record);
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            producer.send(record);
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testPollRecordsGreaterThanMaxBytes() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testHeaders() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            int numRecords = 1;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            record.headers().add("headerKey", "headerValue".getBytes());
            record.headers().add("headerKey2", "headerValue2".getBytes());
            record.headers().add("headerKey3", "headerValue3".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
            assertEquals(numRecords, records.size());

            Header header = records.get(0).headers().lastHeader("headerKey");
            assertEquals("headerValue", new String(header.value()));

            // Test the order of headers in a record is preserved when producing and consuming
            Header[] headers = records.get(0).headers().toArray();
            assertEquals("headerKey", headers[0].key());
            assertEquals("headerKey2", headers[1].key());
            assertEquals("headerKey3", headers[2].key());

            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    private void testHeadersSerializeDeserialize(Serializer<byte[]> serializer, Deserializer<byte[]> deserializer) {
        alterShareAutoOffsetReset("group1", "earliest");
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getClass().getName()
        );

        Map<String, Object> consumerConfig = Map.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer.getClass().getName()
        );

        try (Producer<byte[], byte[]> producer = createProducer(producerConfig);
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", consumerConfig)) {

            int numRecords = 1;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
            assertEquals(numRecords, records.size());
        }
    }

    @ClusterTest
    public void testHeadersSerializerDeserializer() {
        testHeadersSerializeDeserialize(new SerializerImpl(), new DeserializerImpl());
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testMaxPollRecords() {
        int numRecords = 10000;
        int maxPollRecords = 2;

        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1",
            Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)))) {

            long startingTimestamp = System.currentTimeMillis();
            produceMessagesWithTimestamp(numRecords, startingTimestamp);

            shareConsumer.subscribe(Set.of(tp.topic()));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
            long i = 0L;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                assertEquals(TimestampType.CREATE_TIME, record.timestampType());
                assertEquals(startingTimestamp + i, record.timestamp());
                assertEquals("key " + i, new String(record.key()));
                assertEquals("value " + i, new String(record.value()));
                // this is true only because K and V are byte arrays
                assertEquals(("key " + i).length(), record.serializedKeySize());
                assertEquals(("value " + i).length(), record.serializedValueSize());

                i++;
            }
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testControlRecordsSkipped() throws Exception {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             Producer<byte[], byte[]> nonTransactionalProducer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            transactionalProducer.initTransactions();
            transactionalProducer.beginTransaction();
            RecordMetadata transactional1 = transactionalProducer.send(record).get();

            RecordMetadata nonTransactional1 = nonTransactionalProducer.send(record).get();

            transactionalProducer.commitTransaction();

            transactionalProducer.beginTransaction();
            RecordMetadata transactional2 = transactionalProducer.send(record).get();
            transactionalProducer.abortTransaction();

            RecordMetadata nonTransactional2 = nonTransactionalProducer.send(record).get();

            transactionalProducer.close();
            nonTransactionalProducer.close();

            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 4);
            assertEquals(4, records.count());
            assertEquals(transactional1.offset(), records.records(tp).get(0).offset());
            assertEquals(nonTransactional1.offset(), records.records(tp).get(1).offset());
            assertEquals(transactional2.offset(), records.records(tp).get(2).offset());
            assertEquals(nonTransactional2.offset(), records.records(tp).get(3).offset());

            // There will be control records on the topic-partition, so the offsets of the non-control records
            // are not 0, 1, 2, 3. Just assert that the offset of the final one is not 3.
            assertNotEquals(3, nonTransactional2.offset());

            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeSuccess() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(shareConsumer::acknowledge);
            producer.send(record);
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeCommitSuccess() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(shareConsumer::acknowledge);
            producer.send(record);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgementCommitAsync() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer1.subscribe(Set.of(tp.topic()));
            shareConsumer2.subscribe(Set.of(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();
            shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer1, 2500L, 3);
            assertEquals(3, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            // Acknowledging 2 out of the 3 records received via commitAsync.
            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> secondRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> thirdRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            assertEquals(1L, secondRecord.offset());

            shareConsumer1.acknowledge(firstRecord);
            shareConsumer1.acknowledge(secondRecord);
            shareConsumer1.acknowledge(thirdRecord, AcknowledgeType.RELEASE);
            shareConsumer1.commitAsync();

            // The 3rd record should be reassigned to 2nd consumer when it polls.
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(1000));
                return records2.count() == 1 && records2.iterator().next().offset() == 2L;
            }, 30000, 100L, () -> "Didn't receive timed out record");

            assertFalse(partitionExceptionMap1.containsKey(tp));

            // The callback will receive the acknowledgement responses asynchronously after the next poll.
            TestUtils.waitForCondition(() -> {
                shareConsumer1.poll(Duration.ofMillis(1000));
                return partitionOffsetsMap1.containsKey(tp);
            }, 30000, 100L, () -> "Didn't receive call to callback");

            assertFalse(partitionExceptionMap1.containsKey(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgementCommitAsyncPartialBatch() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record4 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer1.subscribe(Set.of(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer1, 2500L, 3);
            assertEquals(3, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            // Acknowledging 2 out of the 3 records received via commitAsync.
            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> secondRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> thirdRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            assertEquals(1L, secondRecord.offset());

            shareConsumer1.acknowledge(firstRecord);
            shareConsumer1.acknowledge(secondRecord);
            shareConsumer1.commitAsync();

            producer.send(record4);
            producer.flush();

            // The next poll() should throw an IllegalStateException as there is still 1 unacknowledged record.
            // In EXPLICIT acknowledgement mode, we are not allowed to have unacknowledged records from a batch.
            assertThrows(IllegalStateException.class, () -> shareConsumer1.poll(Duration.ofMillis(5000)));

            // Acknowledging the 3rd record
            shareConsumer1.acknowledge(thirdRecord);
            shareConsumer1.commitAsync();

            // The next poll() will not throw an exception, it would continue to fetch more records.
            records = shareConsumer1.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            iterator = records.iterator();
            ConsumerRecord<byte[], byte[]> fourthRecord = iterator.next();
            assertEquals(3L, fourthRecord.offset());

            shareConsumer1.acknowledge(fourthRecord);

            // The callback will receive the acknowledgement responses after polling. The callback is
            // called on entry to the poll method or during close. The commit is being performed asynchronously, so
            // we can only rely on the completion once the consumer has closed because that waits for the response.
            shareConsumer1.poll(Duration.ofMillis(500));

            shareConsumer1.close();

            assertFalse(partitionExceptionMap.containsKey(tp));
            assertTrue(partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.get(tp).size() == 4);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleasePollAccept() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleaseAccept() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleaseClose() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeThrowsNotInBatch() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
            shareConsumer.acknowledge(consumedRecord);
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitOverrideAcknowledgeCorruptedMessage() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT),
                null,
                mockErrorDeserializer(3))) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofSeconds(60));
            assertEquals(2, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> secondRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            assertEquals(1L, secondRecord.offset());
            shareConsumer.acknowledge(firstRecord);
            shareConsumer.acknowledge(secondRecord);

            RecordDeserializationException rde = assertThrows(RecordDeserializationException.class, () -> shareConsumer.poll(Duration.ofSeconds(60)));
            assertEquals(2, rde.offset());
            shareConsumer.commitSync();

            // The corrupted record was automatically released, so we can still obtain it.
            rde = assertThrows(RecordDeserializationException.class, () -> shareConsumer.poll(Duration.ofSeconds(60)));
            assertEquals(2, rde.offset());

            // Reject this record
            shareConsumer.acknowledge(rde.topicPartition().topic(), rde.topicPartition().partition(), rde.offset(), AcknowledgeType.REJECT);
            shareConsumer.commitSync();

            records = shareConsumer.poll(Duration.ZERO);
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeOffsetThrowsNotException() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofSeconds(60));
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
            assertEquals(0L, consumedRecord.offset());

            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(tp.topic(), tp.partition(), consumedRecord.offset(), AcknowledgeType.ACCEPT));

            shareConsumer.acknowledge(consumedRecord);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeOffsetThrowsParametersError() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT),
                null,
                mockErrorDeserializer(2))) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofSeconds(60));
            assertEquals(1, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            shareConsumer.acknowledge(firstRecord);

            final RecordDeserializationException rde = assertThrows(RecordDeserializationException.class, () -> shareConsumer.poll(Duration.ofSeconds(60)));
            assertEquals(1, rde.offset());

            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge("foo", rde.topicPartition().partition(), rde.offset(), AcknowledgeType.REJECT));
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(rde.topicPartition().topic(), 1, rde.offset(), AcknowledgeType.REJECT));
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(rde.topicPartition().topic(), tp2.partition(), 0, AcknowledgeType.REJECT));

            // Reject this record.
            shareConsumer.acknowledge(rde.topicPartition().topic(), rde.topicPartition().partition(), rde.offset(), AcknowledgeType.REJECT);
            shareConsumer.commitSync();

            // The next acknowledge() should throw an IllegalStateException as the record has been acked.
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(rde.topicPartition().topic(), rde.topicPartition().partition(), rde.offset(), AcknowledgeType.REJECT));

            records = shareConsumer.poll(Duration.ZERO);
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    private ByteArrayDeserializer mockErrorDeserializer(int recordNumber) {
        int recordIndex = recordNumber - 1;
        return new ByteArrayDeserializer() {
            int i = 0;

            @Override
            public byte[] deserialize(String topic, Headers headers, ByteBuffer data) {
                if (i == recordIndex) {
                    throw new SerializationException();
                } else {
                    i++;
                    return super.deserialize(topic, headers, data);
                }
            }
        };
    }

    @ClusterTest
    public void testImplicitAcknowledgeFailsExplicit() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testImplicitAcknowledgeCommitSync() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            AtomicReference<ConsumerRecords<byte[], byte[]>> recordsAtomic = new AtomicReference<>();
            waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> recs = shareConsumer.poll(Duration.ofMillis(2500L));
                    recordsAtomic.set(recs);
                    return recs.count() == 1;
                },
                DEFAULT_MAX_WAIT_MS,
                500L,
                () -> "records not found"
            );
            ConsumerRecords<byte[], byte[]> records = recordsAtomic.get();
            assertEquals(1, records.count());
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            result = shareConsumer.commitSync();
            assertEquals(0, result.size());
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testImplicitAcknowledgementCommitAsync() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 3);
            assertEquals(3, records.count());

            // Implicitly acknowledging all the records received.
            shareConsumer.commitAsync();

            assertFalse(partitionExceptionMap1.containsKey(tp));
            // The callback will receive the acknowledgement responses after the next poll.
            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(1000));
                return partitionOffsetsMap1.containsKey(tp);
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Acknowledgement commit callback did not receive the response yet");

            assertFalse(partitionExceptionMap1.containsKey(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testConfiguredExplicitAcknowledgeCommitSuccess() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit"))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            records.forEach(shareConsumer::acknowledge);
            producer.send(record);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testConfiguredImplicitAcknowledgeExplicitAcknowledgeFails() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, IMPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(records.iterator().next()));
        }
    }

    @ClusterTest
    public void testFetchRecordLargerThanMaxPartitionFetchBytes() throws Exception {
        int maxPartitionFetchBytes = 10000;

        alterShareAutoOffsetReset("group1", "earliest");
        try (
            Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionFetchBytes)))) {

            ProducerRecord<byte[], byte[]> smallRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> bigRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), new byte[maxPartitionFetchBytes]);
            producer.send(smallRecord).get();
            producer.send(bigRecord).get();

            shareConsumer.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 2);
            assertEquals(2, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testMultipleConsumersWithDifferentGroupIds() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareAutoOffsetReset("group2", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group2")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            shareConsumer1.subscribe(Set.of(tp.topic()));
            shareConsumer2.subscribe(Set.of(tp.topic()));

            // producing 3 records to the topic
            producer.send(record);
            producer.send(record);
            producer.send(record);
            producer.flush();

            // Both the consumers should read all the messages, because they are part of different share groups (both have different group IDs)
            AtomicInteger shareConsumer1Records = new AtomicInteger();
            AtomicInteger shareConsumer2Records = new AtomicInteger();
            TestUtils.waitForCondition(() -> {
                int records1 = shareConsumer1Records.addAndGet(shareConsumer1.poll(Duration.ofMillis(2000)).count());
                int records2 = shareConsumer2Records.addAndGet(shareConsumer2.poll(Duration.ofMillis(2000)).count());
                return records1 == 3 && records2 == 3;
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for both consumers");

            producer.send(record);
            producer.send(record);

            shareConsumer1Records.set(0);
            TestUtils.waitForCondition(() -> shareConsumer1Records.addAndGet(shareConsumer1.poll(Duration.ofMillis(2000)).count()) == 2,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer 1");

            producer.send(record);
            producer.send(record);
            producer.send(record);

            shareConsumer1Records.set(0);
            shareConsumer2Records.set(0);
            TestUtils.waitForCondition(() -> {
                int records1 = shareConsumer1Records.addAndGet(shareConsumer1.poll(Duration.ofMillis(2000)).count());
                int records2 = shareConsumer2Records.addAndGet(shareConsumer2.poll(Duration.ofMillis(2000)).count());
                return records1 == 3 && records2 == 5;
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for both consumers for the last batch");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testConsumerCloseOnBrokerShutdown() {
        alterShareAutoOffsetReset("group1", "earliest");
        ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
        shareConsumer.subscribe(Set.of(tp.topic()));

        // To ensure coordinator discovery is complete before shutting down the broker
        shareConsumer.poll(Duration.ofMillis(100));

        // Shutdown the broker.
        assertEquals(1, cluster.brokers().size());
        KafkaBroker broker = cluster.brokers().get(0);
        cluster.shutdownBroker(0);

        broker.awaitShutdown();

        // Assert that close completes in less than 5 seconds, not the full 30-second timeout.
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> shareConsumer.close(),
            "Consumer close should not wait for full timeout when broker is already shut down");
    }

    @ClusterTest
    public void testLeaderRestartWithoutLeadershipChangeExplicitAcknowledgementSync() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            AtomicBoolean callbackCalled = new AtomicBoolean(false);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) -> {
                assertInstanceOf(NetworkException.class, exception);
                callbackCalled.set(true);
            });

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer.subscribe(Set.of(tp.topic()));

            producer.send(record);
            producer.flush();

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(20000));
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumerRecord = records.iterator().next();

            // Shutdown the broker
            assertEquals(1, cluster.brokers().size());
            KafkaBroker broker = cluster.brokers().get(0);
            cluster.shutdownBroker(0);

            broker.awaitShutdown();

            // Restart the broker
            cluster.startBroker(0);

            shareConsumer.acknowledge(consumerRecord);
            Map<TopicIdPartition, Optional<KafkaException>> commitResult = shareConsumer.commitSync(Duration.ofMillis(30000));
            assertEquals(1, commitResult.size());
            TopicIdPartition tidp = commitResult.keySet().iterator().next();
            assertTrue(commitResult.get(tidp).isPresent());
            assertInstanceOf(NetworkException.class, commitResult.get(tidp).get());

            assertTrue(callbackCalled.get());
        }
    }

    @ClusterTest
    public void testLeaderRestartWithoutLeadershipChangeExplicitAcknowledgementAsync() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            AtomicBoolean callbackCalled = new AtomicBoolean(false);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) -> {
                assertInstanceOf(NetworkException.class, exception);
                callbackCalled.set(true);
            });

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer.subscribe(Set.of(tp.topic()));

            producer.send(record);
            producer.flush();

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(20000));
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumerRecord = records.iterator().next();

            // Shutdown the broker
            assertEquals(1, cluster.brokers().size());
            KafkaBroker broker = cluster.brokers().get(0);
            cluster.shutdownBroker(0);

            broker.awaitShutdown();

            // Restart the broker
            cluster.startBroker(0);

            shareConsumer.acknowledge(consumerRecord);
            shareConsumer.commitAsync();

            int maxRetries = 15;
            int retries = 0;
            while (retries < maxRetries) {
                shareConsumer.poll(Duration.ofMillis(2000));
                if (callbackCalled.get()) {
                    break;
                }
                retries++;
            }

            assertTrue(callbackCalled.get());
        }
    }

    @ClusterTest
    public void testLeaderRestartWithoutLeadershipChangeImplicitAcknowledgement() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            AtomicBoolean callbackCalled = new AtomicBoolean(false);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) -> {
                assertInstanceOf(NetworkException.class, exception);
                callbackCalled.set(true);
            });

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer.subscribe(Set.of(tp.topic()));

            producer.send(record);
            producer.flush();

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(20000));
            assertEquals(1, records.count());

            // Shutdown the broker
            assertEquals(1, cluster.brokers().size());
            KafkaBroker broker = cluster.brokers().get(0);
            cluster.shutdownBroker(0);

            broker.awaitShutdown();

            // Restart the broker
            cluster.startBroker(0);

            int maxRetries = 15;
            int retries = 0;
            while (retries < maxRetries) {
                shareConsumer.poll(Duration.ofMillis(2000));
                if (callbackCalled.get()) {
                    break;
                }
                retries++;
            }

            assertTrue(callbackCalled.get());
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testMultipleConsumersInGroupSequentialConsumption() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer1.subscribe(Set.of(tp.topic()));
            shareConsumer2.subscribe(Set.of(tp.topic()));

            int totalMessages = 2000;
            for (int i = 0; i < totalMessages; i++) {
                producer.send(record);
            }
            producer.flush();

            int consumer1MessageCount = 0;
            int consumer2MessageCount = 0;

            int maxRetries = 10;
            int retries = 0;
            while (retries < maxRetries) {
                ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(2000));
                consumer1MessageCount += records1.count();
                ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(2000));
                consumer2MessageCount += records2.count();
                if (records1.count() + records2.count() == 0)
                    break;
                retries++;
            }

            assertEquals(totalMessages, consumer1MessageCount + consumer2MessageCount);
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testMultipleConsumersInGroupConcurrentConsumption()
        throws InterruptedException, ExecutionException, TimeoutException {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 4;
        int producerCount = 4;
        int messagesPerProducer = 5000;

        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");

        List<CompletableFuture<Void>> producerFutures = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            producerFutures.add(CompletableFuture.runAsync(() -> produceMessages(messagesPerProducer)));
        }

        int maxBytes = 100000;
        List<CompletableFuture<Integer>> consumerFutures = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            consumerFutures.add(CompletableFuture.supplyAsync(() ->
                consumeMessages(totalMessagesConsumed,
                    producerCount * messagesPerProducer, groupId, consumerNumber,
                    30, true, maxBytes)));
        }

        CompletableFuture.allOf(producerFutures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
        CompletableFuture.allOf(consumerFutures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

        int totalResult = consumerFutures.stream().mapToInt(CompletableFuture::join).sum();
        assertEquals(producerCount * messagesPerProducer, totalResult);
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testMultipleConsumersInMultipleGroupsConcurrentConsumption()
        throws ExecutionException, InterruptedException, TimeoutException {
        AtomicInteger totalMessagesConsumedGroup1 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup2 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup3 = new AtomicInteger(0);

        int producerCount = 4;
        int messagesPerProducer = 2000;
        final int totalMessagesSent = producerCount * messagesPerProducer;

        String groupId1 = "group1";
        String groupId2 = "group2";
        String groupId3 = "group3";

        alterShareAutoOffsetReset(groupId1, "earliest");
        alterShareAutoOffsetReset(groupId2, "earliest");
        alterShareAutoOffsetReset(groupId3, "earliest");

        List<CompletableFuture<Integer>> producerFutures = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            producerFutures.add(CompletableFuture.supplyAsync(() -> produceMessages(messagesPerProducer)));
        }
        // Wait for the producers to run
        assertDoesNotThrow(() -> CompletableFuture.allOf(producerFutures.toArray(CompletableFuture[]::new))
            .get(15, TimeUnit.SECONDS), "Exception awaiting produceMessages");
        int actualMessageSent = producerFutures.stream().mapToInt(CompletableFuture::join).sum();

        List<CompletableFuture<Integer>> consumeMessagesFutures1 = new ArrayList<>();
        List<CompletableFuture<Integer>> consumeMessagesFutures2 = new ArrayList<>();
        List<CompletableFuture<Integer>> consumeMessagesFutures3 = new ArrayList<>();

        int maxBytes = 100000;
        for (int i = 0; i < 2; i++) {
            final int consumerNumber = i + 1;
            consumeMessagesFutures1.add(CompletableFuture.supplyAsync(() ->
                consumeMessages(totalMessagesConsumedGroup1, totalMessagesSent,
                    "group1", consumerNumber, 100, true, maxBytes)));

            consumeMessagesFutures2.add(CompletableFuture.supplyAsync(() ->
                consumeMessages(totalMessagesConsumedGroup2, totalMessagesSent,
                    "group2", consumerNumber, 100, true, maxBytes)));

            consumeMessagesFutures3.add(CompletableFuture.supplyAsync(() ->
                consumeMessages(totalMessagesConsumedGroup3, totalMessagesSent,
                    "group3", consumerNumber, 100, true, maxBytes)));
        }

        CompletableFuture.allOf(Stream.of(consumeMessagesFutures1.stream(), consumeMessagesFutures2.stream(),
                consumeMessagesFutures3.stream()).flatMap(Function.identity()).toArray(CompletableFuture[]::new))
            .get(120, TimeUnit.SECONDS);

        int totalResult1 = consumeMessagesFutures1.stream().mapToInt(CompletableFuture::join).sum();
        int totalResult2 = consumeMessagesFutures2.stream().mapToInt(CompletableFuture::join).sum();
        int totalResult3 = consumeMessagesFutures3.stream().mapToInt(CompletableFuture::join).sum();

        assertEquals(totalMessagesSent, totalResult1);
        assertEquals(totalMessagesSent, totalResult2);
        assertEquals(totalMessagesSent, totalResult3);
        assertEquals(totalMessagesSent, actualMessageSent);
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testConsumerCloseInGroupSequential() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer1.subscribe(Set.of(tp.topic()));
            shareConsumer2.subscribe(Set.of(tp.topic()));

            int totalMessages = 1500;
            for (int i = 0; i < totalMessages; i++) {
                producer.send(record);
            }
            producer.close();

            int consumer1MessageCount = 0;
            int consumer2MessageCount = 0;

            // Poll until we receive all the records. The second poll acknowledges the records
            // from the first poll, and so on.
            // The last poll's records are not acknowledged
            // because the consumer is closed, which makes the broker release the records fetched.
            int maxRetries = 10;
            int retries = 0;
            int lastPollRecordCount = 0;
            while (consumer1MessageCount < totalMessages && retries < maxRetries) {
                lastPollRecordCount = shareConsumer1.poll(Duration.ofMillis(5000)).count();
                consumer1MessageCount += lastPollRecordCount;
                retries++;
            }
            assertEquals(totalMessages, consumer1MessageCount);
            shareConsumer1.close();

            // These records are released after the first consumer closes.
            consumer1MessageCount -= lastPollRecordCount;

            retries = 0;
            while (consumer1MessageCount + consumer2MessageCount < totalMessages && retries < maxRetries) {
                ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(5000));
                consumer2MessageCount += records2.count();
                retries++;
            }
            shareConsumer2.close();
            assertEquals(totalMessages, consumer1MessageCount + consumer2MessageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testMultipleConsumersInGroupFailureConcurrentConsumption()
        throws InterruptedException, ExecutionException, TimeoutException {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 4;
        int producerCount = 4;
        int messagesPerProducer = 5000;

        String groupId = "group1";

        alterShareAutoOffsetReset(groupId, "earliest");

        List<CompletableFuture<Void>> produceMessageFutures = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            produceMessageFutures.add(CompletableFuture.runAsync(() -> produceMessages(messagesPerProducer)));
        }

        int maxBytes = 1000000;

        // The "failing" consumer polls but immediately closes, which releases the records for the other consumers
        CompletableFuture<Integer> failedMessagesConsumedFuture = CompletableFuture.supplyAsync(
            () -> consumeMessages(new AtomicInteger(0), producerCount * messagesPerProducer, groupId,
                0, 1, false));

        // Wait for the failed consumer to run
        assertDoesNotThrow(() -> failedMessagesConsumedFuture.get(15, TimeUnit.SECONDS),
            "Exception awaiting consumeMessages");

        List<CompletableFuture<Integer>> consumeMessagesFutures = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            consumeMessagesFutures.add(CompletableFuture.supplyAsync(
                () -> consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer,
                    groupId, consumerNumber, 40, true, maxBytes)));
        }

        CompletableFuture.allOf(produceMessageFutures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
        CompletableFuture.allOf(consumeMessagesFutures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

        int totalSuccessResult = consumeMessagesFutures.stream().mapToInt(CompletableFuture::join).sum();
        assertEquals(producerCount * messagesPerProducer, totalSuccessResult);
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "group.share.min.record.lock.duration.ms", value = "5000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "5000")
    })
    public void testAcquisitionLockTimeoutOnConsumer() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> producerRecord1 = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key_1".getBytes(), "value_1".getBytes());
            ProducerRecord<byte[], byte[]> producerRecord2 = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key_2".getBytes(), "value_2".getBytes());
            shareConsumer.subscribe(Set.of(tp.topic()));

            // Produce a first record which is consumed and acknowledged normally.
            producer.send(producerRecord1);
            producer.flush();

            // Poll twice to receive records. The first poll fetches the record and starts the acquisition lock timer.
            // Since, we are only sending one record and the acquisition lock hasn't timed out, the second poll only
            // acknowledges the record from the first poll and does not fetch any more records.
            ConsumerRecords<byte[], byte[]> consumerRecords = waitedPoll(shareConsumer, 2500L, 1);
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.records(tp).get(0);
            assertEquals("key_1", new String(consumerRecord.key()));
            assertEquals("value_1", new String(consumerRecord.value()));
            assertEquals(1, consumerRecords.count());

            consumerRecords = shareConsumer.poll(Duration.ofMillis(1000));
            assertEquals(0, consumerRecords.count());

            // Produce a second record which is fetched, but not acknowledged before it times out. The record will
            // be released automatically by the broker. It is then fetched again and acknowledged normally.
            producer.send(producerRecord2);
            producer.flush();

            // Poll three more times. The first poll fetches the second record and starts the acquisition lock timer.
            // Before the second poll, acquisition lock times out and hence the consumer needs to fetch the record again.
            // The acquisition lock doesn't time out between the second and third polls, so the third poll only acknowledges
            // the record from the second poll and does not fetch any more records.
            consumerRecords = shareConsumer.poll(Duration.ofMillis(5000));
            consumerRecord = consumerRecords.records(tp).get(0);
            assertEquals("key_2", new String(consumerRecord.key()));
            assertEquals("value_2", new String(consumerRecord.value()));
            assertEquals(1, consumerRecords.count());

            // Allow the acquisition lock to time out.
            Thread.sleep(8000);

            consumerRecords = shareConsumer.poll(Duration.ofMillis(5000));
            consumerRecord = consumerRecords.records(tp).get(0);
            // By checking the key and value before the count, we get a bit more information if too many records are returned.
            // This test has been observed to fail very occasionally because of this.
            assertEquals("key_2", new String(consumerRecord.key()));
            assertEquals("value_2", new String(consumerRecord.value()));
            assertEquals(1, consumerRecords.count());

            consumerRecords = shareConsumer.poll(Duration.ofMillis(1000));
            assertEquals(0, consumerRecords.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    /**
     * Test to verify that calling Thread.interrupt() before ShareConsumer.poll(Duration)
     * causes it to throw InterruptException
     */
    @ClusterTest
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            shareConsumer.subscribe(Set.of(tp.topic()));

            // interrupt the thread and call poll
            try {
                Thread.currentThread().interrupt();
                assertThrows(InterruptException.class, () -> shareConsumer.poll(Duration.ZERO));
            } finally {
                // clear interrupted state again since this thread may be reused by JUnit
                Thread.interrupted();
            }

            assertDoesNotThrow(() -> shareConsumer.poll(Duration.ZERO), "Failed to consume records");
        }
    }

    /**
     * Test to verify that InvalidTopicException is thrown if the consumer subscribes
     * to an invalid topic.
     */
    @ClusterTest
    public void testSubscribeOnInvalidTopicThrowsInvalidTopicException() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            shareConsumer.subscribe(Set.of("topic abc"));

            // The exception depends upon a metadata response which arrives asynchronously. If the delay is
            // too short, the poll might return before the error is known.
            assertThrows(InvalidTopicException.class, () -> shareConsumer.poll(Duration.ofMillis(10000)));
        }
    }

    /**
     * Test to ensure that a wakeup when records are buffered doesn't prevent the records
     * being returned on the next poll.
     */
    @ClusterTest
    public void testWakeupWithFetchedRecordsAvailable() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));

            shareConsumer.wakeup();
            assertThrows(WakeupException.class, () -> shareConsumer.poll(Duration.ZERO));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testSubscriptionFollowedByTopicCreation() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            String topic = "foo";
            shareConsumer.subscribe(Set.of(topic));

            // Topic is created post creation of share consumer and subscription
            createTopic(topic);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer, metadata sync failed");

            producer.send(record);
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            producer.send(record);
            records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "0")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, value = "1000")
        })
    })
    public void testSubscriptionAndPollFollowedByTopicDeletion() throws InterruptedException, ExecutionException {
        String topic1 = "bar";
        String topic2 = "baz";
        createTopic(topic1);
        createTopic(topic2);

        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> recordTopic1 = new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> recordTopic2 = new ProducerRecord<>(topic2, 0, null, "key".getBytes(), "value".getBytes());

            // Consumer subscribes to the topics -> bar and baz.
            shareConsumer.subscribe(Arrays.asList(topic1, topic2));

            producer.send(recordTopic1).get();
            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

            producer.send(recordTopic2).get();
            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

            // Topic bar is deleted, hence poll should not give any results.
            deleteTopic(topic1);
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());

            producer.send(recordTopic2).get();
            // Poll should give the record from the non-deleted topic baz.
            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

            producer.send(recordTopic2).get();
            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testLsoMovementByRecordsDeletion() {
        String groupId = "group1";

        alterShareAutoOffsetReset(groupId, "earliest");
        try (
            Producer<byte[], byte[]> producer = createProducer();
            Admin adminClient = createAdminClient()
        ) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), 0, null, "key".getBytes(), "value".getBytes());

            // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
            for (int i = 0; i < 10; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 5, so the LSO should move to 5.
            assertDoesNotThrow(() -> adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5L))).all().get(), "Failed to delete records");

            int messageCount = consumeMessages(new AtomicInteger(0), 5, groupId, 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, messageCount);

            // We write 5 records to the topic, so they would be written from offsets 10-14 on the topic.
            for (int i = 0; i < 5; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 14, so the LSO should move to 14.
            assertDoesNotThrow(() -> adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(14L))).all().get(), "Failed to delete records");

            int consumeMessagesCount = consumeMessages(new AtomicInteger(0), 1, groupId, 1, 10, true);
            // The record returned belong to offset 14.
            assertEquals(1, consumeMessagesCount);

            // We delete records before offset 15, so the LSO should move to 15 and now no records should be returned.
            assertDoesNotThrow(() -> adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(15L))).all().get(), "Failed to delete records");

            messageCount = consumeMessages(new AtomicInteger(0), 0, groupId, 1, 5, true);
            assertEquals(0, messageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    @Timeout(90)
    public void testShareConsumerAfterCoordinatorMovement() throws Exception {
        String topicName = "multipart";
        String groupId = "multipartGrp";
        Uuid topicId = createTopic(topicName, 3, 3);
        alterShareAutoOffsetReset(groupId, "earliest");
        ScheduledExecutorService service = Executors.newScheduledThreadPool(5);

        TopicPartition tpMulti = new TopicPartition(topicName, 0);

        // produce some messages
        ClientState prodState = new ClientState();
        final Set<String> produced = new HashSet<>();
        service.execute(() -> {
                int i = 0;
                try (Producer<String, String> producer = createProducer(Map.of(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
                ))) {
                    while (!prodState.done().get()) {
                        String key = "key-" + (i++);
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            tpMulti.topic(),
                            tpMulti.partition(),
                            null,
                            key,
                            "value"
                        );
                        try {
                            producer.send(record);
                            producer.flush();
                            // count only correctly produced records
                            prodState.count().incrementAndGet();
                            produced.add(key);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
            }
        );

        // consume messages - start after small delay
        ClientState consState = new ClientState();
        // using map here if we want to debug specific keys
        Map<String, Integer> consumed = new HashMap<>();
        service.schedule(() -> {
                try (ShareConsumer<String, String> shareConsumer = createShareConsumer(groupId, Map.of(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                ))) {
                    shareConsumer.subscribe(List.of(topicName));
                    while (!consState.done().get()) {
                        ConsumerRecords<String, String> records = shareConsumer.poll(Duration.ofMillis(2000L));
                        consState.count().addAndGet(records.count());
                        records.forEach(rec -> consumed.compute(rec.key(), (k, v) -> v == null ? 1 : v + 1));
                        if (prodState.done().get() && records.count() == 0) {
                            consState.done().set(true);
                        }
                    }
                }
            }, 100L, TimeUnit.MILLISECONDS
        );

        // To be closer to real world scenarios, we will execute after
        // some time has elapsed since the producer and consumer started
        // working.
        service.schedule(() -> {
                // Get the current node hosting the __share_group_state partition
                // on which tpMulti is hosted. Then shut down this node and wait
                // for it to be gracefully shutdown. Then fetch the coordinator again
                // and verify that it has moved to some other broker.
                try (Admin admin = createAdminClient()) {
                    SharePartitionKey key = SharePartitionKey.getInstance(groupId, new TopicIdPartition(topicId, tpMulti));
                    int shareGroupStateTp = Utils.abs(key.asCoordinatorKey().hashCode()) % 3;
                    List<Integer> curShareCoordNodeId = null;
                    try {
                        curShareCoordNodeId = topicPartitionLeader(admin, Topic.SHARE_GROUP_STATE_TOPIC_NAME, shareGroupStateTp);
                    } catch (Exception e) {
                        fail(e);
                    }
                    assertEquals(1, curShareCoordNodeId.size());

                    // shutdown the coordinator
                    KafkaBroker broker = cluster.brokers().get(curShareCoordNodeId.get(0));
                    cluster.shutdownBroker(curShareCoordNodeId.get(0));

                    // wait for it to be completely shutdown
                    broker.awaitShutdown();

                    List<Integer> newShareCoordNodeId = null;
                    try {
                        newShareCoordNodeId = topicPartitionLeader(admin, Topic.SHARE_GROUP_STATE_TOPIC_NAME, shareGroupStateTp);
                    } catch (Exception e) {
                        fail(e);
                    }

                    assertEquals(1, newShareCoordNodeId.size());
                    assertNotEquals(curShareCoordNodeId.get(0), newShareCoordNodeId.get(0));
                }
            }, 5L, TimeUnit.SECONDS
        );

        // top the producer after some time (but after coordinator shutdown)
        service.schedule(() -> prodState.done().set(true), 10L, TimeUnit.SECONDS);

        // wait for both producer and consumer to finish
        TestUtils.waitForCondition(
            () -> prodState.done().get() && consState.done().get(),
            45_000L,
            500L,
            () -> "prod/cons not done yet"
        );

        // Make sure we consumed all records. Consumed records could be higher
        // due to re-delivery but that is expected since we are only guaranteeing
        // at least once semantics.
        assertTrue(prodState.count().get() <= consState.count().get());
        Set<String> consumedKeys = consumed.keySet();
        assertTrue(produced.containsAll(consumedKeys) && consumedKeys.containsAll(produced));

        shutdownExecutorService(service);

        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
            @ClusterConfigProperty(key = "group.share.max.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    @Timeout(150)
    public void testComplexShareConsumer() throws Exception {
        String topicName = "multipart";
        String groupId = "multipartGrp";
        createTopic(topicName, 3, 3);
        TopicPartition multiTp = new TopicPartition(topicName, 0);

        ScheduledExecutorService service = Executors.newScheduledThreadPool(5);

        ClientState prodState = new ClientState();

        // Produce a fixed number of messages for deterministic testing.
        int targetRecordCount = 2000;
        service.execute(() -> {
            try (Producer<byte[], byte[]> producer = createProducer()) {
                do {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(multiTp.topic(), multiTp.partition(), null, "key".getBytes(), "value".getBytes());
                    producer.send(record);
                    producer.flush();
                } while (prodState.count().incrementAndGet() < targetRecordCount);
                prodState.done().set(true);
            }
        });

        // Init a complex share consumer.
        ComplexShareConsumer<byte[], byte[]> complexCons1 = new ComplexShareConsumer<>(
            cluster.bootstrapServers(),
            topicName,
            groupId,
            Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT)
        );
        alterShareAutoOffsetReset(groupId, "earliest");

        service.schedule(
            complexCons1,
            100L,
            TimeUnit.MILLISECONDS
        );

        // All messages which can be read are read, some would be redelivered (roughly 2 times the records produced).
        TestUtils.waitForCondition(complexCons1::isDone, 45_000L, () -> "did not close!");
        int delta = complexCons1.recordsRead() - (int) (prodState.count().get() * 2 * 0.95);    // 2 times with margin of error (5%).

        assertTrue(delta > 0,
            String.format("Producer (%d) and share consumer (%d) record count mismatch.", prodState.count().get(), complexCons1.recordsRead()));

        shutdownExecutorService(service);

        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest(
        brokers = 1,
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
            @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.share.max.size", value = "3") // Setting max group size to 3
        }
    )
    public void testShareGroupMaxSizeConfigExceeded() throws Exception {
        // creating 3 consumers in the group1
        ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
        ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1");
        ShareConsumer<byte[], byte[]> shareConsumer3 = createShareConsumer("group1");

        shareConsumer1.subscribe(Set.of(tp.topic()));
        shareConsumer2.subscribe(Set.of(tp.topic()));
        shareConsumer3.subscribe(Set.of(tp.topic()));

        shareConsumer1.poll(Duration.ofMillis(5000));
        shareConsumer2.poll(Duration.ofMillis(5000));
        shareConsumer3.poll(Duration.ofMillis(5000));

        ShareConsumer<byte[], byte[]> shareConsumer4 = createShareConsumer("group1");
        shareConsumer4.subscribe(Set.of(tp.topic()));

        TestUtils.waitForCondition(() -> {
            try {
                shareConsumer4.poll(Duration.ofMillis(5000));
            } catch (GroupMaxSizeReachedException e) {
                return true;
            } catch (Exception e) {
                return false;
            }
            return false;
        }, 30000, 200L, () -> "The 4th consumer was not kicked out of the group");

        shareConsumer1.close();
        shareConsumer2.close();
        shareConsumer3.close();
        shareConsumer4.close();
    }

    @ClusterTest(
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.max.size", value = "1"), // Setting max group size to 1
            @ClusterConfigProperty(key = "group.share.max.share.sessions", value = "1") // Setting max share sessions value to 1
        }
    )
    public void testShareGroupShareSessionCacheIsFull() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group2")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer1.subscribe(Set.of(tp.topic()));
            shareConsumer2.subscribe(Set.of(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer1, 2500L, 1);
            assertEquals(1, records.count());

            producer.send(record);
            producer.flush();

            // The second share consumer should not throw any exception, but should not receive any records as well.
            records = shareConsumer2.poll(Duration.ofMillis(1000));

            assertEquals(0, records.count());

            shareConsumer1.close();
            shareConsumer2.close();
        }
    }

    @ClusterTest
    public void testCommitSyncFailsForDeletedTopic() throws InterruptedException {
        Uuid topicId = createTopic("baz", 1, 1);
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("baz", 0, null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of("baz"));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            records.forEach(shareConsumer::acknowledge);

            // Topic deletion does not necessarily become apparent across the cluster immediately, so sleep a short while
            deleteTopic("baz");
            Thread.sleep(5000);

            Map<TopicIdPartition, Optional<KafkaException>> commitResult = shareConsumer.commitSync();
            assertEquals(1, commitResult.size());
            assertInstanceOf(UnknownTopicIdException.class, commitResult.get(new TopicIdPartition(topicId, 0, "baz")).get());
        }
    }

    @ClusterTest
    public void testDescribeShareGroupOffsetsForEmptySharePartition() {
        String groupId = "group1";
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created.
            shareConsumer.poll(Duration.ofMillis(2000));
            SharePartitionOffsetInfo sharePartitionOffsetInfo = sharePartitionOffsetInfo(adminClient, groupId, tp);
            // Since the partition is empty, and no records have been consumed, the share partition startOffset will be
            // -1. Thus, there will be no description for the share partition.
            assertNull(sharePartitionOffsetInfo);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testAddedPartitionsOfKnownTopicStartAtOffsetZero() throws InterruptedException, ExecutionException {
        String groupId = "expand-group";
        String topic = "expand-topic";
        createTopic(topic, 1, 1);

        alterShareAutoOffsetReset(groupId, "latest");

        try (Producer<byte[], byte[]> producer = createProducer();
             Admin admin = createAdminClient();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId)) {

            // Produce 10 records to partition 0 before anyone subscribes.
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, ("p0-key-" + i).getBytes(), ("p0-value-" + i).getBytes()));
            }
            producer.flush();

            // The consumer joins and polls: with "latest" it starts partition 0 at the log end, so it receives
            // none of the pre-existing records. This also makes the group aware of (initializes) the topic.
            shareConsumer.subscribe(Set.of(topic));
            int received = 0;
            long deadline = System.currentTimeMillis() + 10000L;
            while (System.currentTimeMillis() < deadline) {
                received += shareConsumer.poll(Duration.ofMillis(1000)).count();
            }
            assertEquals(0, received, "latest reset should skip records produced before the topic was known");

            // Expand the topic and produce 10 records to the brand-new partition 1, each with a distinct,
            // known value so we can assert all of them are delivered (none skipped by the latest reset).
            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get();
            cluster.waitTopicCreation(topic, 2);

            List<String> expectedValues = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                String value = "p1-value-" + i;
                expectedValues.add(value);
                producer.send(new ProducerRecord<>(topic, 1, null, ("p1-key-" + i).getBytes(), value.getBytes()));
            }
            producer.flush();

            // Partition 1 belongs to an already-known topic, so it must be initialized at offset 0 and deliver
            // every record produced above — even though it was created under a "latest" reset. Share consumers
            // deliver at-least-once, so records may be redelivered; track distinct values in a set so
            // duplicates don't cause spurious failures, then assert the count of distinct records.
            Set<String> receivedValues = new HashSet<>();
            waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.partition() == 1) {
                        receivedValues.add(new String(record.value()));
                    }
                }
                return receivedValues.size() >= expectedValues.size();
            }, 30000L, 500L, () -> "did not receive all records from the newly added partition");
            assertEquals(expectedValues.size(), receivedValues.size());
        }
    }
}
