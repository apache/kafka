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
package kafka.test.api;

import kafka.api.BaseConsumerTest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Flaky;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(1200)
@Tag("integration")
@ClusterTestDefaults(
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic,consumer,share"),
        @ClusterConfigProperty(key = "group.share.enable", value = "true"),
        @ClusterConfigProperty(key = "group.share.partition.max.record.locks", value = "10000"),
        @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "15000"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "3"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "unstable.api.versions.enable", value = "true")
    },
    types = {Type.KRAFT}
)
public class ShareConsumerTest {
    private final ClusterInstance cluster;
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private final TopicPartition tp2 = new TopicPartition("topic2", 0);
    private final TopicPartition warmupTp = new TopicPartition("warmup", 0);
    private List<TopicPartition> sgsTopicPartitions;

    public ShareConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private void setup() {
        try {
            this.cluster.waitForReadyBrokers();
            createTopic("topic");
            createTopic("topic2");
            sgsTopicPartitions = IntStream.range(0, 3)
                .mapToObj(part -> new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, part))
                .toList();
            this.warmup();
        } catch (Exception e) {
            fail(e);
        }
    }

    @ClusterTest
    public void testPollNoSubscribeFails() {
        setup();
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            assertEquals(Collections.emptySet(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        }
    }

    @ClusterTest
    public void testSubscribeAndPollNoRecords() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Collections.singleton(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscribePollUnsubscribe() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Collections.singleton(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.unsubscribe();
            assertEquals(Collections.emptySet(), shareConsumer.subscription());
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscribePollSubscribe() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Collections.singleton(tp.topic());
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Collections.singleton(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.unsubscribe();
            assertEquals(Collections.emptySet(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();   // due to leader epoch in read
        }
    }

    @ClusterTest
    public void testSubscribeSubscribeEmptyPollFails() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            Set<String> subscription = Collections.singleton(tp.topic());
            shareConsumer.subscribe(subscription);
            assertEquals(subscription, shareConsumer.subscription());
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
            shareConsumer.subscribe(Collections.emptySet());
            assertEquals(Collections.emptySet(), shareConsumer.subscription());
            // "Consumer is not subscribed to any topics."
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
            assertEquals(0, records.count());
            verifyShareGroupStateTopicRecordsProduced();   // due to leader epoch in read
        }
    }

    @ClusterTest
    public void testSubscriptionAndPoll() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscriptionAndPollMultiple() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
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
    public void testAcknowledgementSentOnSubscriptionChange() throws ExecutionException, InterruptedException {
        setup();
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

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            shareConsumer.subscribe(Collections.singletonList(tp2.topic()));

            // Waiting for heartbeat to propagate the subscription change.
            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(500));
                return partitionExceptionMap.containsKey(tp) && partitionExceptionMap.containsKey(tp2);
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records from the updated subscription");

            // Verifying if the callback was invoked without exceptions for the partitions for both topics.
            assertNull(partitionExceptionMap.get(tp));
            assertNull(partitionExceptionMap.get(tp2));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackSuccessfulAcknowledgement() throws Exception {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(500));
                return partitionExceptionMap.containsKey(tp);
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to receive call to callback");

            // We expect null exception as the acknowledgment error code is null.
            assertNull(partitionExceptionMap.get(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testAcknowledgementCommitCallbackOnClose() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());

            // Now in the second poll, we implicitly acknowledge the record received in the first poll.
            // We get back the acknowledgement error code asynchronously after the second poll.
            // The acknowledgement commit callback is invoked in close.
            shareConsumer.poll(Duration.ofMillis(1000));
            shareConsumer.close();

            // We expect null exception as the acknowledgment error code is null.
            assertTrue(partitionExceptionMap.containsKey(tp));
            assertNull(partitionExceptionMap.get(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @Flaky("KAFKA-18033")
    @ClusterTest
    public void testAcknowledgementCommitCallbackInvalidRecordStateException() throws Exception {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());

            // Waiting until the acquisition lock expires.
            Thread.sleep(20000);

            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(500));
                return partitionExceptionMap.containsKey(tp) && partitionExceptionMap.get(tp) instanceof InvalidRecordStateException;
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to be notified by InvalidRecordStateException");
        }
    }

    private static class TestableAcknowledgementCommitCallback implements AcknowledgementCommitCallback {
        private final Map<TopicPartition, Set<Long>> partitionOffsetsMap;
        private final Map<TopicPartition, Exception> partitionExceptionMap;

        public TestableAcknowledgementCommitCallback(Map<TopicPartition, Set<Long>> partitionOffsetsMap,
                                                     Map<TopicPartition, Exception> partitionExceptionMap) {
            this.partitionOffsetsMap = partitionOffsetsMap;
            this.partitionExceptionMap = partitionExceptionMap;
        }

        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            offsetsMap.forEach((partition, offsets) -> {
                partitionOffsetsMap.merge(partition.topicPartition(), offsets, (oldOffsets, newOffsets) -> {
                    Set<Long> mergedOffsets = new HashSet<>();
                    mergedOffsets.addAll(oldOffsets);
                    mergedOffsets.addAll(newOffsets);
                    return mergedOffsets;
                });
                if (!partitionExceptionMap.containsKey(partition.topicPartition())) {
                    partitionExceptionMap.put(partition.topicPartition(), exception);
                }
            });
        }
    }

    @ClusterTest
    public void testHeaders() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            int numRecords = 1;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            record.headers().add("headerKey", "headerValue".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
            assertEquals(numRecords, records.size());

            for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                Header header = consumerRecord.headers().lastHeader("headerKey");
                if (header != null)
                    assertEquals("headerValue", new String(header.value()));
            }
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

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
            assertEquals(numRecords, records.size());
        }
    }

    @ClusterTest
    public void testHeadersSerializerDeserializer() {
        setup();
        testHeadersSerializeDeserialize(new BaseConsumerTest.SerializerImpl(), new BaseConsumerTest.DeserializerImpl());
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testMaxPollRecords() {
        setup();
        int numRecords = 10000;
        int maxPollRecords = 2;

        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1",
            Collections.singletonMap(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)))) {

            long startingTimestamp = System.currentTimeMillis();
            produceMessagesWithTimestamp(numRecords, startingTimestamp);

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

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
        setup();
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

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer1.subscribe(Collections.singleton(tp.topic()));
            shareConsumer2.subscribe(Collections.singleton(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();
            shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

            ConsumerRecords<byte[], byte[]> records = shareConsumer1.poll(Duration.ofMillis(5000));
            assertEquals(3, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            // Acknowledging 2 out of the 3 records received via commitAsync.
            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> secondRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            assertEquals(1L, secondRecord.offset());

            shareConsumer1.acknowledge(firstRecord);
            shareConsumer1.acknowledge(secondRecord);
            shareConsumer1.commitAsync();

            // The 3rd record should be reassigned to 2nd consumer when it polls, kept higher wait time
            // as time out for locks is 15 secs.
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(1000));
                return records2.count() == 1 && records2.iterator().next().offset() == 2L;
            }, 30000, 100L, () -> "Didn't receive timed out record");

            assertFalse(partitionExceptionMap1.containsKey(tp));

            // The callback will receive the acknowledgement responses asynchronously after the next poll.
            TestUtils.waitForCondition(() -> {
                shareConsumer1.poll(Duration.ofMillis(1000));
                return partitionExceptionMap1.containsKey(tp);
            }, 30000, 100L, () -> "Didn't receive call to callback");

            assertNull(partitionExceptionMap1.get(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgementCommitAsyncPartialBatch() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.flush();

            shareConsumer1.subscribe(Collections.singleton(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
            shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, partitionExceptionMap));

            ConsumerRecords<byte[], byte[]> records = shareConsumer1.poll(Duration.ofMillis(5000));
            assertEquals(3, records.count());
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();

            // Acknowledging 2 out of the 3 records received via commitAsync.
            ConsumerRecord<byte[], byte[]> firstRecord = iterator.next();
            ConsumerRecord<byte[], byte[]> secondRecord = iterator.next();
            assertEquals(0L, firstRecord.offset());
            assertEquals(1L, secondRecord.offset());

            shareConsumer1.acknowledge(firstRecord);
            shareConsumer1.acknowledge(secondRecord);
            shareConsumer1.commitAsync();

            // The 3rd record should be re-presented to the consumer when it polls again.
            records = shareConsumer1.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            iterator = records.iterator();
            firstRecord = iterator.next();
            assertEquals(2L, firstRecord.offset());

            // And poll again without acknowledging - the callback will receive the acknowledgement responses too
            records = shareConsumer1.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            iterator = records.iterator();
            firstRecord = iterator.next();
            assertEquals(2L, firstRecord.offset());

            shareConsumer1.acknowledge(firstRecord);

            // The callback will receive the acknowledgement responses after polling. The callback is
            // called on entry to the poll method or during close. The commit is being performed asynchronously, so
            // we can only rely on the completion once the consumer has closed because that waits for the response.
            shareConsumer1.poll(Duration.ofMillis(500));

            shareConsumer1.close();

            assertTrue(partitionExceptionMap.containsKey(tp));
            assertNull(partitionExceptionMap.get(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleasePollAccept() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeThrowsNotInBatch() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
    public void testImplicitAcknowledgeFailsExplicit() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
            records = shareConsumer.poll(Duration.ofMillis(500));
            assertEquals(0, records.count());
            assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testImplicitAcknowledgeCommitSync() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
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
        setup();
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

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
            Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(3, records.count());

            // Implicitly acknowledging all the records received.
            shareConsumer.commitAsync();

            assertFalse(partitionExceptionMap1.containsKey(tp));
            // The callback will receive the acknowledgement responses after the next poll.
            TestUtils.waitForCondition(() -> {
                shareConsumer.poll(Duration.ofMillis(1000));
                return partitionExceptionMap1.containsKey(tp);
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Acknowledgement commit callback did not receive the response yet");

            assertNull(partitionExceptionMap1.get(tp));
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testFetchRecordLargerThanMaxPartitionFetchBytes() throws Exception {
        setup();
        int maxPartitionFetchBytes = 10000;

        alterShareAutoOffsetReset("group1", "earliest");
        try (
            Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Collections.singletonMap(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionFetchBytes))
            )
        ) {

            ProducerRecord<byte[], byte[]> smallRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            ProducerRecord<byte[], byte[]> bigRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), new byte[maxPartitionFetchBytes]);
            producer.send(smallRecord).get();
            producer.send(bigRecord).get();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(2, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testMultipleConsumersWithDifferentGroupIds() throws InterruptedException {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareAutoOffsetReset("group2", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group2")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

            shareConsumer1.subscribe(Collections.singleton(tp.topic()));

            shareConsumer2.subscribe(Collections.singleton(tp.topic()));

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
    public void testMultipleConsumersInGroupSequentialConsumption() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer1.subscribe(Collections.singleton(tp.topic()));
            shareConsumer2.subscribe(Collections.singleton(tp.topic()));

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

    @Flaky("KAFKA-18033")
    @ClusterTest
    public void testMultipleConsumersInGroupConcurrentConsumption()
            throws InterruptedException, ExecutionException, TimeoutException {
        setup();
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

    @ClusterTest
    public void testMultipleConsumersInMultipleGroupsConcurrentConsumption()
            throws ExecutionException, InterruptedException, TimeoutException {
        setup();
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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            shareConsumer1.subscribe(Collections.singleton(tp.topic()));
            shareConsumer2.subscribe(Collections.singleton(tp.topic()));

            int totalMessages = 1500;
            for (int i = 0; i < totalMessages; i++) {
                producer.send(record);
            }
            producer.close();

            int consumer1MessageCount = 0;
            int consumer2MessageCount = 0;

            // Poll three times to receive records. The second poll acknowledges the records
            // from the first poll, and so on. The third poll's records are not acknowledged
            // because the consumer is closed, which makes the broker release the records fetched.
            ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(5000));
            consumer1MessageCount += records1.count();
            int consumer1MessageCountA = records1.count();
            records1 = shareConsumer1.poll(Duration.ofMillis(5000));
            consumer1MessageCount += records1.count();
            int consumer1MessageCountB = records1.count();
            records1 = shareConsumer1.poll(Duration.ofMillis(5000));
            int consumer1MessageCountC = records1.count();
            assertEquals(totalMessages, consumer1MessageCountA + consumer1MessageCountB + consumer1MessageCountC);
            shareConsumer1.close();

            int maxRetries = 10;
            int retries = 0;
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

    @ClusterTest
    public void testMultipleConsumersInGroupFailureConcurrentConsumption()
            throws InterruptedException, ExecutionException, TimeoutException {
        setup();
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

    @ClusterTest
    public void testAcquisitionLockTimeoutOnConsumer() throws InterruptedException {
        setup();
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
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(5000));
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
            Thread.sleep(20000);

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
     * Test to verify that the acknowledgement commit callback cannot invoke methods of ShareConsumer.
     * The exception thrown is verified in {@link TestableAcknowledgementCommitCallbackWithShareConsumer}
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackCallsShareConsumerDisallowed() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackWithShareConsumer<>(shareConsumer));
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            // The acknowledgment commit callback will try to call a method of ShareConsumer
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
            assertThrows(IllegalStateException.class, () -> shareConsumer.subscribe(Collections.singleton(tp.topic())));
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(5000)));
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback can invoke ShareConsumer.wakeup() and it
     * wakes up the enclosing poll.
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackCallsShareConsumerWakeup() throws InterruptedException {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            // The acknowledgment commit callback will try to call a method of ShareConsumer
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackWakeup<>(shareConsumer));
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            // The second poll sends the acknowledgments implicitly.
            shareConsumer.poll(Duration.ofMillis(2000));

            // Till now acknowledgement commit callback has not been called, so no exception thrown yet.
            // On 3rd poll, the acknowledgement commit callback will be called and the exception is thrown.
            AtomicBoolean exceptionThrown = new AtomicBoolean(false);
            TestUtils.waitForCondition(() -> {
                try {
                    shareConsumer.poll(Duration.ofMillis(500));
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    exceptionThrown.set(true);
                }
                return exceptionThrown.get();
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to receive expected exception");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    private static class TestableAcknowledgementCommitCallbackWakeup<K, V> implements AcknowledgementCommitCallback {
        private final ShareConsumer<K, V> shareConsumer;

        TestableAcknowledgementCommitCallbackWakeup(ShareConsumer<K, V> shareConsumer) {
            this.shareConsumer = shareConsumer;
        }

        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            shareConsumer.wakeup();
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback can throw an exception, and it is propagated
     * to the caller of poll().
     */
    @ClusterTest
    public void testAcknowledgementCommitCallbackThrowsException() throws InterruptedException {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackThrows<>());
            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer");

            AtomicBoolean exceptionThrown = new AtomicBoolean(false);
            TestUtils.waitForCondition(() -> {
                try {
                    shareConsumer.poll(Duration.ofMillis(500));
                } catch (org.apache.kafka.common.errors.OutOfOrderSequenceException e) {
                    exceptionThrown.set(true);
                }
                return exceptionThrown.get();
            }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to receive expected exception");
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    private static class TestableAcknowledgementCommitCallbackThrows<K, V> implements AcknowledgementCommitCallback {
        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            throw new org.apache.kafka.common.errors.OutOfOrderSequenceException("Exception thrown in TestableAcknowledgementCommitCallbackThrows.onComplete");
        }
    }

    /**
     * Test to verify that calling Thread.interrupt() before ShareConsumer.poll(Duration)
     * causes it to throw InterruptException
     */
    @ClusterTest
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            shareConsumer.subscribe(Collections.singleton("topic abc"));

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
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            shareConsumer.wakeup();
            assertThrows(WakeupException.class, () -> shareConsumer.poll(Duration.ZERO));

            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testSubscriptionFollowedByTopicCreation() throws InterruptedException {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {

            String topic = "foo";
            shareConsumer.subscribe(Collections.singleton(topic));

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

    @ClusterTest
    public void testSubscriptionAndPollFollowedByTopicDeletion() throws InterruptedException, ExecutionException {
        setup();
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
        setup();
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
            adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(5L)));

            int messageCount = consumeMessages(new AtomicInteger(0), 5, groupId, 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, messageCount);

            // We write 5 records to the topic, so they would be written from offsets 10-14 on the topic.
            for (int i = 0; i < 5; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 14, so the LSO should move to 14.
            adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(14L)));

            int consumeMessagesCount = consumeMessages(new AtomicInteger(0), 1, groupId, 1, 10, true);
            // The record returned belong to offset 14.
            assertEquals(1, consumeMessagesCount);

            // We delete records before offset 15, so the LSO should move to 15 and now no records should be returned.
            adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(15L)));

            messageCount = consumeMessages(new AtomicInteger(0), 0, groupId, 1, 5, true);
            assertEquals(0, messageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetDefaultValue() {
        setup();
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            // No records should be consumed because share.auto.offset.reset has a default of "latest". Since the record
            // was produced before share partition was initialized (which happens after the first share fetch request
            // in the poll method), the start offset would be the latest offset, i.e. 1 (the next offset after the already
            // present 0th record)
            assertEquals(0, records.count());
            // Producing another record.
            producer.send(record);
            producer.flush();
            records = shareConsumer.poll(Duration.ofMillis(5000));
            // Now the next record should be consumed successfully
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetEarliest() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
            // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
            // all messages present on the partition
            assertEquals(1, records.count());
            // Producing another record.
            producer.send(record);
            producer.flush();
            records = shareConsumer.poll(Duration.ofMillis(5000));
            // The next records should also be consumed successfully
            assertEquals(1, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetEarliestAfterLsoMovement() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        try (
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
            Producer<byte[], byte[]> producer = createProducer();
            Admin adminClient = createAdminClient()
        ) {

            shareConsumer.subscribe(Collections.singleton(tp.topic()));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
            for (int i = 0; i < 10; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 5, so the LSO should move to 5.
            adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(5L)));

            int consumedMessageCount = consumeMessages(new AtomicInteger(0), 5, "group1", 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, consumedMessageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetMultipleGroupsWithDifferentValue() {
        setup();
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareAutoOffsetReset("group2", "latest");
        try (ShareConsumer<byte[], byte[]> shareConsumerEarliest = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumerLatest = createShareConsumer("group2");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumerEarliest.subscribe(Collections.singleton(tp.topic()));

            shareConsumerLatest.subscribe(Collections.singleton(tp.topic()));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records1 = shareConsumerEarliest.poll(Duration.ofMillis(5000));
            // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
            // all messages present on the partition
            assertEquals(1, records1.count());

            ConsumerRecords<byte[], byte[]> records2 = shareConsumerLatest.poll(Duration.ofMillis(5000));
            // Since the value for share.auto.offset.reset has been altered to "latest", the consumer should not consume
            // any message
            assertEquals(0, records2.count());

            // Producing another record.
            producer.send(record);

            records1 = shareConsumerEarliest.poll(Duration.ofMillis(5000));
            // The next record should also be consumed successfully by group1
            assertEquals(1, records1.count());

            records2 = shareConsumerLatest.poll(Duration.ofMillis(5000));
            // The next record should also be consumed successfully by group2
            assertEquals(1, records2.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetByDuration() throws Exception {
        setup();
        // Set auto offset reset to 1 hour before current time
        alterShareAutoOffsetReset("group1", "by_duration:PT1H");
        
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            long currentTime = System.currentTimeMillis();
            long twoHoursAgo = currentTime - TimeUnit.HOURS.toMillis(2);
            long thirtyMinsAgo = currentTime - TimeUnit.MINUTES.toMillis(30);
            
            // Produce messages with different timestamps
            ProducerRecord<byte[], byte[]> oldRecord = new ProducerRecord<>(tp.topic(), tp.partition(), 
                twoHoursAgo, "old_key".getBytes(), "old_value".getBytes());
            ProducerRecord<byte[], byte[]> recentRecord = new ProducerRecord<>(tp.topic(), tp.partition(),
                thirtyMinsAgo, "recent_key".getBytes(), "recent_value".getBytes());
            ProducerRecord<byte[], byte[]> currentRecord = new ProducerRecord<>(tp.topic(), tp.partition(),
                currentTime, "current_key".getBytes(), "current_value".getBytes());

            producer.send(oldRecord).get();
            producer.send(recentRecord).get();
            producer.send(currentRecord).get();
            producer.flush();

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            
            // Should only receive messages from last hour (recent and current)
            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, 2);
            assertEquals(2, records.size());
            
            // Verify timestamps and order
            assertEquals(thirtyMinsAgo, records.get(0).timestamp());
            assertEquals("recent_key", new String(records.get(0).key()));
            assertEquals(currentTime, records.get(1).timestamp());
            assertEquals("current_key", new String(records.get(1).key()));
        }

        // Set the auto offset reset to 3 hours before current time
        // so the consumer should consume all messages (3 records)
        alterShareAutoOffsetReset("group2", "by_duration:PT3H");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group2");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Collections.singleton(tp.topic()));
            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, 3);
            assertEquals(3, records.size());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetByDurationInvalidFormat() throws Exception {
        setup();
        // Test invalid duration format
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, "group1");
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        
        // Test invalid duration format
        try (Admin adminClient = createAdminClient()) {
            alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:1h"), AlterConfigOp.OpType.SET)));
            ExecutionException e1 = assertThrows(ExecutionException.class, () ->
                adminClient.incrementalAlterConfigs(alterEntries).all().get());
            assertInstanceOf(InvalidConfigurationException.class, e1.getCause());

            // Test negative duration
            alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
                GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "by_duration:-PT1H"), AlterConfigOp.OpType.SET)));
            ExecutionException e2 = assertThrows(ExecutionException.class, () ->
                adminClient.incrementalAlterConfigs(alterEntries).all().get());
            assertInstanceOf(InvalidConfigurationException.class, e2.getCause());
        }
    }

    private int produceMessages(int messageCount) {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            IntStream.range(0, messageCount).forEach(__ -> producer.send(record));
            producer.flush();
        }
        return messageCount;
    }

    private void produceMessagesWithTimestamp(int messageCount, long startingTimestamp) {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), startingTimestamp + i,
                    ("key " + i).getBytes(), ("value " + i).getBytes());
                producer.send(record);
            }
            producer.flush();
        }
    }

    private int consumeMessages(AtomicInteger totalMessagesConsumed,
                                 int totalMessages,
                                 String groupId,
                                 int consumerNumber,
                                 int maxPolls,
                                 boolean commit) {
        return assertDoesNotThrow(() -> {
            try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                    groupId)) {
                shareConsumer.subscribe(Collections.singleton(tp.topic()));
                return consumeMessages(shareConsumer, totalMessagesConsumed, totalMessages, consumerNumber, maxPolls, commit);
            }
        }, "Consumer " + consumerNumber + " failed with exception");
    }

    private int consumeMessages(AtomicInteger totalMessagesConsumed,
                                 int totalMessages,
                                 String groupId,
                                 int consumerNumber,
                                 int maxPolls,
                                 boolean commit,
                                 int maxFetchBytes) {
        return assertDoesNotThrow(() -> {
            try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                    groupId,
                    Map.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes))) {
                shareConsumer.subscribe(Collections.singleton(tp.topic()));
                return consumeMessages(shareConsumer, totalMessagesConsumed, totalMessages, consumerNumber, maxPolls, commit);
            }
        }, "Consumer " + consumerNumber + " failed with exception");
    }

    private int consumeMessages(ShareConsumer<byte[], byte[]> consumer,
                                 AtomicInteger totalMessagesConsumed,
                                 int totalMessages,
                                 int consumerNumber,
                                 int maxPolls,
                                 boolean commit) {
        return assertDoesNotThrow(() -> {
            int messagesConsumed = 0;
            int retries = 0;
            if (totalMessages > 0) {
                while (totalMessagesConsumed.get() < totalMessages && retries < maxPolls) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(2000));
                    messagesConsumed += records.count();
                    totalMessagesConsumed.addAndGet(records.count());
                    retries++;
                }
            } else {
                while (retries < maxPolls) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(2000));
                    messagesConsumed += records.count();
                    totalMessagesConsumed.addAndGet(records.count());
                    retries++;
                }
            }

            if (commit) {
                // Complete acknowledgement of the records
                consumer.commitSync(Duration.ofMillis(10000));
            }
            return messagesConsumed;
        }, "Consumer " + consumerNumber + " failed with exception");
    }

    private <K, V> List<ConsumerRecord<K, V>> consumeRecords(ShareConsumer<K, V> consumer,
                                                             int numRecords) {
        ArrayList<ConsumerRecord<K, V>> accumulatedRecords = new ArrayList<>();
        long startTimeMs = System.currentTimeMillis();
        while (accumulatedRecords.size() < numRecords) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(accumulatedRecords::add);
            long currentTimeMs = System.currentTimeMillis();
            assertFalse(currentTimeMs - startTimeMs > 60000, "Timed out before consuming expected records.");
        }
        return accumulatedRecords;
    }

    private void createTopic(String topicName) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                admin.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short) 1))).all().get();
            }
        }, "Failed to create topic");
    }

    private void deleteTopic(String topicName) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                admin.deleteTopics(Collections.singleton(topicName)).all().get();
            }
        }, "Failed to delete topic");
    }

    private Admin createAdminClient() {
        return cluster.admin();
    }

    private <K, V> Producer<K, V> createProducer() {
        return createProducer(Map.of());
    }

    private <K, V> Producer<K, V> createProducer(Map<String, Object> config) {
        return cluster.producer(config);
    }

    private <K, V> Producer<K, V> createProducer(String transactionalId) {
        return createProducer(
            Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId
            )
        );
    }

    private <K, V> ShareConsumer<K, V> createShareConsumer(String groupId) {
        return createShareConsumer(groupId, Map.of());
    }

    private <K, V> ShareConsumer<K, V> createShareConsumer(
        String groupId,
        Map<?, ?> additionalProperties
    ) {
        Properties props = new Properties();
        props.putAll(additionalProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Map<String, Object> conf = new HashMap<>();
        props.forEach((k, v) -> conf.put((String) k, v));
        return cluster.shareConsumer(conf);
    }

    private void warmup() throws InterruptedException {
        createTopic(warmupTp.topic());
        waitForMetadataCache();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(warmupTp.topic(), warmupTp.partition(), null, "key".getBytes(), "value".getBytes());
        Set<String> subscription = Collections.singleton(warmupTp.topic());
        alterShareAutoOffsetReset("warmupgroup1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("warmupgroup1")) {

            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(subscription);
            TestUtils.waitForCondition(
                () -> shareConsumer.poll(Duration.ofMillis(5000)).count() == 1, 30000, 200L, () -> "warmup record not received");
        }
    }

    private void waitForMetadataCache() throws InterruptedException {
        TestUtils.waitForCondition(() ->
                !cluster.brokers().get(0).metadataCache().getAliveBrokerNodes(new ListenerName("EXTERNAL")).isEmpty(),
            DEFAULT_MAX_WAIT_MS, 100L, () -> "cache not up yet");
    }

    private void verifyShareGroupStateTopicRecordsProduced() {
        try {
            try (Consumer<byte[], byte[]> consumer = cluster.consumer()) {
                consumer.assign(sgsTopicPartitions);
                consumer.seekToBeginning(sgsTopicPartitions);
                Set<ConsumerRecord<byte[], byte[]>> records = new HashSet<>();
                TestUtils.waitForCondition(() -> {
                        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(Duration.ofMillis(5000L));
                        if (msgs.count() > 0) {
                            msgs.records(Topic.SHARE_GROUP_STATE_TOPIC_NAME).forEach(records::add);
                        }
                        return records.size() > 2; // +2 because of extra warmup records
                    },
                    30000L,
                    200L,
                    () -> "no records produced"
                );
            }
        } catch (InterruptedException e) {
            fail(e);
        }
    }

    private void alterShareAutoOffsetReset(String groupId, String newValue) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
            GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, newValue), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions();
        try (Admin adminClient = createAdminClient()) {
            assertDoesNotThrow(() -> adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                .all()
                .get(60, TimeUnit.SECONDS), "Failed to alter configs");
        }
    }
}
