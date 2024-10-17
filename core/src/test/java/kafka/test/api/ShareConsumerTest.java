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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

@Timeout(600)
@Tag("integration")
public class ShareConsumerTest {
    private KafkaClusterTestKit cluster;
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private final TopicPartition warmupTp = new TopicPartition("warmup", 0);

    @BeforeEach
    public void createCluster() throws Exception {
        cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                    .setNumBrokerNodes(1)
                    .setNumControllerNodes(1)
                    .build())
            .setConfigProp("auto.create.topics.enable", "false")
            .setConfigProp("group.coordinator.rebalance.protocols", "classic,consumer,share")
            .setConfigProp("group.share.enable", "true")
            .setConfigProp("group.share.partition.max.record.locks", "10000")
            .setConfigProp("group.share.persister.class.name", "org.apache.kafka.server.group.share.NoOpShareStatePersister")
            .setConfigProp("group.share.record.lock.duration.ms", "15000")
            .setConfigProp("offsets.topic.replication.factor", "1")
            .setConfigProp("share.coordinator.state.topic.min.isr", "1")
            .setConfigProp("share.coordinator.state.topic.replication.factor", "1")
            .setConfigProp("transaction.state.log.min.isr", "1")
            .setConfigProp("transaction.state.log.replication.factor", "1")
            .setConfigProp("unstable.api.versions.enable", "true")
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForActiveController();
        cluster.waitForReadyBrokers();
        createTopic("topic");
        warmup();
    }

    @AfterEach
    public void destroyCluster() throws Exception {
        cluster.close();
    }

    @Test
    public void testPollNoSubscribeFails() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        shareConsumer.close();
    }

    @Test
    public void testSubscribeAndPollNoRecords() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        Set<String> subscription = Collections.singleton(tp.topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @Test
    public void testSubscribePollUnsubscribe() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        Set<String> subscription = Collections.singleton(tp.topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.unsubscribe();
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @Test
    public void testSubscribePollSubscribe() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        Set<String> subscription = Collections.singleton(tp.topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        records = shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @Test
    public void testSubscribeUnsubscribePollFails() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        Set<String> subscription = Collections.singleton(tp.topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.unsubscribe();
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @Test
    public void testSubscribeSubscribeEmptyPollFails() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        Set<String> subscription = Collections.singleton(tp.topic());
        shareConsumer.subscribe(subscription);
        assertEquals(subscription, shareConsumer.subscription());
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.subscribe(Collections.emptySet());
        assertEquals(Collections.emptySet(), shareConsumer.subscription());
        // "Consumer is not subscribed to any topics."
        assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        shareConsumer.close();
        assertEquals(0, records.count());
    }

    @Test
    public void testSubscriptionAndPoll() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testSubscriptionAndPollMultiple() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testAcknowledgementCommitCallbackSuccessfulAcknowledgement() {
        Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap, partitionExceptionMap));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        // Now in the second poll, we implicitly acknowledge the record received in the first poll.
        // We get back the acknowledgment error code after the second poll.
        // When we start the 3rd poll, the acknowledgment commit callback is invoked.
        shareConsumer.poll(Duration.ofMillis(1000));
        shareConsumer.poll(Duration.ofMillis(1000));

        // We expect null exception as the acknowledgment error code is null.
        assertTrue(partitionExceptionMap.containsKey(tp));
        assertNull(partitionExceptionMap.get(tp));
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testAcknowledgementCommitCallbackOnClose() {
        Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap, partitionExceptionMap));
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
        producer.close();
    }

    @Test
    public void testAcknowledgementCommitCallbackInvalidRecordStateException() throws Exception {
        Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap, partitionExceptionMap));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());

        // Waiting until the acquisition lock expires.
        Thread.sleep(20000);

        // Now in the second poll, we implicitly acknowledge the record received in the first poll.
        // We get back the acknowledgment error code after the second poll.
        // When we start the 3rd poll, the acknowledgment commit callback is invoked.
        records = shareConsumer.poll(Duration.ofMillis(200));
        assertEquals(1, records.count());

        records = shareConsumer.poll(Duration.ofMillis(200));
        assertEquals(0, records.count());

        // As we tried to acknowledge a record after the acquisition lock expired,
        // we wil get an InvalidRecordStateException.
        assertInstanceOf(InvalidRecordStateException.class, partitionExceptionMap.get(tp));
        shareConsumer.close();
        producer.close();
    }

    private static class TestableAcknowledgeCommitCallback implements AcknowledgementCommitCallback {
        private final Map<TopicPartition, Set<Long>> partitionOffsetsMap;
        private final Map<TopicPartition, Exception> partitionExceptionMap;

        public TestableAcknowledgeCommitCallback(Map<TopicPartition, Set<Long>> partitionOffsetsMap,
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

    @Test
    public void testHeaders() {
        int numRecords = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        record.headers().add("headerKey", "headerValue".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
        assertEquals(numRecords, records.size());

        for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
            Header header = consumerRecord.headers().lastHeader("headerKey");
            if (header != null)
                assertEquals("headerValue", new String(header.value()));
        }
        shareConsumer.close();
        producer.close();
    }

    private void testHeadersSerializeDeserialize(Serializer<byte[]> serializer, Deserializer<byte[]> deserializer) {
        int numRecords = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), serializer);
        producer.send(record);

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(deserializer, new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, numRecords);
        assertEquals(numRecords, records.size());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testHeadersSerializerDeserializer() {
        testHeadersSerializeDeserialize(new BaseConsumerTest.SerializerImpl(), new BaseConsumerTest.DeserializerImpl());
    }

    @Test
    public void testMaxPollRecords() {
        int maxPollRecords = 2;
        int numRecords = 10000;

        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        long startingTimestamp = System.currentTimeMillis();
        produceMessagesWithTimestamp(numRecords, startingTimestamp);

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                "group1", Collections.singletonMap(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords)));
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
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testControlRecordsSkipped() throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());

        KafkaProducer<byte[], byte[]> transactionalProducer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), "T1");
        transactionalProducer.initTransactions();
        transactionalProducer.beginTransaction();
        RecordMetadata transactional1 = transactionalProducer.send(record).get();

        KafkaProducer<byte[], byte[]> nonTransactionalProducer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        RecordMetadata nonTransactional1 = nonTransactionalProducer.send(record).get();

        transactionalProducer.commitTransaction();

        transactionalProducer.beginTransaction();
        RecordMetadata transactional2 = transactionalProducer.send(record).get();
        transactionalProducer.abortTransaction();

        RecordMetadata nonTransactional2 = nonTransactionalProducer.send(record).get();

        transactionalProducer.close();
        nonTransactionalProducer.close();

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
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
        shareConsumer.close();
        transactionalProducer.close();
    }

    @Test
    public void testExplicitAcknowledgeSuccess() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(shareConsumer::acknowledge);
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testExplicitAcknowledgeCommitSuccess() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(shareConsumer::acknowledge);
        producer.send(record);
        Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
        assertEquals(1, result.size());
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testExplicitAcknowledgementCommitAsync() throws InterruptedException {
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record1);
        producer.send(record2);
        producer.send(record3);

        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));
        shareConsumer2.subscribe(Collections.singleton(tp.topic()));

        Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();
        shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

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
            ConsumerRecords<byte[], byte[]> records2 = shareConsumer2.poll(Duration.ofMillis(200));
            return records2.count() == 1 && records2.iterator().next().offset() == 2L;
        }, 30000, 100L, () -> "Didn't receive timed out record");

        assertFalse(partitionExceptionMap1.containsKey(tp));
        // The callback will receive the acknowledgement responses asynchronously after the next poll.
        shareConsumer1.poll(Duration.ofMillis(500));

        shareConsumer1.close();
        shareConsumer2.close();
        producer.close();

        assertTrue(partitionExceptionMap1.containsKey(tp));
        assertNull(partitionExceptionMap1.get(tp));
    }

    @Test
    public void testExplicitAcknowledgementCommitAsyncPartialBatch() {
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record1);
        producer.send(record2);
        producer.send(record3);

        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));

        Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
        shareConsumer1.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap, partitionExceptionMap));

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
        producer.close();

        assertTrue(partitionExceptionMap.containsKey(tp));
        assertNull(partitionExceptionMap.get(tp));
    }

    @Test
    public void testExplicitAcknowledgeReleasePollAccept() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
        records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testExplicitAcknowledgeReleaseAccept() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
        records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testExplicitAcknowledgeReleaseClose() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
        shareConsumer.close();
        producer.close();
    }


    @Test
    public void testExplicitAcknowledgeThrowsNotInBatch() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
        shareConsumer.acknowledge(consumedRecord);
        records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testImplicitAcknowledgeFailsExplicit() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        ConsumerRecord<byte[], byte[]> consumedRecord = records.records(tp).get(0);
        records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(consumedRecord));
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testImplicitAcknowledgeCommitSync() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
        assertEquals(1, result.size());
        result = shareConsumer.commitSync();
        assertEquals(0, result.size());
        records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testImplicitAcknowledgementCommitAsync() throws InterruptedException {
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> record3 = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record1);
        producer.send(record2);
        producer.send(record3);

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        Map<TopicPartition, Set<Long>> partitionOffsetsMap1 = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap1 = new HashMap<>();

        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap1, partitionExceptionMap1));

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

        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testFetchRecordLargerThanMaxPartitionFetchBytes() throws Exception {
        int maxPartitionFetchBytes = 10000;
        ProducerRecord<byte[], byte[]> smallRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> bigRecord = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), new byte[maxPartitionFetchBytes]);
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(smallRecord).get();
        producer.send(bigRecord).get();

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                "group1", Collections.singletonMap(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionFetchBytes)));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testMultipleConsumersWithDifferentGroupIds() throws InterruptedException {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());

        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));

        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group2");
        shareConsumer2.subscribe(Collections.singleton(tp.topic()));

        // producing 3 records to the topic
        producer.send(record);
        producer.send(record);
        producer.send(record);
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

        shareConsumer1.close();
        shareConsumer2.close();
        producer.close();
    }

    @Test
    public void testMultipleConsumersInGroupSequentialConsumption() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer2.subscribe(Collections.singleton(tp.topic()));

        int totalMessages = 2000;
        for (int i = 0; i < totalMessages; i++) {
            producer.send(record);
        }

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
        shareConsumer1.close();
        shareConsumer2.close();
        producer.close();
    }

    @Test
    public void testMultipleConsumersInGroupConcurrentConsumption() {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 4;
        int producerCount = 4;
        int messagesPerProducer = 5000;

        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);
        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(consumerCount);

        for (int i = 0; i < producerCount; i++) {
            producerExecutorService.submit(() -> produceMessages(messagesPerProducer));
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures = new ConcurrentLinkedQueue<>();
        int maxBytes = 100000;
        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            consumerExecutorService.submit(() -> {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                futures.add(future);
                consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer, "group1", consumerNumber, 30, true, future, Optional.of(maxBytes));
            });
        }

        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();

        try {
            assertTrue(producerExecutorService.awaitTermination(60, TimeUnit.SECONDS)); // Wait for all producer threads to complete
            assertTrue(consumerExecutorService.awaitTermination(60, TimeUnit.SECONDS)); // Wait for all consumer threads to complete
            int totalResult = 0;
            for (CompletableFuture<Integer> future : futures) {
                totalResult += future.get();
            }
            assertEquals(producerCount * messagesPerProducer, totalMessagesConsumed.get());
            assertEquals(producerCount * messagesPerProducer, totalResult);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }

    @Test
    public void testMultipleConsumersInMultipleGroupsConcurrentConsumption() {
        AtomicInteger totalMessagesConsumedGroup1 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup2 = new AtomicInteger(0);
        AtomicInteger totalMessagesConsumedGroup3 = new AtomicInteger(0);

        int producerCount = 4;
        int consumerCount = 2;
        int messagesPerProducer = 2000;
        final int totalMessagesSent = producerCount * messagesPerProducer;

        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);
        ExecutorService shareGroupExecutorService1 = Executors.newFixedThreadPool(consumerCount);
        ExecutorService shareGroupExecutorService2 = Executors.newFixedThreadPool(consumerCount);
        ExecutorService shareGroupExecutorService3 = Executors.newFixedThreadPool(consumerCount);

        CountDownLatch startSignal = new CountDownLatch(producerCount);

        ConcurrentLinkedQueue<CompletableFuture<Integer>> producerFutures = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < producerCount; i++) {
            producerExecutorService.submit(() -> {
                CompletableFuture<Integer> future = produceMessages(messagesPerProducer);
                producerFutures.add(future);
                startSignal.countDown();
            });
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures1 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures2 = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Integer>> futures3 = new ConcurrentLinkedQueue<>();

        // Wait for the producers to run
        try {
            boolean signalled = startSignal.await(15, TimeUnit.SECONDS);
            assertTrue(signalled);
        } catch (InterruptedException e) {
            fail("Exception awaiting start signal");
        }

        int maxBytes = 100000;

        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            shareGroupExecutorService1.submit(() -> {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                futures1.add(future);
                consumeMessages(totalMessagesConsumedGroup1, totalMessagesSent, "group1", consumerNumber, 100, true, future, Optional.of(maxBytes));
            });
            shareGroupExecutorService2.submit(() -> {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                futures2.add(future);
                consumeMessages(totalMessagesConsumedGroup2, totalMessagesSent, "group2", consumerNumber, 100, true, future, Optional.of(maxBytes));
            });
            shareGroupExecutorService3.submit(() -> {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                futures3.add(future);
                consumeMessages(totalMessagesConsumedGroup3, totalMessagesSent, "group3", consumerNumber, 100, true, future, Optional.of(maxBytes));
            });
        }
        producerExecutorService.shutdown();
        shareGroupExecutorService1.shutdown();
        shareGroupExecutorService2.shutdown();
        shareGroupExecutorService3.shutdown();
        try {
            shareGroupExecutorService1.awaitTermination(120, TimeUnit.SECONDS); // Wait for all consumer threads for group 1 to complete
            shareGroupExecutorService2.awaitTermination(120, TimeUnit.SECONDS); // Wait for all consumer threads for group 2 to complete
            shareGroupExecutorService3.awaitTermination(120, TimeUnit.SECONDS); // Wait for all consumer threads for group 3 to complete

            int totalResult1 = 0;
            for (CompletableFuture<Integer> future : futures1) {
                totalResult1 += future.get();
            }

            int totalResult2 = 0;
            for (CompletableFuture<Integer> future : futures2) {
                totalResult2 += future.get();
            }

            int totalResult3 = 0;
            for (CompletableFuture<Integer> future : futures3) {
                totalResult3 += future.get();
            }

            assertEquals(totalMessagesSent, totalMessagesConsumedGroup1.get());
            assertEquals(totalMessagesSent, totalMessagesConsumedGroup2.get());
            assertEquals(totalMessagesSent, totalMessagesConsumedGroup3.get());
            assertEquals(totalMessagesSent, totalResult1);
            assertEquals(totalMessagesSent, totalResult2);
            assertEquals(totalMessagesSent, totalResult3);

            int actualMessagesSent = 0;
            try {
                producerExecutorService.awaitTermination(60, TimeUnit.SECONDS); // Wait for all producer threads to complete

                for (CompletableFuture<Integer> future : producerFutures) {
                    actualMessagesSent += future.get();
                }
            } catch (Exception e) {
                fail("Exception occurred : " + e.getMessage());
            }
            assertEquals(totalMessagesSent, actualMessagesSent);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }

    @Test
    public void testConsumerCloseInGroupSequential() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));
        KafkaShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
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
    }

    @Test
    public void testMultipleConsumersInGroupFailureConcurrentConsumption() {
        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);

        int consumerCount = 4;
        int producerCount = 4;
        int messagesPerProducer = 5000;

        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(consumerCount);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(producerCount);

        for (int i = 0; i < producerCount; i++) {
            Runnable task = () -> produceMessages(messagesPerProducer);
            producerExecutorService.submit(task);
        }

        ConcurrentLinkedQueue<CompletableFuture<Integer>> futuresSuccess = new ConcurrentLinkedQueue<>();

        CountDownLatch startSignal = new CountDownLatch(1);

        int maxBytes = 1000000;

        consumerExecutorService.submit(() -> {
            // The "failing" consumer polls but immediately closes, which releases the records for the other consumers
            CompletableFuture<Integer> future = new CompletableFuture<>();
            AtomicInteger failedMessagesConsumed = new AtomicInteger(0);
            consumeMessages(failedMessagesConsumed, producerCount * messagesPerProducer, "group1", 0, 1, false, future);
            startSignal.countDown();
        });

        // Wait for the failed consumer to run
        try {
            boolean signalled = startSignal.await(15, TimeUnit.SECONDS);
            assertTrue(signalled);
        } catch (InterruptedException e) {
            fail("Exception awaiting start signal");
        }

        for (int i = 0; i < consumerCount; i++) {
            final int consumerNumber = i + 1;
            consumerExecutorService.submit(() -> {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                futuresSuccess.add(future);
                consumeMessages(totalMessagesConsumed, producerCount * messagesPerProducer, "group1", consumerNumber, 40, true, future, Optional.of(maxBytes));
            });
        }
        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();
        try {
            producerExecutorService.awaitTermination(60, TimeUnit.SECONDS); // Wait for all producer threads to complete
            consumerExecutorService.awaitTermination(60, TimeUnit.SECONDS); // Wait for all consumer threads to complete
            int totalSuccessResult = 0;
            for (CompletableFuture<Integer> future : futuresSuccess) {
                totalSuccessResult += future.get();
            }
            assertEquals(producerCount * messagesPerProducer, totalMessagesConsumed.get());
            assertEquals(producerCount * messagesPerProducer, totalSuccessResult);
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
    }

    @Test
    public void testAcquisitionLockTimeoutOnConsumer() throws InterruptedException {
        ProducerRecord<byte[], byte[]> producerRecord1 = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key_1".getBytes(), "value_1".getBytes());
        ProducerRecord<byte[], byte[]> producerRecord2 = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key_2".getBytes(), "value_2".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        KafkaShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer1.subscribe(Collections.singleton(tp.topic()));

        producer.send(producerRecord1);

        // Poll two times to receive records. The first poll puts the acquisition lock and fetches the record.
        // Since, we are only sending one record and acquisition lock hasn't timed out, the second poll only acknowledges the
        // record from the first poll and no more fetch.
        ConsumerRecords<byte[], byte[]> records1 = shareConsumer1.poll(Duration.ofMillis(5000));
        assertEquals(1, records1.count());
        assertEquals("key_1", new String(records1.iterator().next().key()));
        assertEquals("value_1", new String(records1.iterator().next().value()));
        ConsumerRecords<byte[], byte[]> records2 = shareConsumer1.poll(Duration.ofMillis(500));
        assertEquals(0, records2.count());

        producer.send(producerRecord2);

        // Poll three times. The first poll puts the acquisition lock and fetches the record. Before the second poll,
        // acquisition lock times out and hence the consumer needs to fetch the record again. Since, the acquisition lock
        // hasn't timed out before the third poll, the third poll only acknowledges the record from the second poll and no more fetch.
        records1 = shareConsumer1.poll(Duration.ofMillis(5000));
        assertEquals(1, records1.count());
        assertEquals("key_2", new String(records1.iterator().next().key()));
        assertEquals("value_2", new String(records1.iterator().next().value()));

        // Allowing acquisition lock to expire.
        Thread.sleep(20000);

        records2 = shareConsumer1.poll(Duration.ofMillis(5000));
        assertEquals(1, records2.count());
        assertEquals("key_2", new String(records2.iterator().next().key()));
        assertEquals("value_2", new String(records2.iterator().next().value()));
        ConsumerRecords<byte[], byte[]> records3 = shareConsumer1.poll(Duration.ofMillis(500));
        assertEquals(0, records3.count());

        producer.close();
        shareConsumer1.close();
    }

    /**
     * Test to verify that the acknowledgement commit callback cannot invoke methods of KafkaShareConsumer.
     * The exception thrown is verified in {@link TestableAcknowledgeCommitCallbackWithShareConsumer}
     */
    @Test
    public void testAcknowledgeCommitCallbackCallsShareConsumerDisallowed() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");

        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallbackWithShareConsumer<>(shareConsumer));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        // The acknowledgment commit callback will try to call a method of KafkaShareConsumer
        shareConsumer.poll(Duration.ofMillis(5000));
        // The second poll sends the acknowledgments implicitly.
        // The acknowledgement commit callback will be called and the exception is thrown.
        // This is verified inside the onComplete() method implementation.
        shareConsumer.poll(Duration.ofMillis(500));
        shareConsumer.close();
        producer.close();
    }

    private class TestableAcknowledgeCommitCallbackWithShareConsumer<K, V> implements AcknowledgementCommitCallback {
        private final KafkaShareConsumer<K, V> shareConsumer;

        TestableAcknowledgeCommitCallbackWithShareConsumer(KafkaShareConsumer<K, V> shareConsumer) {
            this.shareConsumer = shareConsumer;
        }

        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            // Accessing methods of KafkaShareConsumer should throw an exception.
            assertThrows(IllegalStateException.class, shareConsumer::close);
            assertThrows(IllegalStateException.class, () -> shareConsumer.subscribe(Collections.singleton(tp.topic())));
            assertThrows(IllegalStateException.class, () -> shareConsumer.poll(Duration.ofMillis(5000)));
        }
    }

    /**
     * Test to verify that the acknowledgement commit callback can invoke KafkaShareConsumer.wakeup() and it
     * wakes up the enclosing poll.
     */
    @Test
    public void testAcknowledgeCommitCallbackCallsShareConsumerWakeup() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");

        // The acknowledgment commit callback will try to call a method of KafkaShareConsumer
        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallbackWakeup<>(shareConsumer));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        shareConsumer.poll(Duration.ofMillis(5000));
        // The second poll sends the acknowledgments implicitly.
        shareConsumer.poll(Duration.ofMillis(1000));
        // Till now acknowledgement commit callback has not been called, so no exception thrown yet.
        // On 3rd poll, the acknowledgement commit callback will be called and the exception is thrown.
        assertThrows(WakeupException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));
        shareConsumer.close();
        producer.close();
    }

    private static class TestableAcknowledgeCommitCallbackWakeup<K, V> implements AcknowledgementCommitCallback {
        private final KafkaShareConsumer<K, V> shareConsumer;

        TestableAcknowledgeCommitCallbackWakeup(KafkaShareConsumer<K, V> shareConsumer) {
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
    @Test
    public void testAcknowledgeCommitCallbackThrowsException() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");

        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallbackThrows<>());
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        shareConsumer.poll(Duration.ofMillis(5000));

        // The second poll sends the acknowledgments implicitly.
        shareConsumer.poll(Duration.ofMillis(1000));

        // On the third poll, the acknowledgement commit callback will be called and the exception is thrown.
        assertThrows(org.apache.kafka.common.errors.OutOfOrderSequenceException.class, () -> shareConsumer.poll(Duration.ofMillis(500)));

        shareConsumer.close();
        producer.close();
    }

    private static class TestableAcknowledgeCommitCallbackThrows<K, V> implements AcknowledgementCommitCallback {
        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            throw new org.apache.kafka.common.errors.OutOfOrderSequenceException("Exception thrown in TestableAcknowledgeCommitCallbackThrows.onComplete");
        }
    }

    /**
     * Test to verify that calling Thread.interrupt() before KafkaShareConsumer.poll(Duration)
     * causes it to throw InterruptException
     */
    @Test
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        // interrupt the thread and call poll
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptException.class, () -> shareConsumer.poll(Duration.ZERO));
        } finally {
            // clear interrupted state again since this thread may be reused by JUnit
            Thread.interrupted();
        }

        assertDoesNotThrow(() -> shareConsumer.poll(Duration.ZERO));
        shareConsumer.close();
    }

    /**
     * Test to verify that InvalidTopicException is thrown if the consumer subscribes
     * to an invalid topic.
     */
    @Test
    public void testSubscribeOnInvalidTopicThrowsInvalidTopicException() {
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton("topic abc"));

        // The exception depends upon a metadata response which arrives asynchronously. If the delay is
        // too short, the poll might return before the error is known.
        assertThrows(InvalidTopicException.class, () -> shareConsumer.poll(Duration.ofMillis(10000)));
        shareConsumer.close();
    }

    /**
     * Test to ensure that a wakeup when records are buffered doesn't prevent the records
     * being returned on the next poll.
     */
    @Test
    public void testWakeupWithFetchedRecordsAvailable() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        producer.send(record);
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        shareConsumer.subscribe(Collections.singleton(tp.topic()));

        shareConsumer.wakeup();
        assertThrows(WakeupException.class, () -> shareConsumer.poll(Duration.ZERO));

        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());

        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testSubscriptionFollowedByTopicCreation() throws InterruptedException {
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        String topic = "foo";
        shareConsumer.subscribe(Collections.singleton(topic));

        // Topic is created post creation of share consumer and subscription
        createTopic(topic);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, null, "key".getBytes(), "value".getBytes());
        producer.send(record);

        TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
            DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer, metadata sync failed");

        producer.send(record);
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        producer.send(record);
        records = shareConsumer.poll(Duration.ofMillis(5000));
        assertEquals(1, records.count());
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testSubscriptionAndPollFollowedByTopicDeletion() throws InterruptedException, ExecutionException {
        String topic1 = "bar";
        String topic2 = "baz";
        createTopic(topic1);
        createTopic(topic2);

        ProducerRecord<byte[], byte[]> recordTopic1 = new ProducerRecord<>(topic1, 0, null, "key".getBytes(), "value".getBytes());
        ProducerRecord<byte[], byte[]> recordTopic2 = new ProducerRecord<>(topic2, 0, null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());

        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "group1");
        // Consumer subscribes to the topics -> bar and baz.
        shareConsumer.subscribe(Arrays.asList(topic1, topic2));

        producer.send(recordTopic1).get();
        TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

        producer.send(recordTopic2);
        TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

        // Topic bar is deleted, hence poll should not give any results.
        deleteTopic(topic1);
        ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(500));
        assertEquals(0, records.count());

        producer.send(recordTopic2);
        // Poll should give the record from the non-deleted topic baz.
        TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");

        producer.send(recordTopic2);
        TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
                DEFAULT_MAX_WAIT_MS, 100L, () -> "incorrect number of records");
        shareConsumer.close();
        producer.close();
    }

    @Test
    public void testLsoMovementByRecordsDeletion() {
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), 0, null, "key".getBytes(), "value".getBytes());

        // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
        try {
            for (int i = 0; i < 10; i++) {
                producer.send(record).get();
            }
        } catch (Exception e) {
            fail("Failed to send records: " + e);
        }

        Admin adminClient = createAdminClient();
        // We delete records before offset 5, so the LSO should move to 5.
        adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(5L)));

        AtomicInteger totalMessagesConsumed = new AtomicInteger(0);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        consumeMessages(totalMessagesConsumed, 5, "group1", 1, 10, true, future);
        // The records returned belong to offsets 5-9.
        assertEquals(5, totalMessagesConsumed.get());
        try {
            assertEquals(5, future.get());
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }

        // We write 5 records to the topic, so they would be written from offsets 10-14 on the topic.
        try {
            for (int i = 0; i < 5; i++) {
                producer.send(record).get();
            }
        } catch (Exception e) {
            fail("Failed to send records: " + e);
        }

        // We delete records before offset 14, so the LSO should move to 14.
        adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(14L)));

        totalMessagesConsumed = new AtomicInteger(0);
        future = new CompletableFuture<>();
        consumeMessages(totalMessagesConsumed, 1, "group1", 1, 10, true, future);
        // The record returned belong to offset 14.
        assertEquals(1, totalMessagesConsumed.get());
        try {
            assertEquals(1, future.get());
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }

        // We delete records before offset 15, so the LSO should move to 15 and now no records should be returned.
        adminClient.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(15L)));

        totalMessagesConsumed = new AtomicInteger(0);
        future = new CompletableFuture<>();
        consumeMessages(totalMessagesConsumed, 0, "group1", 1, 5, true, future);
        assertEquals(0, totalMessagesConsumed.get());
        try {
            assertEquals(0, future.get());
        } catch (Exception e) {
            fail("Exception occurred : " + e.getMessage());
        }
        adminClient.close();
        producer.close();
    }

    private CompletableFuture<Integer> produceMessages(int messageCount) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        Future<?>[] recordFutures = new Future<?>[messageCount];
        int messagesSent = 0;
        try (KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer())) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < messageCount; i++) {
                recordFutures[i] = producer.send(record);
            }
            for (int i = 0; i < messageCount; i++) {
                try {
                    recordFutures[i].get();
                    messagesSent++;
                } catch (Exception e) {
                    fail("Failed to send record: " + e);
                }
            }
        } finally {
            future.complete(messagesSent);
        }
        return future;
    }

    private void produceMessagesWithTimestamp(int messageCount, long startingTimestamp) {
        try (KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), startingTimestamp + i,
                        ("key " + i).getBytes(), ("value " + i).getBytes());
                producer.send(record);
            }
            producer.flush();
        }
    }

    private void consumeMessages(AtomicInteger totalMessagesConsumed,
                                 int totalMessages,
                                 String groupId,
                                 int consumerNumber,
                                 int maxPolls,
                                 boolean commit,
                                 CompletableFuture<Integer> future) {
        consumeMessages(totalMessagesConsumed, totalMessages, groupId, consumerNumber, maxPolls, commit, future, Optional.empty());
    }

    private void consumeMessages(AtomicInteger totalMessagesConsumed,
                                 int totalMessages,
                                 String groupId,
                                 int consumerNumber,
                                 int maxPolls,
                                 boolean commit,
                                 CompletableFuture<Integer> future,
                                 Optional<Integer> maxFetchBytes) {
        KafkaShareConsumer<byte[], byte[]> shareConsumer;
        Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
        Map<TopicPartition, Exception> partitionExceptionMap = new HashMap<>();
        if (maxFetchBytes.isPresent()) {
            shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), groupId,
                    Collections.singletonMap(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxFetchBytes.get())));
        } else {
            shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), groupId);
        }
        shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgeCommitCallback(partitionOffsetsMap, partitionExceptionMap));
        shareConsumer.subscribe(Collections.singleton(tp.topic()));
        int messagesConsumed = 0;
        int retries = 0;
        try {
            if (totalMessages > 0) {
                while (totalMessagesConsumed.get() < totalMessages && retries < maxPolls) {
                    ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
                    messagesConsumed += records.count();
                    totalMessagesConsumed.addAndGet(records.count());
                    retries++;
                }
            } else {
                while (retries < maxPolls) {
                    ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2000));
                    messagesConsumed += records.count();
                    totalMessagesConsumed.addAndGet(records.count());
                    retries++;
                }
            }

            if (commit) {
                // Complete acknowledgement of the records
                shareConsumer.commitSync(Duration.ofMillis(10000));
            }
        } catch (Exception e) {
            fail("Consumer " + consumerNumber + " failed with exception: " + e);
        } finally {
            shareConsumer.close();
            future.complete(messagesConsumed);
        }
    }

    private <K, V> List<ConsumerRecord<K, V>> consumeRecords(KafkaShareConsumer<K, V> consumer,
                                                             int numRecords) {
        ArrayList<ConsumerRecord<K, V>> accumulatedRecords = new ArrayList<>();
        long startTimeMs = System.currentTimeMillis();
        while (accumulatedRecords.size() < numRecords) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(accumulatedRecords::add);
            long currentTimeMs = System.currentTimeMillis();
            if (currentTimeMs - startTimeMs > 60000) {
                fail("Timed out before consuming expected records.");
            }
        }
        return accumulatedRecords;
    }

    private void createTopic(String topicName) {
        Properties props = cluster.clientProperties();
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short) 1))).all().get();
        } catch (Exception e) {
            fail("Failed to create topic");
        }
    }

    private void deleteTopic(String topicName) {
        Properties props = cluster.clientProperties();
        try (Admin admin = Admin.create(props)) {
            admin.deleteTopics(Collections.singleton(topicName)).all().get();
        } catch (Exception e) {
            fail("Failed to create topic");
        }
    }

    private Admin createAdminClient() {
        Properties props = cluster.clientProperties();
        return Admin.create(props);
    }

    private <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer,
                                                      Serializer<V> valueSerializer) {
        Properties props = cluster.clientProperties();
        return new KafkaProducer<>(props, keySerializer, valueSerializer);
    }

    private <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer,
                                                      Serializer<V> valueSerializer,
                                                      String transactionalId) {
        Properties props = cluster.clientProperties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return new KafkaProducer<>(props, keySerializer, valueSerializer);
    }

    private <K, V> KafkaShareConsumer<K, V> createShareConsumer(Deserializer<K> keyDeserializer,
                                                                Deserializer<V> valueDeserializer,
                                                                String groupId) {
        Properties props = cluster.clientProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaShareConsumer<>(props, keyDeserializer, valueDeserializer);
    }

    private <K, V> KafkaShareConsumer<K, V> createShareConsumer(Deserializer<K> keyDeserializer,
                                                                Deserializer<V> valueDeserializer,
                                                                String groupId,
                                                                Map<?, ?> additionalProperties) {
        Properties props = cluster.clientProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.putAll(additionalProperties);
        return new KafkaShareConsumer<>(props, keyDeserializer, valueDeserializer);
    }

    private void warmup() throws InterruptedException, ExecutionException, TimeoutException {
        createTopic(warmupTp.topic());
        TestUtils.waitForCondition(() ->
                        !cluster.brokers().get(0).metadataCache().getAliveBrokerNodes(new ListenerName("EXTERNAL")).isEmpty(),
                DEFAULT_MAX_WAIT_MS, 100L, () -> "cache not up yet");
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(warmupTp.topic(), warmupTp.partition(), null, "key".getBytes(), "value".getBytes());
        KafkaProducer<byte[], byte[]> producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer());
        KafkaShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(new ByteArrayDeserializer(), new ByteArrayDeserializer(), "warmupgroup1");
        Set<String> subscription = Collections.singleton(warmupTp.topic());
        try {
            producer.send(record).get(15000, TimeUnit.MILLISECONDS);
            shareConsumer.subscribe(subscription);
            TestUtils.waitForCondition(
                    () -> shareConsumer.poll(Duration.ofMillis(5000)).count() == 1, 30000, 200L, () -> "warmup record not received");
        } finally {
            producer.close();
            shareConsumer.close();
        }
    }
}
