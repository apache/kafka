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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName;
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
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS;
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

@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
@Timeout(1200)
@Tag("integration")
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
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
public class ShareConsumerTest {
    private final ClusterInstance cluster;
    private final TopicPartition tp = new TopicPartition("topic", 0);
    private Uuid tpId;
    private final TopicPartition tp2 = new TopicPartition("topic2", 0);
    private final TopicPartition warmupTp = new TopicPartition("warmup", 0);
    private List<TopicPartition> sgsTopicPartitions;
    private static final String KEY = "content-type";
    private static final String VALUE = "application/octet-stream";
    private static final String EXPLICIT = "explicit";
    private static final String IMPLICIT = "implicit";
    private static final String RECORD_LIMIT = "record_limit";
    private static final String BATCH_OPTIMIZED = "batch_optimized";

    public ShareConsumerTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() {
        try {
            this.cluster.waitForReadyBrokers();
            tpId = createTopic("topic");
            createTopic("topic2");
            sgsTopicPartitions = IntStream.range(0, 3)
                .mapToObj(part -> new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, part))
                .toList();
            this.warmup();
        } catch (Exception e) {
            fail(e);
        }
    }

    @AfterEach
    public void tearDown() {
        kafka.utils.TestUtils.clearYammerMetrics();
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

    @ClusterTest
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

    @ClusterTest
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
     * Test implementation of AcknowledgementCommitCallback to track the completed acknowledgements.
     * partitionOffsetsMap is used to track the offsets acknowledged for each partition.
     * partitionExceptionMap is used to track the exception encountered for each partition if any.
     * Note - Multiple calls to {@link #onComplete(Map, Exception)} will not update the partitionExceptionMap for any existing partitions,
     * so please ensure to clear the partitionExceptionMap after every call to onComplete() in a single test.
     */
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
                if (!partitionExceptionMap.containsKey(partition.topicPartition()) && exception != null) {
                    partitionExceptionMap.put(partition.topicPartition(), exception);
                }
            });
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
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            shareConsumer.close();
        }, "Consumer close should not wait for full timeout when broker is already shutdown");
    }

    @ClusterTest
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

    @ClusterTest
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

    @ClusterTest
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

    @ClusterTest
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

    @ClusterTest
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
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackWakeup<>(shareConsumer));
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
            shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallbackThrows(callbackCalled));
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

    private static class TestableAcknowledgementCommitCallbackThrows implements AcknowledgementCommitCallback {
        private final AtomicBoolean callbackCalled;

        public TestableAcknowledgementCommitCallbackThrows(AtomicBoolean callbackCalled) {
            this.callbackCalled = callbackCalled;
        }

        @Override
        public void onComplete(Map<TopicIdPartition, Set<Long>> offsetsMap, Exception exception) {
            callbackCalled.set(true);
            throw new org.apache.kafka.common.errors.OutOfOrderSequenceException("Exception thrown in TestableAcknowledgementCommitCallbackThrows.onComplete");
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

    @ClusterTest
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

    @ClusterTest
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
            adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5L)));

            int messageCount = consumeMessages(new AtomicInteger(0), 5, groupId, 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, messageCount);

            // We write 5 records to the topic, so they would be written from offsets 10-14 on the topic.
            for (int i = 0; i < 5; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 14, so the LSO should move to 14.
            adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(14L)));

            int consumeMessagesCount = consumeMessages(new AtomicInteger(0), 1, groupId, 1, 10, true);
            // The record returned belong to offset 14.
            assertEquals(1, consumeMessagesCount);

            // We delete records before offset 15, so the LSO should move to 15 and now no records should be returned.
            adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(15L)));

            messageCount = consumeMessages(new AtomicInteger(0), 0, groupId, 1, 5, true);
            assertEquals(0, messageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetDefaultValue() {
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 0, true, "group1", List.of(tp));
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
        alterShareAutoOffsetReset("group1", "earliest");
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
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
        alterShareAutoOffsetReset("group1", "earliest");
        try (
            Producer<byte[], byte[]> producer = createProducer();
            Admin adminClient = createAdminClient()
        ) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
            for (int i = 0; i < 10; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }

            // We delete records before offset 5, so the LSO should move to 5.
            adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5L)));

            int consumedMessageCount = consumeMessages(new AtomicInteger(0), 5, "group1", 1, 10, true);
            // The records returned belong to offsets 5-9.
            assertEquals(5, consumedMessageCount);
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetMultipleGroupsWithDifferentValue() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareAutoOffsetReset("group2", "latest");
        try (ShareConsumer<byte[], byte[]> shareConsumerEarliest = createShareConsumer("group1");
             ShareConsumer<byte[], byte[]> shareConsumerLatest = createShareConsumer("group2");
             Producer<byte[], byte[]> producer = createProducer()) {

            shareConsumerEarliest.subscribe(Set.of(tp.topic()));
            shareConsumerLatest.subscribe(Set.of(tp.topic()));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // Producing a record.
            producer.send(record);
            producer.flush();
            ConsumerRecords<byte[], byte[]> records1 = waitedPoll(shareConsumerEarliest, 2500L, 1);
            // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
            // all messages present on the partition
            assertEquals(1, records1.count());

            ConsumerRecords<byte[], byte[]> records2 = waitedPoll(shareConsumerLatest, 2500L, 0, true, "group2", List.of(tp));
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

            shareConsumer.subscribe(Set.of(tp.topic()));

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
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group2")) {

            shareConsumer.subscribe(Set.of(tp.topic()));
            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(shareConsumer, 3);
            assertEquals(3, records.size());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testShareAutoOffsetResetByDurationInvalidFormat() {
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

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
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

    @ClusterTest
    public void testDeliveryCountNotIncreaseAfterSessionClose() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
            for (int i = 0; i < 10; i++) {
                assertDoesNotThrow(() -> producer.send(record).get(), "Failed to send records");
            }
        }

        // Perform the fetch, close in a loop.
        for (int count = 0; count < ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT; count++) {
            consumeMessages(new AtomicInteger(0), 10, "group1", 1, 10, false);
        }

        // If the delivery count is increased, consumer will get nothing.
        int consumedMessageCount = consumeMessages(new AtomicInteger(0), 10, "group1", 1, 10, true);
        // The records returned belong to offsets 0-9.
        assertEquals(10, consumedMessageCount);
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testDeliveryCountDifferentBehaviorWhenClosingSessionWithExplicitAcknowledgement() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 2);
            assertEquals(2, records.count());
            // Acknowledge the first record with AcknowledgeType.RELEASE
            shareConsumer.acknowledge(records.records(tp).get(0), AcknowledgeType.RELEASE);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
        }

        // Test delivery count
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of())) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 2);
            assertEquals(2, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
            assertEquals((short) 1, records.records(tp).get(1).deliveryCount().get());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "2"),
        }
    )
    public void testBehaviorOnDeliveryCountBoundary() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null,
                "key".getBytes(), "value".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 1, records.records(tp).get(0).deliveryCount().get());
            // Acknowledge the record with AcknowledgeType.RELEASE.
            shareConsumer.acknowledge(records.records(tp).get(0), AcknowledgeType.RELEASE);
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());

            // Consume again, the delivery count should be 2.
            records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());

        }

        // Start again and same record should be delivered
        try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of())) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());
            assertEquals((short) 2, records.records(tp).get(0).deliveryCount().get());
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
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

        // Produce messages until we want.
        service.execute(() -> {
            try (Producer<byte[], byte[]> producer = createProducer()) {
                while (!prodState.done().get()) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(multiTp.topic(), multiTp.partition(), null, "key".getBytes(), "value".getBytes());
                    producer.send(record);
                    producer.flush();
                    prodState.count().incrementAndGet();
                }
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

        // Let the complex consumer read the messages.
        service.schedule(() -> prodState.done().set(true), 5L, TimeUnit.SECONDS);

        // All messages which can be read are read, some would be redelivered (roughly 3 times the records produced).
        TestUtils.waitForCondition(complexCons1::isDone, 45_000L, () -> "did not close!");
        int delta = complexCons1.recordsRead() - (int) (prodState.count().get() * 3 * 0.95);    // 3 times with margin of error (5%).

        assertTrue(delta > 0,
            String.format("Producer (%d) and share consumer (%d) record count mismatch.", prodState.count().get(), complexCons1.recordsRead()));

        shutdownExecutorService(service);

        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest(
        brokers = 1,
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
    public void testReadCommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            produceCommittedAndAbortedTransactionsInInterval(transactionalProducer, 10, 5);
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 8);
            // 5th and 10th message transaction was aborted, hence they won't be included in the fetched records.
            assertEquals(8, records.count());
            int messageCounter = 1;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                if (messageCounter % 5 == 0)
                    messageCounter++;
                assertEquals("Message " + messageCounter, new String(record.value()));
                messageCounter++;
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testReadUncommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_uncommitted");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1")) {
            produceCommittedAndAbortedTransactionsInInterval(transactionalProducer, 10, 5);
            shareConsumer.subscribe(Set.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 10);
            // Even though 5th and 10th message transaction was aborted, they will be included in the fetched records since IsolationLevel is READ_UNCOMMITTED.
            assertEquals(10, records.count());
            int messageCounter = 1;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                assertEquals("Message " + messageCounter, new String(record.value()));
                messageCounter++;
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadUncommittedToReadCommittedIsolationLevel() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_uncommitted");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();
            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 2", new String(record.value()));
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                records = waitedPoll(shareConsumer, 5000L, 2);
                // Message 3 and Message 4 would be returned by this poll.
                assertEquals(2, records.count());
                Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();
                record = recordIterator.next();
                assertEquals("Message 3", new String(record.value()));
                record = recordIterator.next();
                assertEquals("Message 4", new String(record.value()));
                // We will make Message 3 and Message 4 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
                shareConsumer.commitSync();

                // We are altering IsolationLevel to READ_COMMITTED now. We will only read committed transactions now.
                alterShareIsolationLevel("group1", "read_committed");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_COMMITTED, we can consume Message 3 (committed transaction that was released), Message 5 and Message 8.
                // We cannot consume Message 4 (aborted transaction that was released), Message 6 and Message 7 since they were aborted.
                List<String> messages = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            messages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return messages.size() == 3;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                assertEquals("Message 3", messages.get(0));
                assertEquals("Message 5", messages.get(1));
                assertEquals("Message 8", messages.get(2));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadCommittedToReadUncommittedIsolationLevelWithReleaseAck() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();

            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, Map.of()));

                // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
                // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    return pollRecords.count() == 0 && partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.get(tp).contains(3L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction and marker offset for Message 2");

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap2 = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap2, Map.of()));

                records = waitedPoll(shareConsumer, 5000L, 1);
                // Message 3 would be returned by this poll.
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 3", new String(record.value()));
                // We will make Message 3 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.RELEASE));
                shareConsumer.commitSync();

                // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    if (pollRecords.count() > 0) {
                        // We will release Message 3 again if it was received in this poll().
                        pollRecords.forEach(consumerRecord -> shareConsumer.acknowledge(consumerRecord, AcknowledgeType.RELEASE));
                    }
                    return partitionOffsetsMap2.containsKey(tp) && partitionOffsetsMap2.get(tp).contains(7L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction marker offset for Message 4");

                // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
                alterShareIsolationLevel("group1", "read_uncommitted");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_UNCOMMITTED, we can consume Message 3 (committed transaction that was released), Message 5, Message 6, Message 7 and Message 8.
                Set<String> finalMessages = new HashSet<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            finalMessages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return finalMessages.size() == 5;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                Set<String> expected = Set.of("Message 3", "Message 5", "Message 6", "Message 7", "Message 8");
                assertEquals(expected, finalMessages);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testAlterReadCommittedToReadUncommittedIsolationLevelWithRejectAck() {
        alterShareAutoOffsetReset("group1", "earliest");
        alterShareIsolationLevel("group1", "read_committed");
        try (Producer<byte[], byte[]> transactionalProducer = createProducer("T1");
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer("group1", Map.of(
                 ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))) {
            shareConsumer.subscribe(Set.of(tp.topic()));
            transactionalProducer.initTransactions();

            try {
                // First transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 1");

                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 5000L, 1);
                assertEquals(1, records.count());
                ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                assertEquals("Message 1", new String(record.value()));
                assertEquals(tp.topic(), record.topic());
                assertEquals(tp.partition(), record.partition());
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                shareConsumer.commitSync();

                // Second transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 2");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap, Map.of()));

                // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
                // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(500));
                    return pollRecords.count() == 0 && partitionOffsetsMap.containsKey(tp) && partitionOffsetsMap.get(tp).contains(3L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction and marker offset for Message 2");

                // Third transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 3");
                // Fourth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 4");

                // Setting the acknowledgement commit callback to verify acknowledgement completion.
                Map<TopicPartition, Set<Long>> partitionOffsetsMap2 = new HashMap<>();
                shareConsumer.setAcknowledgementCommitCallback(new TestableAcknowledgementCommitCallback(partitionOffsetsMap2, Map.of()));

                records = waitedPoll(shareConsumer, 5000L, 1);
                // Message 3 would be returned by this poll.
                assertEquals(1, records.count());
                record = records.iterator().next();
                assertEquals("Message 3", new String(record.value()));
                // We will make Message 3 available for re-consumption.
                records.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.REJECT));
                shareConsumer.commitSync();

                // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
                TestUtils.waitForCondition(() -> {
                    shareConsumer.poll(Duration.ofMillis(500));
                    return partitionOffsetsMap2.containsKey(tp) && partitionOffsetsMap2.get(tp).contains(7L);
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume abort transaction marker offset for Message 4");

                // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
                alterShareIsolationLevel("group1", "read_uncommitted");

                // Fifth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 5");
                // Sixth transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 6");
                // Seventh transaction is aborted.
                produceAbortedTransaction(transactionalProducer, "Message 7");
                // Eighth transaction is committed.
                produceCommittedTransaction(transactionalProducer, "Message 8");

                // Since isolation level is READ_UNCOMMITTED, we can consume Message 5, Message 6, Message 7 and Message 8.
                List<String> finalMessages = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> pollRecords = shareConsumer.poll(Duration.ofMillis(5000));
                    if (pollRecords.count() > 0) {
                        for (ConsumerRecord<byte[], byte[]> pollRecord : pollRecords)
                            finalMessages.add(new String(pollRecord.value()));
                        pollRecords.forEach(consumedRecord -> shareConsumer.acknowledge(consumedRecord, AcknowledgeType.ACCEPT));
                        shareConsumer.commitSync();
                    }
                    return finalMessages.size() == 4;
                }, DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume all records post altering share isolation level");

                assertEquals("Message 5", finalMessages.get(0));
                assertEquals("Message 6", finalMessages.get(1));
                assertEquals("Message 7", finalMessages.get(2));
                assertEquals("Message 8", finalMessages.get(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                transactionalProducer.close();
            }
        }
        verifyShareGroupStateTopicRecordsProduced();
    }

    @ClusterTest
    public void testPollInBatchOptimizedMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5, ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, BATCH_OPTIMIZED))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            // although max.poll.records is set to 5, in batch optimized mode we will still get all 10 records.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testPollInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5, ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            // In record limit mode we will get only up to max.poll.records number of records.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testPollAndExplicitAcknowledgeSingleMessageInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 10; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));

            for (int i = 0; i < 10; i++) {
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
                assertEquals(1, records.count());
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    shareConsumer.acknowledge(rec, AcknowledgeType.ACCEPT);
                }
                shareConsumer.commitSync();
            }
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeSuccessInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            for (int i = 0; i < 15; i++) {
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            for (ConsumerRecord<byte[], byte[]> rec : records) {
                shareConsumer.acknowledge(rec, AcknowledgeType.ACCEPT);
            }
            shareConsumer.commitSync();

            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testExplicitAcknowledgeReleaseAcceptInRecordLimitMode() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                     ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                     ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            for (int i = 0; i < 20; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp2.topic(), tp2.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();
            shareConsumer.subscribe(List.of(tp2.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            Map<TopicIdPartition, Optional<KafkaException>> result;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                }
                result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                count++;
            }

            // Poll again to get 10 records.
            records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            verifyShareGroupStateTopicRecordsProduced();
        }
    }

    @ClusterTest
    public void testRenewAcknowledgementOnPoll() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                count++;
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();
            assertEquals(15, acknowledgementsCommitted.get());
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    @ClusterTest
    public void testRenewAcknowledgementOnCommitSync() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());
            assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());

            // The updated acquisition lock timeout is only applied when the next poll is called.
            alterShareRecordLockDurationMs("group1", 25000);

            int count = 0;
            Map<TopicIdPartition, Optional<KafkaException>> result;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.of(15000), shareConsumer.acquisitionLockTimeoutMs());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
                count++;
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 2500L, 5);
            assertEquals(5, records.count());
            assertEquals(Optional.of(25000), shareConsumer.acquisitionLockTimeoutMs());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    @ClusterTest
    public void testRenewAcknowledgementInvalidStateRecord() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message ".getBytes());
            producer.send(record);
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            assertEquals(1, records.count());

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                shareConsumer.acknowledge(rec, AcknowledgeType.REJECT);
                shareConsumer.commitSync();
                assertThrows(IllegalStateException.class, () -> shareConsumer.acknowledge(rec, AcknowledgeType.RENEW));
            }
        }
        verifyYammerMetricCount("ackType=Renew", 0);
    }

    @ClusterTest(
        brokers = 1,
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.record.lock.duration.ms", value = "12000"),
            @ClusterConfigProperty(key = "group.share.min.record.lock.duration.ms", value = "12000"),
        }
    )
    public void testRenewAcknowledgementNoResultInPoll() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                 "group1",
                 Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            AtomicInteger acknowledgementsCommitted = new AtomicInteger(0);
            shareConsumer.setAcknowledgementCommitCallback((offsetsByTopicPartition, exception) ->
                offsetsByTopicPartition.forEach((tip, offsets) -> acknowledgementsCommitted.addAndGet(offsets.size())));

            for (int i = 0; i < 10; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 10);
            assertEquals(10, records.count());

            int count = 0;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (count % 2 == 0) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                } else {
                    shareConsumer.acknowledge(record, AcknowledgeType.RENEW);
                }
                count++;
            }

            // 5 more records (total 15 produced).
            for (int i = 10; i < 15; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }

            // Get the rest of all 5 records.
            records = waitedPoll(shareConsumer, 11500L, 0);  // This will send the acks but not return next 5 records (10-15)
            assertEquals(10, acknowledgementsCommitted.get());
            assertEquals(0, records.count());
            verifyYammerMetricCount("ackType=Renew", 5);

            // Renewal duration passed, now records will be back.
            records = waitedPoll(shareConsumer, 2500L, 5);  // Renewed records as well as 10-15 records.
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();

            records = waitedPoll(shareConsumer, 2500L, 5);  // 10-15 records.
            assertEquals(5, records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
            }

            shareConsumer.commitSync();

            // Initial - 5 renew + 5 accept, Subsequent - 5 renewed accepted + 5 fresh accepted (10-15)
            assertEquals(20, acknowledgementsCommitted.get());
        }
        verifyYammerMetricCount("ackType=Renew", 5);
    }

    private void verifyYammerMetricCount(String filterString, int count) {
        com.yammer.metrics.core.Metric renewAck = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().toString().contains(filterString))
            .findAny()
            .orElseThrow(() -> new AssertionError("metric not found"))
            .getValue();

        assertEquals(count, ((Meter) renewAck).count());
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
    public void testSharePartitionLagForSingleShareConsumer() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
            Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagForMultipleShareConsumers() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(groupId);
             ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer1.subscribe(List.of(tp.topic()));
            shareConsumer2.subscribe(List.of(tp.topic()));
            // Polling share consumer 1 to make sure the share partition is created and the records are consumed.
            waitedPoll(shareConsumer1, 2500L, 1);
            // Acknowledge and commit the consumed records to update the share partition state.
            shareConsumer1.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing more records to the share partition.
            producer.send(record);
            // Polling share consumer 2 this time.
            waitedPoll(shareConsumer2, 2500L, 1);
            // Since the consumed record hasn't been acknowledged yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            // Acknowledge and commit the consumed records to update the share partition state.
            shareConsumer2.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagWithReleaseAcknowledgement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // The produced record is consumed.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Now release the record - it should be available for redelivery.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.RELEASE));
            shareConsumer.commitSync();
            // After releasing the lag should be 1, because the record is released for redelivery.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            // The record is now consumed again.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record to mark it as consumed.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting the record, the lag should be 0 because all the produced records have been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testSharePartitionLagWithRejectAcknowledgement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId, Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT));
             Admin adminClient = createAdminClient()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, 1);
            // Accept the record first to move the offset forward and register the state with persister.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.ACCEPT));
            shareConsumer.commitSync();
            // After accepting, the lag should be 0 because the record is consumed successfully.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // The produced record is consumed.
            records = waitedPoll(shareConsumer, 2500L, 1);
            // Now reject the record - it should not be available for redelivery.
            records.forEach(r -> shareConsumer.acknowledge(r, AcknowledgeType.REJECT));
            shareConsumer.commitSync();
            // After rejecting the lag should be 0, because the record is permanently rejected and offset moves forward.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    public void testSharePartitionLagOnGroupCoordinatorMovement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            String topicName = "testTopicWithReplicas";
            // Create a topic with replication factor 3
            createTopic(topicName, 1, 3);
            TopicPartition tp = new TopicPartition(topicName, 0);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Produce first record and consume it
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            List<Integer> curGroupCoordNodeId;
            // Find the broker which is the group coordinator for the share group.
            curGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
            assertEquals(1, curGroupCoordNodeId.size());
            // Shut down the coordinator broker
            KafkaBroker broker = cluster.brokers().get(curGroupCoordNodeId.get(0));
            cluster.shutdownBroker(curGroupCoordNodeId.get(0));
            // Wait for it to be completely shutdown
            broker.awaitShutdown();
            // Wait for the leaders of share coordinator, group coordinator and topic partition to be elected, if needed, on a different broker.
            TestUtils.waitForCondition(() -> {
                List<Integer> newShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
                List<Integer> newGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
                List<Integer> newTopicPartitionLeader = topicPartitionLeader(adminClient, tp.topic(), tp.partition());

                return newShareCoordNodeId.size() == 1 && !Objects.equals(newShareCoordNodeId.get(0), curGroupCoordNodeId.get(0)) &&
                    newGroupCoordNodeId.size() == 1 && !Objects.equals(newGroupCoordNodeId.get(0), curGroupCoordNodeId.get(0)) &&
                    newTopicPartitionLeader.size() == 1 && !Objects.equals(newTopicPartitionLeader.get(0), curGroupCoordNodeId.get(0));
            }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to elect new leaders after broker shutdown");
            // After group coordinator shutdown, check that lag is still 1
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "3")
        }
    )
    public void testSharePartitionLagOnShareCoordinatorMovement() {
        String groupId = "group1";
        alterShareAutoOffsetReset(groupId, "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
             ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId);
             Admin adminClient = createAdminClient()) {
            String topicName = "testTopicWithReplicas";
            // Create a topic with replication factor 3
            createTopic(topicName, 1, 3);
            TopicPartition tp = new TopicPartition(topicName, 0);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "Message".getBytes());
            // Produce first record and consume it
            producer.send(record);
            producer.flush();
            shareConsumer.subscribe(List.of(tp.topic()));
            // Polling share consumer to make sure the share partition is created and the record is consumed.
            waitedPoll(shareConsumer, 2500L, 1);
            // Acknowledge and commit the consumed record to update the share partition state.
            shareConsumer.commitSync();
            // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
            verifySharePartitionLag(adminClient, groupId, tp, 0L);
            // Producing another record to the share partition.
            producer.send(record);
            producer.flush();
            // Since the new record has not been consumed yet, the share partition lag should be 1.
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
            List<Integer> curShareCoordNodeId;
            // Find the broker which is the share coordinator for the share partition.
            curShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
            assertEquals(1, curShareCoordNodeId.size());
            // Shut down the coordinator broker
            KafkaBroker broker = cluster.brokers().get(curShareCoordNodeId.get(0));
            cluster.shutdownBroker(curShareCoordNodeId.get(0));
            // Wait for it to be completely shutdown
            broker.awaitShutdown();
            // Wait for the leaders of share coordinator, group coordinator and topic partition to be elected, if needed, on a different broker.
            TestUtils.waitForCondition(() -> {
                List<Integer> newShareCoordNodeId = topicPartitionLeader(adminClient, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
                List<Integer> newGroupCoordNodeId = topicPartitionLeader(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, 0);
                List<Integer> newTopicPartitionLeader = topicPartitionLeader(adminClient, tp.topic(), tp.partition());

                return newShareCoordNodeId.size() == 1 && !Objects.equals(newShareCoordNodeId.get(0), curShareCoordNodeId.get(0)) &&
                    newGroupCoordNodeId.size() == 1 && !Objects.equals(newGroupCoordNodeId.get(0), curShareCoordNodeId.get(0)) &&
                    newTopicPartitionLeader.size() == 1 && !Objects.equals(newTopicPartitionLeader.get(0), curShareCoordNodeId.get(0));
            }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to elect new leaders after broker shutdown");
            // After share coordinator shutdown and new leader's election, check that lag is still 1
            verifySharePartitionLag(adminClient, groupId, tp, 1L);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @ClusterTest
    public void testFetchWithThrottledDelivery() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            // Produce a batch of 100 messages
            for (int i = 0; i < 100; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 5 iterations, each time acknowledging with RELEASE. 5 is the default
            // delivery limit hence we should see throttling from Math.ceil(5/2) = 3 fetches.
            int throttleDeliveryLimit = 3;
            for (int i = 0; i < 5; i++) {
                // Adjust expected fetch count based on throttling. If i < throttleDeliveryLimit, we get full batch of 100.
                // If i == 4 i.e. the last delivery, then we get 1 record.
                // Otherwise, we get half the previous fetch count due to throttling. In this case, 100 >> (i - throttleDeliveryLimit + 1) it is 50 for i=3.
                int expectedFetchCount = (i < throttleDeliveryLimit) ? 100 : ((i == 4) ? 1 : 50);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());

                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(),
                    result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Offset 0 has already reached the delivery limit hence shall be archived.
            // Offset 1 to 49 shall be in last delivery attempt and hence 1 record per poll.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 1, 50, 1);
            // Delivery limit 4.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 50, 100, 50);
            // Delivery limit 5.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 50, 100, 1);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "10"),
        }
    )
    public void testFetchWithThrottledDeliveryBatchesWithIncreasedDeliveryLimit() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(
                    ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 512
                )
            )
        ) {
            // Produce records in complete power of 2 to fully test the throttling behavior.
            int producedMessageCount = 512;
            // Produce a batch of 512 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            // Map which defines expected fetch count for each delivery attempt from 1 to 10.
            Map<Integer, Integer> expectedFetchCountMap = Map.of(1, 512, 2, 512, 3, 512, 4, 512, 5, 512,
                6, 256, 7, 128, 8, 64, 9, 32, 10, 1);
            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
            // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
            for (int i = 0; i < 10; i++) {
                int expectedFetchCount = expectedFetchCountMap.get(i + 1);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());
                // Acknowledge all records with RELEASE.
                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Offset 0 is already verified above, so start from offset 1 and as it's last delivery cycle
            // hence expectedRecords is 1 for each poll till offset 32.
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 1, 32, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 32, 64, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 32, 64, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 128, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 96, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 64, 96, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 96, 128, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 96, 128, 1);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 256, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 192, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 160, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 128, 160, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 160, 192, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 160, 192, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 256, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 224, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 192, 224, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 224, 256, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 224, 256, 1);
            // Delivery 6
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 512, 256);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 384, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 320, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 288, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 256, 288, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 288, 320, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 288, 320, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 384, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 352, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 320, 352, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 352, 384, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 352, 384, 1);
            // Delivery 7
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 512, 128);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 448, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 416, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 384, 416, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 416, 448, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 416, 448, 1);
            // Delivery 8
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 512, 64);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 480, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 448, 480, 1);
            // Delivery 9
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 480, 512, 32);
            // Delivery 10
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 480, 512, 1);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "10"),
        }
    )
    public void testFetchWithThrottledDeliveryValidateDeliveryCount() throws InterruptedException {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT))
        ) {
            int producedMessageCount = 500;
            // Produce a batch of 500 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            // Map to track delivery count for each offset.
            Map<Long, Integer> offsetToDeliveryCountMap = new HashMap<>();
            // Map which defines expected fetch count for each delivery attempt from 1 to 10.
            Map<Integer, Integer> expectedFetchCountMap = Map.of(1, 500, 2, 500, 3, 500, 4, 500, 5, 500,
                6, 250, 7, 125, 8, 62, 9, 31, 10, 1);
            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
            // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
            for (int i = 0; i < 10; i++) {
                int expectedFetchCount = expectedFetchCountMap.get(i + 1);
                ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedFetchCount);
                assertEquals(expectedFetchCount, records.count());
                // Update delivery count for each offset.
                records.forEach(record -> {
                    if (!offsetToDeliveryCountMap.containsKey(record.offset())) {
                        offsetToDeliveryCountMap.put(record.offset(), 1);
                    } else  {
                        offsetToDeliveryCountMap.put(record.offset(), offsetToDeliveryCountMap.get(record.offset()) + 1);
                    }
                });
                // Acknowledge with RELEASE.
                records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                assertEquals(1, result.size());
                assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            }

            // Validate every offset is delivered at most till delivery limit.
            waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
                if (!records.isEmpty()) {
                    records.forEach(record -> {
                        if (!offsetToDeliveryCountMap.containsKey(record.offset())) {
                            offsetToDeliveryCountMap.put(record.offset(), 1);
                        } else  {
                            offsetToDeliveryCountMap.put(record.offset(), offsetToDeliveryCountMap.get(record.offset()) + 1);
                        }
                    });
                    records.forEach(record -> shareConsumer.acknowledge(record, AcknowledgeType.RELEASE));
                    Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
                    assertEquals(1, result.size());
                    assertEquals(Optional.empty(),
                        result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
                }
                return offsetToDeliveryCountMap.size() == 500 &&
                    offsetToDeliveryCountMap.values().stream().allMatch(deliveryCount -> deliveryCount == 10);
                },
                120000L, // 120 seconds.
                50L,
                () -> "failed to get records till delivery limit"
            );

            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "group.share.delivery.count.limit", value = "2"),
        }
    )
    public void testFetchWithThrottledDeliveryBatchesWithDecreasedDeliveryLimit() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(
                "group1",
                Map.of(
                    ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 512
                ))
        ) {
            // Produce records in complete power of 2 to fully test the throttling behavior.
            int producedMessageCount = 512;
            // Produce a batch of 512 messages
            for (int i = 0; i < producedMessageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
            }
            producer.flush();

            shareConsumer.subscribe(List.of(tp.topic()));
            // Fetch records in 2 iterations, each time acknowledging with RELEASE. As throttling
            // currently applies for delivery limit > 2, hence we should get full batch in both fetches.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 0, 512, 512);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer, 0, 512, 512);
            // Next poll should not have any records as all records have reached delivery limit.
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    @ClusterTest
    public void testFetchWithThrottledDeliveryBatchesMultipleConsumers() {
        alterShareAutoOffsetReset("group1", "earliest");
        try (Producer<byte[], byte[]> producer = createProducer();
            ShareConsumer<byte[], byte[]> shareConsumer1 = createShareConsumer(
                "group1",
                Map.of(
                    ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1
                ));
            ShareConsumer<byte[], byte[]> shareConsumer2 = createShareConsumer(
                "group1",
                Map.of(
                    ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2
                ))
        ) {
            // Produce 2 records in separate batches.
            for (int i = 0; i < 2; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(),
                    null, "key".getBytes(), ("Message " + i).getBytes());
                producer.send(record);
                // Flush immediately to create 2 different batches.
                producer.flush();
            }

            shareConsumer1.subscribe(List.of(tp.topic()));
            shareConsumer2.subscribe(List.of(tp.topic()));
            // Fetch from consumer1 - should get 1 record as max.poll.records=1.
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer1, 2500L, 1);
            assertEquals(1, records.count());
            // Verify the first offset of the fetched records.
            assertEquals(0, records.iterator().next().offset());
            // Fetch from consumer2 - should get 1 record as offset 0 is Acquired by consumer1.
            // Release the record from consumer2 after fetching until the last delivery attempt.
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);
            validateExpectedRecordsInEachPollAndRelease(shareConsumer2, 1, 2, 1);

            // Now release the record from consumer1. Fetch again from consumer2 to verify it gets the released record.
            // And should only get 1 record at offset 0 as offset 1 record is in final delivery attempt.
            records.forEach(record -> shareConsumer1.acknowledge(record, AcknowledgeType.RELEASE));
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer1.commitSync();
            assertEquals(1, result.size());
            assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
            // Fetch from consumer2 - should get the released record at offset 0. Accept the record after fetching.
            validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer2, 0, 1, 1, AcknowledgeType.ACCEPT);
            // Now fetch the last record at offset 1 from consumer2 in its final delivery attempt.
            validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer2, 1, 2, 1, AcknowledgeType.ACCEPT);

            // Next poll from consumer1 should not have any records as all records have reached delivery limit.
            records = shareConsumer1.poll(Duration.ofMillis(2500L));
            assertTrue(records.isEmpty(), "Records should be empty as all records have reached delivery limit. But received: " + records.count());
        }
    }

    /**
     * Util class to encapsulate state for a consumer/producer
     * being executed by an {@link ExecutorService}.
     */
    private static class ClientState {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicInteger count = new AtomicInteger(0);

        AtomicBoolean done() {
            return done;
        }

        AtomicInteger count() {
            return count;
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

    private void produceCommittedTransaction(Producer<byte[], byte[]> transactionalProducer, String message) {
        try {
            transactionalProducer.beginTransaction();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, message.getBytes(), message.getBytes());
            Future<RecordMetadata> future = transactionalProducer.send(record);
            transactionalProducer.flush();
            future.get(); // Ensure producer send is complete before committing
            transactionalProducer.commitTransaction();
        } catch (Exception e) {
            transactionalProducer.abortTransaction();
        }
    }

    private void produceAbortedTransaction(Producer<byte[], byte[]> transactionalProducer, String message) {
        try {
            transactionalProducer.beginTransaction();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, message.getBytes(), message.getBytes());
            Future<RecordMetadata> future = transactionalProducer.send(record);
            transactionalProducer.flush();
            future.get(); // Ensure producer send is complete before aborting
            transactionalProducer.abortTransaction();
        } catch (Exception e) {
            transactionalProducer.abortTransaction();
        }
    }

    private void produceCommittedAndAbortedTransactionsInInterval(Producer<byte[], byte[]> transactionalProducer, int messageCount, int intervalAbortedTransactions) {
        transactionalProducer.initTransactions();
        int transactionCount = 0;
        try {
            for (int i = 0; i < messageCount; i++) {
                transactionalProducer.beginTransaction();
                String recordMessage = "Message " + (i + 1);
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, recordMessage.getBytes(), recordMessage.getBytes());
                Future<RecordMetadata> future = transactionalProducer.send(record);
                transactionalProducer.flush();
                // Increment transaction count
                transactionCount++;
                if (transactionCount % intervalAbortedTransactions == 0) {
                    // Aborts every intervalAbortedTransactions transaction
                    transactionalProducer.abortTransaction();
                } else {
                    // Commits other transactions
                    future.get(); // Ensure producer send is complete before committing
                    transactionalProducer.commitTransaction();
                }
            }
        } catch (Exception e) {
            transactionalProducer.abortTransaction();
        } finally {
            transactionalProducer.close();
        }
    }

    private int consumeMessages(AtomicInteger totalMessagesConsumed,
                                int totalMessages,
                                String groupId,
                                int consumerNumber,
                                int maxPolls,
                                boolean commit) {
        return assertDoesNotThrow(() -> {
            try (ShareConsumer<byte[], byte[]> shareConsumer = createShareConsumer(groupId)) {
                shareConsumer.subscribe(Set.of(tp.topic()));
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
                shareConsumer.subscribe(Set.of(tp.topic()));
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

    private Uuid createTopic(String topicName) {
        return createTopic(topicName, 1, 1);
    }

    private Uuid createTopic(String topicName, int numPartitions, int replicationFactor) {
        AtomicReference<Uuid> topicId = new AtomicReference<>(null);
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                CreateTopicsResult result = admin.createTopics(Set.of(new NewTopic(topicName, numPartitions, (short) replicationFactor)));
                result.all().get();
                topicId.set(result.topicId(topicName).get());
            }
        }, "Failed to create topic");

        return topicId.get();
    }

    private void deleteTopic(String topicName) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                admin.deleteTopics(Set.of(topicName)).all().get();
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
        return createShareConsumer(groupId, additionalProperties, null, null);
    }

    private <K, V> ShareConsumer<K, V> createShareConsumer(
        String groupId,
        Map<?, ?> additionalProperties,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        Properties props = new Properties();
        props.putAll(additionalProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Map<String, Object> conf = new HashMap<>();
        props.forEach((k, v) -> conf.put((String) k, v));
        return cluster.shareConsumer(conf, keyDeserializer, valueDeserializer);
    }

    private void warmup() throws InterruptedException {
        createTopic(warmupTp.topic());
        waitForMetadataCache();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(warmupTp.topic(), warmupTp.partition(), null, "key".getBytes(), "value".getBytes());
        Set<String> subscription = Set.of(warmupTp.topic());
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

    private void alterShareIsolationLevel(String groupId, String newValue) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
            GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, newValue), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions();
        try (Admin adminClient = createAdminClient()) {
            assertDoesNotThrow(() -> adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                .all()
                .get(60, TimeUnit.SECONDS), "Failed to alter configs");
        }
    }

    private List<Integer> topicPartitionLeader(Admin adminClient, String topicName, int partition) throws InterruptedException, ExecutionException {
        return adminClient.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName)
            .partitions().stream()
            .filter(info -> info.partition() == partition)
            .map(info -> info.leader().id())
            .filter(info -> info != -1)
            .toList();
    }

    private SharePartitionOffsetInfo sharePartitionOffsetInfo(Admin adminClient, String groupId, TopicPartition tp) throws InterruptedException, ExecutionException {
        SharePartitionOffsetInfo partitionResult;
        ListShareGroupOffsetsResult result = adminClient.listShareGroupOffsets(
            Map.of(groupId, new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp))),
            new ListShareGroupOffsetsOptions().timeoutMs(30000)
        );
        partitionResult = result.partitionsToOffsetInfo(groupId).get().get(tp);
        return partitionResult;
    }

    private void verifySharePartitionLag(Admin adminClient, String groupId, TopicPartition tp, long expectedLag) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            SharePartitionOffsetInfo sharePartitionOffsetInfo = sharePartitionOffsetInfo(adminClient, groupId, tp);
            return sharePartitionOffsetInfo != null &&
                sharePartitionOffsetInfo.lag().isPresent() &&
                sharePartitionOffsetInfo.lag().get() == expectedLag;
        }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to retrieve share partition lag");
    }

    private void alterShareRecordLockDurationMs(String groupId, int newValue) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
            GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, Integer.toString(newValue)), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions();
        try (Admin adminClient = createAdminClient()) {
            assertDoesNotThrow(() -> adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                .all()
                .get(60, TimeUnit.SECONDS), "Failed to alter configs");
        }
    }

    /**
     * Test utility which encapsulates a {@link ShareConsumer} whose record processing
     * behavior can be supplied as a function argument.
     * <p></p>
     * This can be used to create different consume patterns on the broker and study
     * the status of broker side share group abstractions.
     *
     * @param <K> - key type of the records consumed
     * @param <V> - value type of the records consumed
     */
    private static class ComplexShareConsumer<K, V> implements Runnable {
        public static final int POLL_TIMEOUT_MS = 15000;
        public static final int MAX_DELIVERY_COUNT = ShareGroupConfig.SHARE_GROUP_DELIVERY_COUNT_LIMIT_DEFAULT;

        private final String topicName;
        private final Map<String, Object> configs = new HashMap<>();
        private final ClientState state = new ClientState();
        private final Predicate<ConsumerRecords<K, V>> exitCriteria;
        private final BiConsumer<ShareConsumer<K, V>, ConsumerRecord<K, V>> processFunc;

        ComplexShareConsumer(
            String bootstrapServers,
            String topicName,
            String groupId,
            Map<String, Object> additionalProperties
        ) {
            this(
                bootstrapServers,
                topicName,
                groupId,
                additionalProperties,
                records -> records.count() == 0,
                (consumer, record) -> {
                    short deliveryCountBeforeAccept = (short) ((record.offset() + record.offset() / (MAX_DELIVERY_COUNT + 2)) % (MAX_DELIVERY_COUNT + 2));
                    if (deliveryCountBeforeAccept == 0) {
                        consumer.acknowledge(record, AcknowledgeType.REJECT);
                    } else if (record.deliveryCount().get() == deliveryCountBeforeAccept) {
                        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    } else {
                        consumer.acknowledge(record, AcknowledgeType.RELEASE);
                    }
                }
            );
        }

        ComplexShareConsumer(
            String bootstrapServers,
            String topicName,
            String groupId,
            Map<String, Object> additionalProperties,
            Predicate<ConsumerRecords<K, V>> exitCriteria,
            BiConsumer<ShareConsumer<K, V>, ConsumerRecord<K, V>> processFunc
        ) {
            this.exitCriteria = Objects.requireNonNull(exitCriteria);
            this.processFunc = Objects.requireNonNull(processFunc);
            this.topicName = topicName;
            this.configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            this.configs.putAll(additionalProperties);
            this.configs.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            this.configs.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        }

        @Override
        public void run() {
            try (ShareConsumer<K, V> consumer = new KafkaShareConsumer<>(configs)) {
                consumer.subscribe(Set.of(this.topicName));
                while (!state.done().get()) {
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                    state.count().addAndGet(records.count());
                    if (exitCriteria.test(records)) {
                        state.done().set(true);
                    }
                    records.forEach(record -> processFunc.accept(consumer, record));
                }
            }
        }

        boolean isDone() {
            return state.done().get();
        }

        int recordsRead() {
            return state.count().get();
        }
    }

    private void shutdownExecutorService(ExecutorService service) {
        service.shutdown();
        try {
            if (!service.awaitTermination(5L, TimeUnit.SECONDS)) {
                service.shutdownNow();
            }
        } catch (Exception e) {
            service.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ConsumerRecords<byte[], byte[]> waitedPoll(
        ShareConsumer<byte[], byte[]> shareConsumer,
        long pollMs,
        int recordCount
    ) {
        return waitedPoll(shareConsumer, pollMs, recordCount, false, "", List.of());
    }

    private ConsumerRecords<byte[], byte[]> waitedPoll(
        ShareConsumer<byte[], byte[]> shareConsumer,
        long pollMs,
        int recordCount,
        boolean checkAssignment,
        String groupId,
        List<TopicPartition> tps
    ) {
        AtomicReference<ConsumerRecords<byte[], byte[]>> recordsAtomic = new AtomicReference<>();
        try {
            waitForCondition(() -> {
                    ConsumerRecords<byte[], byte[]> recs = shareConsumer.poll(Duration.ofMillis(pollMs));
                    recordsAtomic.set(recs);
                    if (checkAssignment) {
                        waitForAssignment(groupId, tps);
                    }
                    return recs.count() == recordCount;
                },
                DEFAULT_MAX_WAIT_MS,
                500L,
                () -> "failed to get records"
            );
            return recordsAtomic.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateExpectedRecordsInEachPollAndRelease(
        ShareConsumer<byte[], byte[]> shareConsumer,
        int startOffset,
        int lastOffset,
        int expectedRecordsInEachPoll
    ) {
        validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer, startOffset, lastOffset, expectedRecordsInEachPoll, AcknowledgeType.RELEASE);
    }

    private void validateExpectedRecordsInEachPollAndAcknowledge(
        ShareConsumer<byte[], byte[]> shareConsumer,
        int startOffset,
        int lastOffset,
        int expectedRecordsInEachPoll,
        AcknowledgeType acknowledgeType
    ) {
        for (int i = startOffset; i < lastOffset; i = i + expectedRecordsInEachPoll) {
            ConsumerRecords<byte[], byte[]> records = waitedPoll(shareConsumer, 2500L, expectedRecordsInEachPoll);
            assertEquals(expectedRecordsInEachPoll, records.count());
            // Verify the first offset of the fetched records.
            assertEquals(i, records.iterator().next().offset());

            records.forEach(record -> shareConsumer.acknowledge(record, acknowledgeType));
            Map<TopicIdPartition, Optional<KafkaException>> result = shareConsumer.commitSync();
            assertEquals(1, result.size());
            assertEquals(Optional.empty(), result.get(new TopicIdPartition(tpId, tp.partition(), tp.topic())));
        }
    }

    private void waitForAssignment(String groupId, List<TopicPartition> tps) {
        try {
            waitForCondition(() -> {
                    try (Admin admin = createAdminClient()) {
                        Collection<ShareMemberDescription> members = admin.describeShareGroups(List.of(groupId),
                            new DescribeShareGroupsOptions().includeAuthorizedOperations(true)
                        ).describedGroups().get(groupId).get().members();
                        Set<TopicPartition> assigned = new HashSet<>();
                        members.forEach(desc -> {
                            if (desc.assignment() != null) {
                                assigned.addAll(desc.assignment().topicPartitions());
                            }
                        });
                        return assigned.containsAll(tps);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                DEFAULT_MAX_WAIT_MS,
                1000L,
                () -> "tps not assigned to members"
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class SerializerImpl implements Serializer<byte[]> {

        @Override
        public byte[] serialize(String topic, Headers headers, byte[] data) {
            headers.add(KEY, VALUE.getBytes());
            return data;
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            fail("method should not be invoked");
            return null;
        }
    }

    public static class DeserializerImpl implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(String topic, Headers headers, byte[] data) {
            Header header = headers.lastHeader(KEY);
            assertEquals("application/octet-stream", header == null ? null : new String(header.value()));
            return data;
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            fail("method should not be invoked");
            return null;
        }
    }
}
