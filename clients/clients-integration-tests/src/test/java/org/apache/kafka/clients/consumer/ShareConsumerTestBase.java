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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.server.util.ServerTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class ShareConsumerTestBase {
    protected final ClusterInstance cluster;
    protected final TopicPartition tp = new TopicPartition("topic", 0);
    protected Uuid tpId;
    protected final TopicPartition tp2 = new TopicPartition("topic2", 0);
    protected final TopicPartition warmupTp = new TopicPartition("warmup", 0);
    protected List<TopicPartition> sgsTopicPartitions;
    protected static final String KEY = "content-type";
    protected static final String VALUE = "application/octet-stream";
    protected static final String EXPLICIT = "explicit";
    protected static final String IMPLICIT = "implicit";
    protected static final String RECORD_LIMIT = "record_limit";
    protected static final String BATCH_OPTIMIZED = "batch_optimized";

    public ShareConsumerTestBase(ClusterInstance cluster) {
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
        ServerTestUtils.clearYammerMetrics();
    }

    /**
     * Test implementation of AcknowledgementCommitCallback to track the completed acknowledgements.
     * partitionOffsetsMap is used to track the offsets acknowledged for each partition.
     * partitionExceptionMap is used to track the exception encountered for each partition if any.
     * Note - Multiple calls to {@link #onComplete(Map, Exception)} will not update the partitionExceptionMap for any existing partitions,
     * so please ensure to clear the partitionExceptionMap after every call to onComplete() in a single test.
     */
    public static class TestableAcknowledgementCommitCallback implements AcknowledgementCommitCallback {
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

    /**
     * Util class to encapsulate state for a consumer/producer
     * being executed by an {@link ExecutorService}.
     */
    protected static class ClientState {
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicInteger count = new AtomicInteger(0);

        AtomicBoolean done() {
            return done;
        }

        AtomicInteger count() {
            return count;
        }
    }

    protected int produceMessages(int messageCount) {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes());
            IntStream.range(0, messageCount).forEach(__ -> producer.send(record));
            producer.flush();
        }
        return messageCount;
    }

    protected void produceMessagesWithTimestamp(int messageCount, long startingTimestamp) {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tp.topic(), tp.partition(), startingTimestamp + i,
                    ("key " + i).getBytes(), ("value " + i).getBytes());
                producer.send(record);
            }
            producer.flush();
        }
    }

    protected void produceCommittedTransaction(Producer<byte[], byte[]> transactionalProducer, String message) {
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

    protected void produceAbortedTransaction(Producer<byte[], byte[]> transactionalProducer, String message) {
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

    protected void produceCommittedAndAbortedTransactionsInInterval(Producer<byte[], byte[]> transactionalProducer, int messageCount, int intervalAbortedTransactions) {
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

    protected int consumeMessages(AtomicInteger totalMessagesConsumed,
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

    protected int consumeMessages(AtomicInteger totalMessagesConsumed,
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

    protected int consumeMessages(ShareConsumer<byte[], byte[]> consumer,
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

    protected <K, V> List<ConsumerRecord<K, V>> consumeRecords(ShareConsumer<K, V> consumer,
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

    protected Uuid createTopic(String topicName) {
        return createTopic(topicName, 1, 1);
    }

    protected Uuid createTopic(String topicName, int numPartitions, int replicationFactor) {
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

    protected void deleteTopic(String topicName) {
        assertDoesNotThrow(() -> {
            try (Admin admin = createAdminClient()) {
                admin.deleteTopics(Set.of(topicName)).all().get();
            }
        }, "Failed to delete topic");
    }

    protected Admin createAdminClient() {
        return cluster.admin();
    }

    protected <K, V> Producer<K, V> createProducer() {
        return createProducer(Map.of());
    }

    protected <K, V> Producer<K, V> createProducer(Map<String, Object> config) {
        return cluster.producer(config);
    }

    protected <K, V> Producer<K, V> createProducer(String transactionalId) {
        return createProducer(
            Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId
            )
        );
    }

    protected <K, V> ShareConsumer<K, V> createShareConsumer(String groupId) {
        return createShareConsumer(groupId, Map.of());
    }

    protected <K, V> ShareConsumer<K, V> createShareConsumer(
        String groupId,
        Map<?, ?> additionalProperties
    ) {
        return createShareConsumer(groupId, additionalProperties, null, null);
    }

    protected <K, V> ShareConsumer<K, V> createShareConsumer(
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

    protected void warmup() throws InterruptedException {
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

    protected void waitForMetadataCache() throws InterruptedException {
        TestUtils.waitForCondition(() ->
                !cluster.brokers().get(0).metadataCache().getAliveBrokerNodes(new ListenerName("EXTERNAL")).isEmpty(),
            DEFAULT_MAX_WAIT_MS, 100L, () -> "cache not up yet");
    }

    protected void verifyShareGroupStateTopicRecordsProduced() {
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

    protected void alterShareGroupConfig(String groupId, String configKey, String newValue) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
        Map<ConfigResource, Collection<AlterConfigOp>> alterEntries = new HashMap<>();
        alterEntries.put(configResource, List.of(new AlterConfigOp(new ConfigEntry(
            configKey, newValue), AlterConfigOp.OpType.SET)));
        AlterConfigsOptions alterOptions = new AlterConfigsOptions();
        try (Admin adminClient = createAdminClient()) {
            assertDoesNotThrow(() -> adminClient.incrementalAlterConfigs(alterEntries, alterOptions)
                .all()
                .get(60, TimeUnit.SECONDS), "Failed to alter configs");
        }

        // This config is changed dynamically in tests, and we need it to have propagated before the test proceeds.
        // Describing the config with a new admin client is not totally foolproof, but it's better than just
        // altering the config and continuing.
        try (Admin adminClient = createAdminClient()) {
            assertDoesNotThrow(() ->
                TestUtils.waitForCondition(() -> {
                    Config config = adminClient.describeConfigs(List.of(configResource)).all().get().get(configResource);
                    ConfigEntry entry = config.get(configKey);
                    return entry != null && entry.value().equals(newValue);
                }, 10000L, 100L, () -> "New config value did not propagate"), "Failed to describe configs");
        }
    }

    protected void alterShareAutoOffsetReset(String groupId, String newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, newValue);
    }

    protected void alterShareDeliveryCountLimit(String groupId, String newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG, newValue);
    }

    protected void alterShareIsolationLevel(String groupId, String newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG, newValue);
    }

    protected void alterShareRenewAcknowledgeEnable(String groupId, boolean newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG, Boolean.toString(newValue));
    }

    protected List<Integer> topicPartitionLeader(Admin adminClient, String topicName, int partition) throws InterruptedException, ExecutionException {
        return adminClient.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName)
            .partitions().stream()
            .filter(info -> info.partition() == partition)
            .map(info -> info.leader().id())
            .filter(info -> info != -1)
            .toList();
    }

    protected SharePartitionOffsetInfo sharePartitionOffsetInfo(Admin adminClient, String groupId, TopicPartition tp) throws InterruptedException, ExecutionException {
        SharePartitionOffsetInfo partitionResult;
        ListShareGroupOffsetsResult result = adminClient.listShareGroupOffsets(
            Map.of(groupId, new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp))),
            new ListShareGroupOffsetsOptions().timeoutMs(30000)
        );
        partitionResult = result.partitionsToOffsetInfo(groupId).get().get(tp);
        return partitionResult;
    }

    protected void verifySharePartitionStartOffset(Admin adminClient, String groupId, TopicPartition tp, long expectedStartOffset) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            SharePartitionOffsetInfo sharePartitionOffsetInfo = sharePartitionOffsetInfo(adminClient, groupId, tp);
            return sharePartitionOffsetInfo != null &&
                sharePartitionOffsetInfo.startOffset() == expectedStartOffset;
        }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to retrieve share partition lag");
    }

    protected void verifySharePartitionLag(Admin adminClient, String groupId, TopicPartition tp, long expectedLag) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            SharePartitionOffsetInfo sharePartitionOffsetInfo = sharePartitionOffsetInfo(adminClient, groupId, tp);
            return sharePartitionOffsetInfo != null &&
                sharePartitionOffsetInfo.lag().isPresent() &&
                sharePartitionOffsetInfo.lag().get() == expectedLag;
        }, DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, () -> "Failed to retrieve share partition lag");
    }

    protected void verifySharePartitionOffsetsDeleted(Admin adminClient, String groupId, TopicPartition tp) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> sharePartitionOffsetInfo(adminClient, groupId, tp) == null,
            DEFAULT_MAX_WAIT_MS,
            DEFAULT_POLL_INTERVAL_MS,
            () -> "Failed to retrieve share partition lag");
    }

    protected void alterShareGroupOffsets(Admin adminClient, String groupId, TopicPartition topicPartition, Long newOffset) throws InterruptedException, ExecutionException {
        adminClient.alterShareGroupOffsets(
            groupId,
            Map.of(topicPartition, newOffset),
            new AlterShareGroupOffsetsOptions().timeoutMs(30000)).partitionResult(topicPartition).get();
    }

    protected void deleteShareGroupOffsets(Admin adminClient, String groupId, String topic) throws InterruptedException, ExecutionException {
        adminClient.deleteShareGroupOffsets(
            groupId,
            Set.of(topic),
            new DeleteShareGroupOffsetsOptions().timeoutMs(30000)).topicResult(topic).get();
    }

    protected void alterSharePartitionMaxRecordLocks(String groupId, String newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG, newValue);
    }

    protected void alterShareRecordLockDurationMs(String groupId, int newValue) {
        alterShareGroupConfig(groupId, GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG, Integer.toString(newValue));
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
    protected static class ComplexShareConsumer<K, V> implements Runnable {
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

    protected void shutdownExecutorService(ExecutorService service) {
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

    protected ConsumerRecords<byte[], byte[]> waitedPoll(
        ShareConsumer<byte[], byte[]> shareConsumer,
        long pollMs,
        int recordCount
    ) {
        return waitedPoll(shareConsumer, pollMs, recordCount, false, "", List.of());
    }

    protected ConsumerRecords<byte[], byte[]> waitedPoll(
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

    protected void validateExpectedRecordsInEachPollAndRelease(
        ShareConsumer<byte[], byte[]> shareConsumer,
        int startOffset,
        int lastOffset,
        int expectedRecordsInEachPoll
    ) {
        validateExpectedRecordsInEachPollAndAcknowledge(shareConsumer, startOffset, lastOffset, expectedRecordsInEachPoll, AcknowledgeType.RELEASE);
    }

    protected void validateExpectedRecordsInEachPollAndAcknowledge(
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

    protected void waitForAssignment(String groupId, List<TopicPartition> tps) {
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