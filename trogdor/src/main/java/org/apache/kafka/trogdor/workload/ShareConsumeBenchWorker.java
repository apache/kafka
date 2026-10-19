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

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.ThreadUtils;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ShareConsumeBenchWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ShareConsumeBenchWorker.class);

    private static final int THROTTLE_PERIOD_MS = 100;

    private final String id;
    private final ShareConsumeBenchSpec spec;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Queue<ThreadSafeShareConsumer> consumers = new ConcurrentLinkedQueue<>();
    private ScheduledExecutorService executor;
    private WorkerStatusTracker workerStatus;
    private StatusUpdater statusUpdater;
    private Future<?> statusUpdaterFuture;
    private KafkaFutureImpl<String> doneFuture;
    public ShareConsumeBenchWorker(String id, ShareConsumeBenchSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ShareConsumeBenchWorker is already running.");
        }
        log.info("{}: Activating ShareConsumeBenchWorker with {}", id, spec);
        if (spec.shareGroupDLQConfig().isPresent()) {
            try {
                setupDLQ(spec.shareGroupDLQConfig().get());
            } catch (Throwable e) {
                running.set(false);
                if (e instanceof Exception) {
                    throw (Exception) e;
                }
                throw new RuntimeException("Failed to set up the DLQ for share group " + shareGroup(), e);
            }
        }
        this.statusUpdater = new StatusUpdater();
        this.executor = Executors.newScheduledThreadPool(
                spec.threadsPerWorker() + 2, // 1 thread for all the ConsumeStatusUpdater and 1 for the StatusUpdater
                ThreadUtils.createThreadFactory("ShareConsumeBenchWorkerThread%d", false));
        this.statusUpdaterFuture = executor.scheduleAtFixedRate(this.statusUpdater, 1, 1, TimeUnit.MINUTES);
        this.workerStatus = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
    }

    /**
     * Creates the DLQ topic, enables it for use as a DLQ and configures the share group to use it.
     */
    private void setupDLQ(ShareGroupDLQConfig dlqConfig) throws Throwable {
        String dlqTopic = dlqConfig.topicName();
        NewTopic newTopic = dlqConfig.topicSpec().newTopic(dlqTopic);
        WorkerUtils.createTopics(log, spec.bootstrapServers(), spec.commonClientConf(), spec.adminClientConf(),
                Map.of(dlqTopic, newTopic), false);

        try (Admin adminClient = WorkerUtils.createAdminClient(spec.bootstrapServers(), spec.commonClientConf(), spec.adminClientConf())) {
            // Also done for a pre-existing topic, which would not have been created with the DLQ config.
            alterConfigs(adminClient, new ConfigResource(ConfigResource.Type.TOPIC, dlqTopic), List.of(
                    setOp(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true")));
            alterConfigs(adminClient, new ConfigResource(ConfigResource.Type.GROUP, shareGroup()), List.of(
                    setOp(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG, dlqTopic),
                    setOp(GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG,
                            String.valueOf(dlqConfig.copyRecordEnable()))));
        }
        log.info("{}: Share group {} will use {} as its DLQ topic.", id, shareGroup(), dlqTopic);
    }

    private static AlterConfigOp setOp(String name, String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static void alterConfigs(Admin adminClient, ConfigResource resource, Collection<AlterConfigOp> ops) throws Exception {
        try {
            adminClient.incrementalAlterConfigs(Map.of(resource, ops)).all().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to alter configs " + ops + " of " + resource, e);
        }
    }

    private String shareGroup() {
        return spec.shareGroup();
    }

    private boolean explicitAcknowledgement() {
        return spec.acknowledgeType().isPresent() || spec.recordAcknowledger().isPresent();
    }

    /**
     * Returns the acknowledge type for the record, or empty if the consumers use implicit acknowledgement.
     */
    private Optional<AcknowledgeType> acknowledgeTypeFor(ConsumerRecord<byte[], byte[]> record) {
        if (spec.recordAcknowledger().isPresent()) {
            AcknowledgeType type = spec.recordAcknowledger().get().acknowledge(record);
            if (type == null) {
                throw new IllegalStateException("RecordAcknowledger must return an acknowledge type, but returned null");
            }
            return Optional.of(type);
        }
        return spec.acknowledgeType();
    }

    public class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                List<Future<Void>> consumeTasks = new ArrayList<>();
                for (ConsumeMessages task : consumeTasks()) {
                    consumeTasks.add(executor.submit(task));
                }
                executor.submit(new CloseStatusUpdater(consumeTasks));
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }

        private List<ConsumeMessages> consumeTasks() {
            List<ConsumeMessages> tasks = new ArrayList<>();
            String shareGroup = shareGroup();
            int consumerCount = spec.threadsPerWorker();

            Set<String> topics = new HashSet<>(spec.expandTopicNames());

            for (int i = 0; i < consumerCount; i++) {
                ThreadSafeShareConsumer consumer = consumer(shareGroup, clientId(i));
                consumers.add(consumer);
                tasks.add(new ConsumeMessages(consumer, spec.recordProcessor(), topics));
            }

            return tasks;
        }

        private String clientId(int idx) {
            return String.format("consumer.%s-%d", id, idx);
        }

        /**
         * Creates a new KafkaConsumer instance
         */
        private ThreadSafeShareConsumer consumer(String shareGroup, String clientId) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, shareGroup);
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
            // an explicit acknowledge type requires the consumer to acknowledge each record itself
            if (explicitAcknowledgement()) {
                props.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
            }
            // these defaults maybe over-written by the user-specified commonClientConf or consumerConf
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf());
            if (explicitAcknowledgement()
                    && "implicit".equalsIgnoreCase(String.valueOf(props.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG)))) {
                throw new IllegalArgumentException(String.format("acknowledgeType or recordAcknowledger requires %s=explicit",
                        ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG));
            }
            return new ThreadSafeShareConsumer(new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), clientId);
        }
    }

    public class ConsumeMessages implements Callable<Void> {
        private final Histogram latencyHistogram;
        private final Histogram messageSizeHistogram;
        private final Future<?> statusUpdaterFuture;
        private final Throttle throttle;
        private final String clientId;
        private final ThreadSafeShareConsumer consumer;
        private final Optional<RecordProcessor> recordProcessor;

        private ConsumeMessages(ThreadSafeShareConsumer consumer,
                                Optional<RecordProcessor> recordProcessor) {
            this.latencyHistogram = new Histogram(10000);
            this.messageSizeHistogram = new Histogram(2 * 1024 * 1024);
            this.clientId = consumer.clientId();
            this.statusUpdaterFuture = executor.scheduleAtFixedRate(
                    new ConsumeStatusUpdater(latencyHistogram, messageSizeHistogram, consumer, recordProcessor), 1, 1, TimeUnit.MINUTES);
            int perPeriod;
            if (spec.targetMessagesPerSec() <= 0)
                perPeriod = Integer.MAX_VALUE;
            else
                perPeriod = WorkerUtils.perSecToPerPeriod(spec.targetMessagesPerSec(), THROTTLE_PERIOD_MS);

            this.throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
            this.consumer = consumer;
            this.recordProcessor = recordProcessor;
        }

        ConsumeMessages(ThreadSafeShareConsumer consumer,
                        Optional<RecordProcessor> recordProcessor,
                        Set<String> topics) {
            this(consumer, recordProcessor);
            log.info("Will consume from topics {}.", topics);
            this.consumer.subscribe(topics);
        }

        @Override
        public Void call() throws Exception {
            long messagesConsumed = 0;
            long bytesConsumed = 0;
            long startTimeMs = Time.SYSTEM.milliseconds();
            long startBatchMs = startTimeMs;
            long maxMessages = spec.maxMessages();
            try {
                while (messagesConsumed < maxMessages) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll();
                    if (records.isEmpty()) {
                        continue;
                    }
                    long endBatchMs = Time.SYSTEM.milliseconds();
                    long elapsedBatchMs = endBatchMs - startBatchMs;

                    // Do the record batch processing immediately to avoid latency skew.
                    recordProcessor.ifPresent(processor -> processor.processRecords(records));

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        // Explicit acknowledgement requires acknowledging each record individually.
                        acknowledgeTypeFor(record).ifPresent(type -> consumer.acknowledge(record, type));
                        messagesConsumed++;
                        long messageBytes = 0;
                        if (record.key() != null) {
                            messageBytes += record.serializedKeySize();
                        }
                        if (record.value() != null) {
                            messageBytes += record.serializedValueSize();
                        }
                        latencyHistogram.add(elapsedBatchMs);
                        messageSizeHistogram.add(messageBytes);
                        bytesConsumed += messageBytes;
                        if (messagesConsumed >= maxMessages)
                            break;

                        throttle.increment();
                    }
                    startBatchMs = Time.SYSTEM.milliseconds();
                }
                // Send the pending acknowledgements while the consumer is still a member of the share group.
                // Records after the maxMessages break are deliberately left unacknowledged.
                if (explicitAcknowledgement()) {
                    consumer.commitSync();
                }
            } catch (Exception e) {
                WorkerUtils.abort(log, "ConsumeRecords", e, doneFuture);
            } finally {
                statusUpdaterFuture.cancel(false);
                StatusData statusData =
                        new ConsumeStatusUpdater(latencyHistogram, messageSizeHistogram, consumer, spec.recordProcessor()).update();
                long curTimeMs = Time.SYSTEM.milliseconds();
                log.info("{} Consumed total number of messages={}, bytes={} in {} ms.  status: {}",
                        clientId, messagesConsumed, bytesConsumed, curTimeMs - startTimeMs, statusData);
            }
            consumer.close();
            return null;
        }
    }

    public class CloseStatusUpdater implements Runnable {
        private final List<Future<Void>> consumeTasks;

        CloseStatusUpdater(List<Future<Void>> consumeTasks) {
            this.consumeTasks = consumeTasks;
        }

        @Override
        public void run() {
            while (!consumeTasks.stream().allMatch(Future::isDone)) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    log.debug("{} was interrupted. Closing...", this.getClass().getName());
                    break; // close the thread
                }
            }
            statusUpdaterFuture.cancel(false);
            statusUpdater.update();
            doneFuture.complete("");
        }
    }

    class StatusUpdater implements Runnable {
        final Map<String, JsonNode> statuses;

        StatusUpdater() {
            statuses = new HashMap<>();
        }

        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                WorkerUtils.abort(log, "ConsumeStatusUpdater", e, doneFuture);
            }
        }

        synchronized void update() {
            workerStatus.update(JsonUtil.JSON_SERDE.valueToTree(statuses));
        }

        synchronized void updateConsumeStatus(String clientId, StatusData status) {
            statuses.put(clientId, JsonUtil.JSON_SERDE.valueToTree(status));
        }
    }

    /**
     * Runnable class that updates the status of a single consumer
     */
    public class ConsumeStatusUpdater implements Runnable {
        private final Histogram latencyHistogram;
        private final Histogram messageSizeHistogram;
        private final ThreadSafeShareConsumer consumer;
        private final Optional<RecordProcessor> recordProcessor;

        ConsumeStatusUpdater(Histogram latencyHistogram,
                             Histogram messageSizeHistogram,
                             ThreadSafeShareConsumer consumer,
                             Optional<RecordProcessor> recordProcessor) {
            this.latencyHistogram = latencyHistogram;
            this.messageSizeHistogram = messageSizeHistogram;
            this.consumer = consumer;
            this.recordProcessor = recordProcessor;
        }

        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                WorkerUtils.abort(log, "ConsumeStatusUpdater", e, doneFuture);
            }
        }

        StatusData update() {
            Histogram.Summary latSummary = latencyHistogram.summarize(StatusData.PERCENTILES);
            Histogram.Summary msgSummary = messageSizeHistogram.summarize(StatusData.PERCENTILES);

            // Parse out the RecordProcessor's status, id specified.
            Optional<JsonNode> recordProcessorStatus = Optional.empty();
            if (recordProcessor.isPresent()) {
                recordProcessorStatus = Optional.of(recordProcessor.get().processorStatus());
            }

            Optional<JsonNode> recordAcknowledgerStatus = spec.recordAcknowledger().map(RecordAcknowledger::acknowledgerStatus);

            StatusData statusData = new StatusData(
                    consumer.subscription(),
                    latSummary.numSamples(),
                    (long) (msgSummary.numSamples() * msgSummary.average()),
                    (long) msgSummary.average(),
                    latSummary.average(),
                    latSummary.percentiles().get(0).value(),
                    latSummary.percentiles().get(1).value(),
                    latSummary.percentiles().get(2).value(),
                    recordProcessorStatus,
                    recordAcknowledgerStatus);
            statusUpdater.updateConsumeStatus(consumer.clientId(), statusData);
            log.info("Status={}", JsonUtil.toJsonString(statusData));
            return statusData;
        }
    }

    public static class StatusData {
        private final long totalMessagesReceived;
        private final Set<String> subscription;
        private final long totalBytesReceived;
        private final long averageMessageSizeBytes;
        private final float averageLatencyMs;
        private final int p50LatencyMs;
        private final int p95LatencyMs;
        private final int p99LatencyMs;
        private final Optional<JsonNode> recordProcessorStatus;
        private final Optional<JsonNode> recordAcknowledgerStatus;

        /**
         * The percentiles to use when calculating the histogram data.
         * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
         */
        static final float[] PERCENTILES = {0.5f, 0.95f, 0.99f};
        @JsonCreator
        StatusData(@JsonProperty("subscription") Set<String> subscription,
                   @JsonProperty("totalMessagesReceived") long totalMessagesReceived,
                   @JsonProperty("totalBytesReceived") long totalBytesReceived,
                   @JsonProperty("averageMessageSizeBytes") long averageMessageSizeBytes,
                   @JsonProperty("averageLatencyMs") float averageLatencyMs,
                   @JsonProperty("p50LatencyMs") int p50latencyMs,
                   @JsonProperty("p95LatencyMs") int p95latencyMs,
                   @JsonProperty("p99LatencyMs") int p99latencyMs,
                   @JsonProperty("recordProcessorStatus") Optional<JsonNode> recordProcessorStatus,
                   @JsonProperty("recordAcknowledgerStatus") Optional<JsonNode> recordAcknowledgerStatus) {
            this.subscription = subscription;
            this.totalMessagesReceived = totalMessagesReceived;
            this.totalBytesReceived = totalBytesReceived;
            this.averageMessageSizeBytes = averageMessageSizeBytes;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
            this.recordProcessorStatus = recordProcessorStatus;
            this.recordAcknowledgerStatus = recordAcknowledgerStatus;
        }

        @JsonProperty
        public Set<String> subscription() {
            return subscription;
        }

        @JsonProperty
        public long totalMessagesReceived() {
            return totalMessagesReceived;
        }

        @JsonProperty
        public long totalBytesReceived() {
            return totalBytesReceived;
        }

        @JsonProperty
        public long averageMessageSizeBytes() {
            return averageMessageSizeBytes;
        }

        @JsonProperty
        public float averageLatencyMs() {
            return averageLatencyMs;
        }

        @JsonProperty
        public int p50LatencyMs() {
            return p50LatencyMs;
        }

        @JsonProperty
        public int p95LatencyMs() {
            return p95LatencyMs;
        }

        @JsonProperty
        public int p99LatencyMs() {
            return p99LatencyMs;
        }

        @JsonProperty
        public JsonNode recordProcessorStatus() {
            return recordProcessorStatus.orElse(null);
        }

        @JsonProperty
        public JsonNode recordAcknowledgerStatus() {
            return recordAcknowledgerStatus.orElse(null);
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ShareConsumeBenchWorker is not running.");
        }
        log.info("{}: Deactivating ShareConsumeBenchWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        for (ThreadSafeShareConsumer consumer : consumers) {
            consumer.close();
        }
        this.consumers.clear();
        this.executor = null;
        this.statusUpdater = null;
        this.statusUpdaterFuture = null;
        this.workerStatus = null;
        this.doneFuture = null;
    }

    /**
     * A thread-safe KafkaShareConsumer wrapper
     */
    private static class ThreadSafeShareConsumer {
        private final KafkaShareConsumer<byte[], byte[]> consumer;
        private final String clientId;
        private final ReentrantLock consumerLock;
        private boolean closed = false;

        ThreadSafeShareConsumer(KafkaShareConsumer<byte[], byte[]> consumer, String clientId) {
            this.consumer = consumer;
            this.clientId = clientId;
            this.consumerLock = new ReentrantLock();
        }

        ConsumerRecords<byte[], byte[]> poll() {
            this.consumerLock.lock();
            try {
                return consumer.poll(Duration.ofMillis(50));
            } finally {
                this.consumerLock.unlock();
            }
        }

        void acknowledge(ConsumerRecord<byte[], byte[]> record, AcknowledgeType type) {
            this.consumerLock.lock();
            try {
                consumer.acknowledge(record, type);
            } finally {
                this.consumerLock.unlock();
            }
        }

        void commitSync() {
            this.consumerLock.lock();
            try {
                consumer.commitSync().forEach((partition, exception) ->
                        exception.ifPresent(e -> log.warn("{} failed to acknowledge records of {}", clientId, partition, e)));
            } finally {
                this.consumerLock.unlock();
            }
        }

        void close() {
            if (closed)
                return;
            this.consumerLock.lock();
            try {
                consumer.unsubscribe();
                Utils.closeQuietly(consumer, "consumer");
                closed = true;
            } finally {
                this.consumerLock.unlock();
            }
        }

        void subscribe(Set<String> topics) {
            this.consumerLock.lock();
            try {
                consumer.subscribe(topics);
            } finally {
                this.consumerLock.unlock();
            }
        }

        Set<String> subscription() {
            this.consumerLock.lock();
            try {
                return consumer.subscription();
            } finally {
                this.consumerLock.unlock();
            }
        }

        String clientId() {
            return clientId;
        }

        KafkaShareConsumer<byte[], byte[]> consumer() {
            return consumer;
        }
    }
}
