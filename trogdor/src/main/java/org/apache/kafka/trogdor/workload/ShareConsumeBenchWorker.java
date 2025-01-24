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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
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
        this.statusUpdater = new StatusUpdater();
        this.executor = Executors.newScheduledThreadPool(
                spec.threadsPerWorker() + 2, // 1 thread for all the ConsumeStatusUpdater and 1 for the StatusUpdater
                ThreadUtils.createThreadFactory("ShareConsumeBenchWorkerThread%d", false));
        this.statusUpdaterFuture = executor.scheduleAtFixedRate(this.statusUpdater, 1, 1, TimeUnit.MINUTES);
        this.workerStatus = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
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
                tasks.add(new ConsumeMessages(consumer(shareGroup, clientId(i)), spec.recordProcessor(), topics));
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
            // these defaults maybe over-written by the user-specified commonClientConf or consumerConf
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf());
            return new ThreadSafeShareConsumer(new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), clientId);
        }

        private String shareGroup() {
            return spec.shareGroup();
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

            StatusData statusData = new StatusData(
                    consumer.subscription(),
                    latSummary.numSamples(),
                    (long) (msgSummary.numSamples() * msgSummary.average()),
                    (long) msgSummary.average(),
                    latSummary.average(),
                    latSummary.percentiles().get(0).value(),
                    latSummary.percentiles().get(1).value(),
                    latSummary.percentiles().get(2).value(),
                    recordProcessorStatus);
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
                   @JsonProperty("recordProcessorStatus") Optional<JsonNode> recordProcessorStatus) {
            this.subscription = subscription;
            this.totalMessagesReceived = totalMessagesReceived;
            this.totalBytesReceived = totalBytesReceived;
            this.averageMessageSizeBytes = averageMessageSizeBytes;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
            this.recordProcessorStatus = recordProcessorStatus;
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
