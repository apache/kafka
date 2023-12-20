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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.apache.kafka.trogdor.workload.TransactionGenerator.TransactionAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceBenchWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ProduceBenchWorker.class);
    
    private static final int THROTTLE_PERIOD_MS = 100;

    private final String id;

    private final ProduceBenchSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService executor;

    private WorkerStatusTracker status;

    private KafkaFutureImpl<String> doneFuture;

    public ProduceBenchWorker(String id, ProduceBenchSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ProducerBenchWorker is already running.");
        }
        log.info("{}: Activating ProduceBenchWorker with {}", id, spec);
        // Create an executor with 2 threads.  We need the second thread so
        // that the StatusUpdater can run in parallel with SendRecords.
        this.executor = Executors.newScheduledThreadPool(2,
            ThreadUtils.createThreadFactory("ProduceBenchWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
    }

    public class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                Map<String, NewTopic> newTopics = new HashMap<>();
                HashSet<TopicPartition> active = new HashSet<>();
                for (Map.Entry<String, PartitionsSpec> entry :
                        spec.activeTopics().materialize().entrySet()) {
                    String topicName = entry.getKey();
                    PartitionsSpec partSpec = entry.getValue();
                    newTopics.put(topicName, partSpec.newTopic(topicName));
                    for (Integer partitionNumber : partSpec.partitionNumbers()) {
                        active.add(new TopicPartition(topicName, partitionNumber));
                    }
                }
                if (active.isEmpty()) {
                    throw new RuntimeException("You must specify at least one active topic.");
                }
                for (Map.Entry<String, PartitionsSpec> entry :
                        spec.inactiveTopics().materialize().entrySet()) {
                    String topicName = entry.getKey();
                    PartitionsSpec partSpec = entry.getValue();
                    newTopics.put(topicName, partSpec.newTopic(topicName));
                }
                status.update(new TextNode("Creating " + newTopics.keySet().size() + " topic(s)"));
                WorkerUtils.createTopics(log, spec.bootstrapServers(), spec.commonClientConf(),
                                         spec.adminClientConf(), newTopics, false);
                status.update(new TextNode("Created " + newTopics.keySet().size() + " topic(s)"));
                executor.submit(new SendRecords(active));
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }
    }

    private static class SendRecordsCallback implements Callback {
        private final SendRecords sendRecords;
        private final long startMs;

        SendRecordsCallback(SendRecords sendRecords, long startMs) {
            this.sendRecords = sendRecords;
            this.startMs = startMs;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = Time.SYSTEM.milliseconds();
            long durationMs = now - startMs;
            sendRecords.recordDuration(durationMs);
            if (exception != null) {
                log.error("SendRecordsCallback: error", exception);
            }
        }
    }

    /**
     * A subclass of Throttle which flushes the Producer right before the throttle injects a delay.
     * This avoids including throttling latency in latency measurements.
     */
    private static class SendRecordsThrottle extends Throttle {
        private final KafkaProducer<?, ?> producer;

        SendRecordsThrottle(int maxPerPeriod, KafkaProducer<?, ?> producer) {
            super(maxPerPeriod, THROTTLE_PERIOD_MS);
            this.producer = producer;
        }

        @Override
        protected synchronized void delay(long amount) throws InterruptedException {
            long startMs = time().milliseconds();
            producer.flush();
            long endMs = time().milliseconds();
            long delta = endMs - startMs;
            super.delay(amount - delta);
        }
    }

    public class SendRecords implements Callable<Void> {
        private final HashSet<TopicPartition> activePartitions;

        private final Histogram histogram;

        private final Future<?> statusUpdaterFuture;

        private final KafkaProducer<byte[], byte[]> producer;

        private final PayloadIterator keys;

        private final PayloadIterator values;

        private final Optional<TransactionGenerator> transactionGenerator;

        private final Throttle throttle;

        private Iterator<TopicPartition> partitionsIterator;
        private Future<RecordMetadata> sendFuture;
        private AtomicLong transactionsCommitted;
        private boolean enableTransactions;

        SendRecords(HashSet<TopicPartition> activePartitions) {
            this.activePartitions = activePartitions;
            this.partitionsIterator = activePartitions.iterator();
            this.histogram = new Histogram(10000);

            this.transactionGenerator = spec.transactionGenerator();
            this.enableTransactions = this.transactionGenerator.isPresent();
            this.transactionsCommitted = new AtomicLong();

            int perPeriod = WorkerUtils.perSecToPerPeriod(spec.targetMessagesPerSec(), THROTTLE_PERIOD_MS);
            this.statusUpdaterFuture = executor.scheduleWithFixedDelay(
                new StatusUpdater(histogram, transactionsCommitted), 30, 30, TimeUnit.SECONDS);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            if (enableTransactions)
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "produce-bench-transaction-id-" + UUID.randomUUID());
            // add common client configs to producer properties, and then user-specified producer configs
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf());
            this.producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
            this.keys = new PayloadIterator(spec.keyGenerator());
            this.values = new PayloadIterator(spec.valueGenerator());
            if (spec.skipFlush()) {
                this.throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
            } else {
                this.throttle = new SendRecordsThrottle(perPeriod, producer);
            }
        }

        @Override
        public Void call() throws Exception {
            long startTimeMs = Time.SYSTEM.milliseconds();
            try {
                try {
                    if (enableTransactions)
                        producer.initTransactions();

                    long sentMessages = 0;
                    while (sentMessages < spec.maxMessages()) {
                        if (enableTransactions) {
                            boolean tookAction = takeTransactionAction();
                            if (tookAction)
                                continue;
                        }
                        sendMessage();
                        sentMessages++;
                    }
                    if (enableTransactions)
                        takeTransactionAction(); // give the transactionGenerator a chance to commit if configured evenly
                } catch (Exception e) {
                    if (enableTransactions)
                        producer.abortTransaction();
                    throw e;
                } finally {
                    if (sendFuture != null) {
                        try {
                            sendFuture.get();
                        } catch (Exception e) {
                            log.error("Exception on final future", e);
                        }
                    }
                    producer.close();
                }
            } catch (Exception e) {
                WorkerUtils.abort(log, "SendRecords", e, doneFuture);
            } finally {
                statusUpdaterFuture.cancel(false);
                StatusData statusData = new StatusUpdater(histogram, transactionsCommitted).update();
                long curTimeMs = Time.SYSTEM.milliseconds();
                log.info("Sent {} total record(s) in {} ms.  status: {}",
                    histogram.summarize().numSamples(), curTimeMs - startTimeMs, statusData);
            }
            doneFuture.complete("");
            return null;
        }

        private boolean takeTransactionAction() {
            boolean tookAction = true;
            TransactionAction nextAction = transactionGenerator.get().nextAction();
            switch (nextAction) {
                case BEGIN_TRANSACTION:
                    log.debug("Beginning transaction.");
                    producer.beginTransaction();
                    break;
                case COMMIT_TRANSACTION:
                    log.debug("Committing transaction.");
                    producer.commitTransaction();
                    transactionsCommitted.getAndIncrement();
                    break;
                case ABORT_TRANSACTION:
                    log.debug("Aborting transaction.");
                    producer.abortTransaction();
                    break;
                case NO_OP:
                    tookAction = false;
                    break;
            }
            return tookAction;
        }

        private void sendMessage() throws InterruptedException {
            if (!partitionsIterator.hasNext())
                partitionsIterator = activePartitions.iterator();

            TopicPartition partition = partitionsIterator.next();
            ProducerRecord<byte[], byte[]> record;
            if (spec.useConfiguredPartitioner()) {
                record = new ProducerRecord<>(
                    partition.topic(), keys.next(), values.next());
            } else {
                record = new ProducerRecord<>(
                    partition.topic(), partition.partition(), keys.next(), values.next());
            }
            sendFuture = producer.send(record,
                new SendRecordsCallback(this, Time.SYSTEM.milliseconds()));
            throttle.increment();
        }

        void recordDuration(long durationMs) {
            histogram.add(durationMs);
        }
    }

    public class StatusUpdater implements Runnable {
        private final Histogram histogram;
        private final AtomicLong transactionsCommitted;

        StatusUpdater(Histogram histogram, AtomicLong transactionsCommitted) {
            this.histogram = histogram;
            this.transactionsCommitted = transactionsCommitted;
        }

        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }

        StatusData update() {
            Histogram.Summary summary = histogram.summarize(StatusData.PERCENTILES);
            StatusData statusData = new StatusData(summary.numSamples(), summary.average(),
                summary.percentiles().get(0).value(),
                summary.percentiles().get(1).value(),
                summary.percentiles().get(2).value(),
                transactionsCommitted.get());
            status.update(JsonUtil.JSON_SERDE.valueToTree(statusData));
            return statusData;
        }
    }

    public static class StatusData {
        private final long totalSent;
        private final float averageLatencyMs;
        private final int p50LatencyMs;
        private final int p95LatencyMs;
        private final int p99LatencyMs;
        private final long transactionsCommitted;

        /**
         * The percentiles to use when calculating the histogram data.
         * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
         */
        final static float[] PERCENTILES = {0.5f, 0.95f, 0.99f};

        @JsonCreator
        StatusData(@JsonProperty("totalSent") long totalSent,
                   @JsonProperty("averageLatencyMs") float averageLatencyMs,
                   @JsonProperty("p50LatencyMs") int p50latencyMs,
                   @JsonProperty("p95LatencyMs") int p95latencyMs,
                   @JsonProperty("p99LatencyMs") int p99latencyMs,
                   @JsonProperty("transactionsCommitted") long transactionsCommitted) {
            this.totalSent = totalSent;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
            this.transactionsCommitted = transactionsCommitted;
        }

        @JsonProperty
        public long totalSent() {
            return totalSent;
        }

        @JsonProperty
        public long transactionsCommitted() {
            return transactionsCommitted;
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
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ProduceBenchWorker is not running.");
        }
        log.info("{}: Deactivating ProduceBenchWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
        this.status = null;
        this.doneFuture = null;
    }
}
