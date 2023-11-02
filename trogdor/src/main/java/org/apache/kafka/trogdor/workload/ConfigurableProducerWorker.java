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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This workload allows for customized and even variable configurations in terms of messages per second, message size,
 * batch size, key size, and even the ability to target a specific partition out of a topic.
 *
 * See `ConfigurableProducerSpec` for a more detailed description.
 */

public class ConfigurableProducerWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ConfigurableProducerWorker.class);

    private final String id;

    private final ConfigurableProducerSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService executor;

    private WorkerStatusTracker status;

    private KafkaFutureImpl<String> doneFuture;

    public ConfigurableProducerWorker(String id, ConfigurableProducerSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ConfigurableProducerWorker is already running.");
        }
        log.info("{}: Activating ConfigurableProducerWorker with {}", id, spec);
        // Create an executor with 2 threads.  We need the second thread so
        // that the StatusUpdater can run in parallel with SendRecords.
        this.executor = Executors.newScheduledThreadPool(2,
            ThreadUtils.createThreadFactory("ConfigurableProducerWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
    }

    public class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                Map<String, NewTopic> newTopics = new HashMap<>();
                if (spec.activeTopic().materialize().size() != 1) {
                    throw new RuntimeException("Can only run against 1 topic.");
                }
                List<TopicPartition> active = new ArrayList<>();
                for (Map.Entry<String, PartitionsSpec> entry :
                        spec.activeTopic().materialize().entrySet()) {
                    String topicName = entry.getKey();
                    PartitionsSpec partSpec = entry.getValue();
                    newTopics.put(topicName, partSpec.newTopic(topicName));
                    for (Integer partitionNumber : partSpec.partitionNumbers()) {
                        active.add(new TopicPartition(topicName, partitionNumber));
                    }
                }
                status.update(new TextNode("Creating " + newTopics.keySet().size() + " topic(s)"));
                WorkerUtils.createTopics(log, spec.bootstrapServers(), spec.commonClientConf(),
                                         spec.adminClientConf(), newTopics, false);
                status.update(new TextNode("Created " + newTopics.keySet().size() + " topic(s)"));
                executor.submit(new SendRecords(active.get(0).topic(), spec.activePartition()));
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

    public class SendRecords implements Callable<Void> {
        private final String activeTopic;
        private final int activePartition;

        private final Histogram histogram;

        private final Future<?> statusUpdaterFuture;

        private final KafkaProducer<byte[], byte[]> producer;

        private final PayloadIterator keys;

        private final PayloadIterator values;

        private Future<RecordMetadata> sendFuture;

        SendRecords(String topic, int partition) {
            this.activeTopic = topic;
            this.activePartition = partition;
            this.histogram = new Histogram(10000);

            this.statusUpdaterFuture = executor.scheduleWithFixedDelay(
                new StatusUpdater(histogram), 30, 30, TimeUnit.SECONDS);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf());
            this.producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
            this.keys = new PayloadIterator(spec.keyGenerator());
            this.values = new PayloadIterator(spec.valueGenerator());
        }

        @Override
        public Void call() throws Exception {
            long startTimeMs = Time.SYSTEM.milliseconds();
            try {
                try {
                    long sentMessages = 0;
                    while (true) {
                        sendMessage();
                        sentMessages++;
                    }
                } catch (Exception e) {
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
                StatusData statusData = new StatusUpdater(histogram).update();
                long curTimeMs = Time.SYSTEM.milliseconds();
                log.info("Sent {} total record(s) in {} ms.  status: {}",
                    histogram.summarize().numSamples(), curTimeMs - startTimeMs, statusData);
            }
            doneFuture.complete("");
            return null;
        }

        private void sendMessage() throws InterruptedException {
            ProducerRecord<byte[], byte[]> record;
            if (activePartition != -1) {
                record = new ProducerRecord<>(activeTopic, activePartition, keys.next(), values.next());
            } else {
                record = new ProducerRecord<>(activeTopic, keys.next(), values.next());
            }
            sendFuture = producer.send(record, new SendRecordsCallback(this, Time.SYSTEM.milliseconds()));
            spec.flushGenerator().ifPresent(flushGenerator -> flushGenerator.increment(producer));
            spec.throughputGenerator().throttle();
        }

        void recordDuration(long durationMs) {
            histogram.add(durationMs);
        }
    }

    public class StatusUpdater implements Runnable {
        private final Histogram histogram;

        StatusUpdater(Histogram histogram) {
            this.histogram = histogram;
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
                summary.percentiles().get(2).value());
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
                   @JsonProperty("p99LatencyMs") int p99latencyMs) {
            this.totalSent = totalSent;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
        }

        @JsonProperty
        public long totalSent() {
            return totalSent;
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
            throw new IllegalStateException("ConfigurableProducerWorker is not running.");
        }
        log.info("{}: Deactivating ConfigurableProducerWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
        this.status = null;
        this.doneFuture = null;
    }
}
