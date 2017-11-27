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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ProduceBenchWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ProduceBenchWorker.class);

    private static final short REPLICATION_FACTOR = 3;

    private static final int MESSAGE_SIZE = 512;

    private final String id;

    private final ProduceBenchSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService executor;

    private AtomicReference<String> status;

    private KafkaFutureImpl<String> doneFuture;

    public ProduceBenchWorker(String id, ProduceBenchSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, AtomicReference<String> status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ProducerBenchWorker is already running.");
        }
        log.info("{}: Activating ProduceBenchWorker.", id);
        this.executor = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("ProduceBenchWorkerThread%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        executor.submit(new ValidateSpec());
    }

    private static String topicIndexToName(int topicIndex) {
        return String.format("topic%05d", topicIndex);
    }

    private void abort(String what, Exception e) throws KafkaException {
        log.warn(what + " caught an exception: ", e);
        doneFuture.completeExceptionally(new KafkaException(what + " caught an exception.", e));
        throw new KafkaException(e);
    }

    public class ValidateSpec implements Callable<Void> {
        @Override
        public Void call() throws Exception {
            try {
                if (spec.activeTopics() == 0) {
                    throw new ConfigException("Can't have activeTopics == 0.");
                }
                if (spec.totalTopics() < spec.activeTopics()) {
                    throw new ConfigException(String.format(
                        "activeTopics was %d, but totalTopics was only %d.  activeTopics must " +
                            "be less than or equal to totalTopics.", spec.activeTopics(), spec.totalTopics()));
                }
                executor.submit(new CreateBenchmarkTopics());
            } catch (Exception e) {
                abort("ValidateSpec", e);
            }
            return null;
        }
    }

    public class CreateBenchmarkTopics implements Callable<Void> {
        private final static int MAX_BATCH_SIZE = 10;

        @Override
        public Void call() throws Exception {
            try {
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
                try (AdminClient adminClient = AdminClient.create(props)) {
                    List<String> topicsToCreate = new ArrayList<>();
                    for (int i = 0; i < spec.totalTopics(); i++) {
                        topicsToCreate.add(topicIndexToName(i));
                    }
                    log.info("Creating " + spec.totalTopics() + " topics...");
                    List<Future<Void>> futures = new ArrayList<>();
                    while (!topicsToCreate.isEmpty()) {
                        List<NewTopic> newTopics = new ArrayList<>();
                        for (int i = 0; (i < MAX_BATCH_SIZE) && !topicsToCreate.isEmpty(); i++) {
                            String topic = topicsToCreate.remove(0);
                            newTopics.add(new NewTopic(topic, 1, REPLICATION_FACTOR));
                        }
                        futures.add(adminClient.createTopics(newTopics).all());
                    }
                    for (Future<Void> future : futures) {
                        future.get();
                    }
                    log.info("Successfully created " + spec.totalTopics() + " topics.");
                }
                executor.submit(new SendRecords());
            } catch (Exception e) {
                abort("CreateBenchmarkTopics", e);
            }
            return null;
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
        private final Histogram histogram;

        private final Throttle throttle;

        private final Future<?> statusUpdaterFuture;

        SendRecords() {
            this.histogram = new Histogram(5000);
            this.throttle = new Throttle(spec.targetMessagesPerSec());
            this.statusUpdaterFuture = executor.scheduleWithFixedDelay(
                new StatusUpdater(histogram), 1, 1, TimeUnit.MINUTES);
        }

        @Override
        public Void call() throws Exception {
            long startTimeMs = Time.SYSTEM.milliseconds();
            try {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
                for (Map.Entry<String, String> entry : spec.producerConf().entrySet()) {
                    props.setProperty(entry.getKey(), entry.getValue());
                }
                byte[] key = new byte[MESSAGE_SIZE];
                byte[] value = new byte[MESSAGE_SIZE];
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                            props, new ByteArraySerializer(), new ByteArraySerializer());
                Future<RecordMetadata> future = null;
                try {
                    for (int m = 0; m < spec.maxMessages(); m++) {
                        for (int i = 0; i < spec.activeTopics(); i++) {
                            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicIndexToName(i), key, value);
                            future = producer.send(record, new SendRecordsCallback(this, Time.SYSTEM.milliseconds()));
                        }
                        throttle.increment();
                    }
                } finally {
                    if (future != null) {
                        future.get();
                    }
                    producer.close();
                }
            } catch (Exception e) {
                abort("SendRecords", e);
            } finally {
                statusUpdaterFuture.cancel(false);
                new StatusUpdater(histogram).run();
                long curTimeMs = Time.SYSTEM.milliseconds();
                log.info("Sent {} total record(s) in {} ms.  status: {}",
                    histogram.summarize().numSamples(), curTimeMs - startTimeMs, status.get());
            }
            doneFuture.complete("");
            return null;
        }

        void recordDuration(long durationMs) {
            histogram.add(durationMs);
        }
    }

    public class StatusUpdater implements Runnable {
        private final Histogram histogram;
        private final float[] percentiles;

        StatusUpdater(Histogram histogram) {
            this.histogram = histogram;
            this.percentiles = new float[3];
            this.percentiles[0] = 0.50f;
            this.percentiles[1] = 0.95f;
            this.percentiles[2] = 0.99f;
        }

        @Override
        public void run() {
            try {
                Histogram.Summary summary = histogram.summarize(percentiles);
                StatusData statusData = new StatusData(summary.numSamples(), summary.average(),
                    summary.percentiles().get(0).value(),
                    summary.percentiles().get(1).value(),
                    summary.percentiles().get(2).value());
                String statusDataString = JsonUtil.toJsonString(statusData);
                status.set(statusDataString);
            } catch (Exception e) {
                abort("StatusUpdater", e);
            }
        }
    }

    public static class StatusData {
        private final long totalSent;
        private final float averageLatencyMs;
        private final int p50LatencyMs;
        private final int p90LatencyMs;
        private final int p99LatencyMs;

        @JsonCreator
        StatusData(@JsonProperty("totalSent") long totalSent,
                   @JsonProperty("averageLatencyMs") float averageLatencyMs,
                   @JsonProperty("p50LatencyMs") int p50latencyMs,
                   @JsonProperty("p90LatencyMs") int p90latencyMs,
                   @JsonProperty("p99LatencyMs") int p99latencyMs) {
            this.totalSent = totalSent;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p90LatencyMs = p90latencyMs;
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
        public int p90LatencyMs() {
            return p90LatencyMs;
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
