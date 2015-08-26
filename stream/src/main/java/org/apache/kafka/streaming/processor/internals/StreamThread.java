/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streaming.StreamingConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streaming.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);

    private final TopologyBuilder builder;
    private final RecordCollector collector;
    private final Consumer<byte[], byte[]> consumer;
    private final Map<Integer, StreamTask> tasks = new HashMap<>();
    private final Metrics metrics;
    private final Time time;

    private final StreamingConfig config;
    private final File stateDir;
    private final long pollTimeMs;
    private final long commitTimeMs;
    private final long stateCleanupDelayMs;
    private final long totalRecordsToProcess;
    private final KafkaStreamingMetrics streamingMetrics;

    private volatile boolean running;
    private long lastCommit;
    private long nextStateCleaning;
    private long recordsProcessed;

    protected final ConsumerRebalanceCallback rebalanceCallback = new ConsumerRebalanceCallback() {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            addPartitions(assignment);
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            commitAll(time.milliseconds());
            removePartitions();
        }
    };

    @SuppressWarnings("unchecked")
    public StreamThread(TopologyBuilder builder, StreamingConfig config) throws Exception {
        super();

        this.config = config;
        this.builder = builder;

        this.streamingMetrics = new KafkaStreamingMetrics();

        // create the producer and consumer clients
        Producer<byte[], byte[]> producer = new KafkaProducer<>(config.getProducerProperties(),
            new ByteArraySerializer(),
            new ByteArraySerializer());
        this.collector = new RecordCollector(producer,
            (Serializer<Object>) config.getConfiguredInstance(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serializer.class),
            (Serializer<Object>) config.getConfiguredInstance(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serializer.class));

        consumer = new KafkaConsumer<>(config.getConsumerProperties(),
            rebalanceCallback,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());

        this.stateDir = new File(this.config.getString(StreamingConfig.STATE_DIR_CONFIG));
        this.pollTimeMs = config.getLong(StreamingConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamingConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.stateCleanupDelayMs = config.getLong(StreamingConfig.STATE_CLEANUP_DELAY_MS_CONFIG);
        this.totalRecordsToProcess = config.getLong(StreamingConfig.TOTAL_RECORDS_TO_PROCESS);

        this.running = true;
        this.lastCommit = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();

        this.metrics = new Metrics();
    }

    /**
     * Execute the stream processors
     */
    @Override
    public synchronized void run() {
        log.info("Starting a stream thread");
        try {
            runLoop();
        } catch (RuntimeException e) {
            log.error("Uncaught error during processing: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        log.info("Shutting down a stream thread");
        commitAll(time.milliseconds());

        collector.close();
        consumer.close();
        removePartitions();
        log.info("Stream thread shutdown complete");
    }

    /**
     * Shutdown this streaming thread.
     */
    public synchronized void close() {
        running = false;
    }

    private void runLoop() {
        try {
            boolean readyForNextExecution = false;

            while (stillRunning()) {
                // try to fetch some records and put them to tasks' queues
                // TODO: we may not need to poll every iteration
                ConsumerRecords<byte[], byte[]> records = consumer.poll(readyForNextExecution ? 0 : this.pollTimeMs);

                for (StreamTask task : tasks.values()) {
                    for (TopicPartition partition : task.partitions()) {
                        task.addRecords(partition, records.records(partition).iterator());
                    }
                }

                // try to process one record from each task
                // TODO: we may want to process more than one record in each iteration
                for (StreamTask task : tasks.values()) {
                    readyForNextExecution = task.process();
                }

                maybeCommit();
                maybeCleanState();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean stillRunning() {
        if (!running) {
            log.debug("Shutting down at user request.");
            return false;
        }
        if (totalRecordsToProcess >= 0 && recordsProcessed >= totalRecordsToProcess) {
            log.debug("Shutting down as we've reached the user-configured limit of {} records to process.", totalRecordsToProcess);
            return false;
        }
        return true;
    }

    private void maybeCommit() {
        long now = time.milliseconds();
        if (commitTimeMs >= 0 && lastCommit + commitTimeMs < now) {
            log.trace("Committing processor instances because the commit interval has elapsed.");
            commitAll(now);
        }
    }

    private void commitAll(long now) {
        Map<TopicPartition, Long> commit = new HashMap<>();
        for (ProcessorContextImpl context : tasks.values()) {
            context.flush();
            commit.putAll(context.consumedOffsets());
        }

        // check if commit is really needed, i.e. if all the offsets are already committed
        if (consumer.commitNeeded(commit)) {
            // TODO: for exactly-once we need to make sure the flush and commit
            // are executed atomically whenever it is triggered by user
            collector.flush();
            consumer.commit(commit); // TODO: can this be async?
            streamingMetrics.commitTime.record(now - lastCommit);
        }
    }

    /* delete any state dirs that aren't for active contexts */
    private void maybeCleanState() {
        long now = time.milliseconds();
        if (now > nextStateCleaning) {
            File[] stateDirs = stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir : stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if (!tasks.keySet().contains(id)) {
                            log.info("Deleting obsolete state directory {} after {} delay ms.", dir.getAbsolutePath(), stateCleanupDelayMs);
                            Utils.rm(dir);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Deleting unknown directory in state directory {}.", dir.getAbsolutePath());
                        Utils.rm(dir);
                    }
                }
            }
            nextStateCleaning = Long.MAX_VALUE;
        }
    }

    private void addPartitions(Collection<TopicPartition> assignment) {
        HashSet<TopicPartition> partitions = new HashSet<>(assignment);

        // TODO: change this hard-coded co-partitioning behavior
        for (TopicPartition partition : partitions) {
            final Integer id = partition.partition();
            StreamTask task = tasks.get(id);
            if (task == null) {
                // get the partitions for the task
                HashSet<TopicPartition> partitionsForTask = new HashSet<>();
                for (TopicPartition part : partitions)
                    if (part.partition() == id)
                        partitionsForTask.add(part);

                // create the task
                task = new StreamTask(id, consumer, builder.build(), partitionsForTask, collector, config);

                tasks.put(id, task);
            }
        }

        nextStateCleaning = time.milliseconds() + stateCleanupDelayMs;
    }

    private void removePartitions() {

        // TODO: change this clearing tasks behavior
        for (StreamTask task : tasks.values()) {
            log.info("Removing task {}", task.id());
            try {
                task.close();
            } catch (Exception e) {
                throw new KafkaException(e);
            }
            streamingMetrics.processorDestruction.record();
        }
        tasks.clear();
    }

    private class KafkaStreamingMetrics {
        final Sensor commitTime;
        final Sensor processTime;
        final Sensor windowTime;
        final Sensor processorCreation;
        final Sensor processorDestruction;

        public KafkaStreamingMetrics() {
            String group = "kafka-streaming";

            this.commitTime = metrics.sensor("commit-time");
            this.commitTime.add(new MetricName(group, "commit-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "commits-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "commits-per-second"), new Rate(new Count()));

            this.processTime = metrics.sensor("process-time");
            this.processTime.add(new MetricName(group, "process-time-avg-ms"), new Avg());
            this.processTime.add(new MetricName(group, "process-time-max-ms"), new Max());
            this.processTime.add(new MetricName(group, "process-calls-per-second"), new Rate(new Count()));

            this.windowTime = metrics.sensor("window-time");
            this.windowTime.add(new MetricName(group, "window-time-avg-ms"), new Avg());
            this.windowTime.add(new MetricName(group, "window-time-max-ms"), new Max());
            this.windowTime.add(new MetricName(group, "window-calls-per-second"), new Rate(new Count()));

            this.processorCreation = metrics.sensor("processor-creation");
            this.processorCreation.add(new MetricName(group, "processor-creation"), new Rate(new Count()));

            this.processorDestruction = metrics.sensor("processor-destruction");
            this.processorDestruction.add(new MetricName(group, "processor-destruction"), new Rate(new Count()));

        }

    }

}
