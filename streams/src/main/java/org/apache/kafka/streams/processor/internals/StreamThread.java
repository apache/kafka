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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.StreamingConfig;
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
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static AtomicInteger nextThreadNumber = new AtomicInteger(1);

    private final AtomicBoolean running;

    private final TopologyBuilder builder;
    private final Producer<byte[], byte[]> producer;
    private final Consumer<byte[], byte[]> consumer;
    private final Map<Integer, StreamTask> tasks;
    private final Time time;

    private final File stateDir;
    private final long pollTimeMs;
    private final long cleanTimeMs;
    private final long commitTimeMs;
    private final long totalRecordsToProcess;
    private final KafkaStreamingMetrics metrics;
    private final StreamingConfig config;

    private long lastClean;
    private long lastCommit;
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
        super("StreamThread-" + nextThreadNumber.getAndIncrement());

        this.config = config;
        this.builder = builder;

        // create the producer and consumer clients
        log.info("Creating producer client for stream thread [" + this.getName() + "]");

        this.producer = new KafkaProducer<>(config.getProducerConfigs(),
            new ByteArraySerializer(),
            new ByteArraySerializer());

        log.info("Creating consumer client for stream thread [" + this.getName() + "]");

        this.consumer = new KafkaConsumer<>(config.getConsumerConfigs(),
            rebalanceCallback,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());

        // initialize the task list
        this.tasks = new HashMap<>();

        // read in task specific config values
        this.stateDir = new File(this.config.getString(StreamingConfig.STATE_DIR_CONFIG));
        this.stateDir.mkdir();
        this.pollTimeMs = config.getLong(StreamingConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamingConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.cleanTimeMs = config.getLong(StreamingConfig.STATE_CLEANUP_DELAY_MS_CONFIG);
        this.totalRecordsToProcess = config.getLong(StreamingConfig.TOTAL_RECORDS_TO_PROCESS);

        this.lastClean = 0;
        this.lastCommit = 0;
        this.recordsProcessed = 0;
        this.time = new SystemTime();

        this.metrics = new KafkaStreamingMetrics();

        this.running = new AtomicBoolean(true);
    }

    /**
     * Execute the stream processors
     */
    @Override
    public void run() {
        log.info("Starting stream thread [" + this.getName() + "]");

        try {
            runLoop();
        } catch (RuntimeException e) {
            log.error("Uncaught error during processing in thread [" + this.getName() + "]: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    /**
     * Shutdown this streaming thread.
     */
    public void close() {
        running.set(false);
    }

    private void shutdown() {
        log.info("Shutting down stream thread [" + this.getName() + "]");

        // Exceptions should not prevent this call from going through all shutdown steps.
        try {
            commitAll(time.milliseconds());
        } catch (Throwable e) {
            // already logged in commitAll()
        }
        try {
            producer.close();
        } catch (Throwable e) {
            log.error("Failed to close producer in thread [" + this.getName() + "]: ", e);
        }
        try {
            consumer.close();
        } catch (Throwable e) {
            log.error("Failed to close consumer in thread [" + this.getName() + "]: ", e);
        }
        try {
            removePartitions();
        } catch (Throwable e) {
            // already logged in removePartition()
        }

        log.info("Stream thread shutdown complete [" + this.getName() + "]");
    }

    private void runLoop() {
        try {
            int totalNumBuffered = 0;

            for (String topic : builder.sourceTopics()) {
                consumer.subscribe(topic);
            }

            while (stillRunning()) {
                // try to fetch some records if necessary
                ConsumerRecords<byte[], byte[]> records = consumer.poll(totalNumBuffered == 0 ? this.pollTimeMs : 0);

                for (StreamTask task : tasks.values()) {
                    for (TopicPartition partition : task.partitions()) {
                        task.addRecords(partition, records.records(partition));
                    }
                }

                // try to process one record from each task
                totalNumBuffered = 0;

                for (StreamTask task : tasks.values()) {
                    totalNumBuffered += task.process();
                }

                maybeClean();
                maybeCommit();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean stillRunning() {
        if (!running.get()) {
            log.debug("Shutting down at user request.");
            return false;
        }

        if (totalRecordsToProcess >= 0 && recordsProcessed >= totalRecordsToProcess) {
            log.debug("Shutting down as we've reached the user configured limit of {} records to process.", totalRecordsToProcess);
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

    /**
     * Commit the states of all its tasks
     * @param now
     */
    private void commitAll(long now) {
        for (StreamTask task : tasks.values()) {
            try {
                task.commit();
            } catch (Exception e) {
                log.error("Failed to commit task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
        }

        metrics.commitTime.record(now - time.milliseconds());

        lastCommit = now;
    }

    /**
     * Cleanup any states of the tasks that have been removed from this thread
     */
    private void maybeClean() {
        long now = time.milliseconds();

        if (now > lastClean) {
            File[] stateDirs = stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir : stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if (!tasks.containsKey(id)) {
                            log.info("Deleting obsolete state directory {} after delayed {} ms.", dir.getAbsolutePath(), cleanTimeMs);
                            Utils.rm(dir);
                        }
                    } catch (NumberFormatException e) {
                        // there may be some unknown files that sits in the same directory,
                        // we should ignore these files instead trying to delete them as well
                    }
                }
            }

            lastClean = now;
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
                try {
                    task = new StreamTask(id, consumer, producer, partitionsForTask, builder.build(), config);
                } catch (Exception e) {
                    log.error("Failed to create a task #" + id + " in thread [" + this.getName() + "]: ", e);
                    throw e;
                }
                tasks.put(id, task);
            }
        }

        lastClean = time.milliseconds() + cleanTimeMs;
    }

    private void removePartitions() {

        // TODO: change this clearing tasks behavior
        for (StreamTask task : tasks.values()) {
            log.info("Removing task {}", task.id());
            try {
                task.close();
            } catch (Exception e) {
                log.error("Failed to close a task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
            metrics.processorDestruction.record();
        }
        tasks.clear();
    }

    private class KafkaStreamingMetrics {
        final Metrics metrics;

        final Sensor commitTime;
        final Sensor processTime;
        final Sensor windowTime;
        final Sensor processorCreation;
        final Sensor processorDestruction;

        public KafkaStreamingMetrics() {
            String group = "kafka-streaming";

            this.metrics = new Metrics();

            this.commitTime = metrics.sensor("commit-time");
            this.commitTime.add(new MetricName(group, "commit-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "commit-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "commit-per-second"), new Rate(new Count()));

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
