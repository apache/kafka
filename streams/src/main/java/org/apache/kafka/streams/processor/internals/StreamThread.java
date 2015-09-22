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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static AtomicInteger nextThreadNumber = new AtomicInteger(1);

    private final AtomicBoolean running;

    protected final StreamingConfig config;
    protected final TopologyBuilder builder;
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;

    private final Map<Integer, StreamTask> tasks;
    private final Time time;
    private final File stateDir;
    private final long pollTimeMs;
    private final long cleanTimeMs;
    private final long commitTimeMs;
    private final long totalRecordsToProcess;
    private final StreamingMetrics metrics;

    private long lastClean;
    private long lastCommit;
    private long recordsProcessed;

    final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            addPartitions(assignment);
            lastClean = time.milliseconds(); // start the cleaning cycle
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            commitAll();
            removePartitions();
            lastClean = Long.MAX_VALUE; // stop the cleaning cycle until partitions are assigned
        }
    };

    public StreamThread(TopologyBuilder builder, StreamingConfig config) throws Exception {
        this(builder, config, null , null, new SystemTime());
    }

    @SuppressWarnings("unchecked")
    StreamThread(TopologyBuilder builder, StreamingConfig config,
                 Producer<byte[], byte[]> producer,
                 Consumer<byte[], byte[]> consumer,
                 Time time) throws Exception {
        super("StreamThread-" + nextThreadNumber.getAndIncrement());

        this.config = config;
        this.builder = builder;

        // set the producer and consumer clients
        this.producer = (producer != null) ? producer : createProducer();
        this.consumer = (consumer != null) ? consumer : createConsumer();

        // initialize the task list
        this.tasks = new HashMap<>();

        // read in task specific config values
        this.stateDir = new File(this.config.getString(StreamingConfig.STATE_DIR_CONFIG));
        this.stateDir.mkdir();
        this.pollTimeMs = config.getLong(StreamingConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamingConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.cleanTimeMs = config.getLong(StreamingConfig.STATE_CLEANUP_DELAY_MS_CONFIG);
        this.totalRecordsToProcess = config.getLong(StreamingConfig.TOTAL_RECORDS_TO_PROCESS);

        this.lastClean = Long.MAX_VALUE; // the cleaning cycle won't start until partition assignment
        this.lastCommit = time.milliseconds();
        this.recordsProcessed = 0;
        this.time = time;

        this.metrics = new StreamingMetrics();

        this.running = new AtomicBoolean(true);
    }

    private Producer<byte[], byte[]> createProducer() {
        log.info("Creating producer client for stream thread [" + this.getName() + "]");
        return new KafkaProducer<>(config.getProducerConfigs(),
                new ByteArraySerializer(),
                new ByteArraySerializer());
    }

    private Consumer<byte[], byte[]> createConsumer() {
        log.info("Creating consumer client for stream thread [" + this.getName() + "]");
        return new KafkaConsumer<>(config.getConsumerConfigs(),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
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

    public Map<Integer, StreamTask> tasks() {
        return Collections.unmodifiableMap(tasks);
    }

    private void shutdown() {
        log.info("Shutting down stream thread [" + this.getName() + "]");

        // Exceptions should not prevent this call from going through all shutdown steps.
        try {
            commitAll();
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

            consumer.subscribe(new ArrayList<>(builder.sourceTopics()), rebalanceListener);

            while (stillRunning()) {
                long startPoll = time.milliseconds();

                // try to fetch some records if necessary
                ConsumerRecords<byte[], byte[]> records = consumer.poll(totalNumBuffered == 0 ? this.pollTimeMs : 0);

                for (StreamTask task : tasks.values()) {
                    for (TopicPartition partition : task.partitions()) {
                        task.addRecords(partition, records.records(partition));
                    }
                }

                long endPoll = time.milliseconds();
                metrics.pollTimeSensor.record(endPoll - startPoll);

                // try to process one record from each task
                totalNumBuffered = 0;

                for (StreamTask task : tasks.values()) {
                    long startProcess = time.milliseconds();

                    totalNumBuffered += task.process();

                    metrics.processTimeSensor.record(time.milliseconds() - startProcess);
                }

                maybePunctuate();
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

    private void maybePunctuate() {
        for (StreamTask task : tasks.values()) {
            try {
                long now = time.milliseconds();

                if (task.maybePunctuate(now))
                    metrics.punctuateTimeSensor.record(time.milliseconds() - now);

            } catch (Exception e) {
                log.error("Failed to commit task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
        }
    }

    protected void maybeCommit() {
        long now = time.milliseconds();

        if (commitTimeMs >= 0 && lastCommit + commitTimeMs < now) {
            log.trace("Committing processor instances because the commit interval has elapsed.");

            commitAll();
            lastCommit = now;
        } else {
            for (StreamTask task : tasks.values()) {
                try {
                    if (task.commitNeeded())
                        commitOne(task, time.milliseconds());
                } catch (Exception e) {
                    log.error("Failed to commit task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                    throw e;
                }
            }
        }
    }

    /**
     * Commit the states of all its tasks
     */
    private void commitAll() {
        for (StreamTask task : tasks.values()) {
            try {
                commitOne(task, time.milliseconds());
            } catch (Exception e) {
                log.error("Failed to commit task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
        }
    }

    /**
     * Commit the state of a task
     */
    private void commitOne(StreamTask task, long now) {
        try {
            task.commit();
        } catch (Exception e) {
            log.error("Failed to commit task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
            throw e;
        }

        metrics.commitTimeSensor.record(time.milliseconds() - now);
    }

    /**
     * Cleanup any states of the tasks that have been removed from this thread
     */
    protected void maybeClean() {
        long now = time.milliseconds();

        if (now > lastClean + cleanTimeMs) {
            File[] stateDirs = stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir : stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());

                        // try to acquire the exclusive lock on the state directory
                        FileLock directoryLock = null;
                        try {
                            directoryLock = ProcessorStateManager.lockStateDirectory(dir);
                            if (directoryLock != null) {
                                log.info("Deleting obsolete state directory {} after delayed {} ms.", dir.getAbsolutePath(), cleanTimeMs);
                                Utils.delete(dir);
                            }
                        } catch (IOException e) {
                            log.error("Failed to lock the state directory due to an unexpected exception", e);
                        } finally {
                            if (directoryLock != null) {
                                try {
                                    directoryLock.release();
                                } catch (IOException e) {
                                    log.error("Failed to release the state directory lock");
                                }
                            }
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

    protected StreamTask createStreamTask(int id, Collection<TopicPartition> partitionsForTask) {
        metrics.taskCreationSensor.record();

        return new StreamTask(id, consumer, producer, partitionsForTask, builder.build(), config);
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
                    task = createStreamTask(id, partitionsForTask);
                } catch (Exception e) {
                    log.error("Failed to create a task #" + id + " in thread [" + this.getName() + "]: ", e);
                    throw e;
                }
                tasks.put(id, task);
            }
        }

        lastClean = time.milliseconds();
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
            metrics.taskDestructionSensor.record();
        }
        tasks.clear();
    }

    private class StreamingMetrics {
        final Metrics metrics;

        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreationSensor;
        final Sensor taskDestructionSensor;

        public StreamingMetrics() {
            String metricGrpName = "streaming-metrics";

            this.metrics = new Metrics();
            Map<String, String> metricTags = new LinkedHashMap<String, String>();
            metricTags.put("client-id", config.getString(StreamingConfig.CLIENT_ID_CONFIG) + "-" + getName());

            this.commitTimeSensor = metrics.sensor("commit-time");
            this.commitTimeSensor.add(new MetricName("commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.commitTimeSensor.add(new MetricName("commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.commitTimeSensor.add(new MetricName("commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.pollTimeSensor = metrics.sensor("poll-time");
            this.pollTimeSensor.add(new MetricName("poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
            this.pollTimeSensor.add(new MetricName("poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
            this.pollTimeSensor.add(new MetricName("poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

            this.processTimeSensor = metrics.sensor("process-time");
            this.processTimeSensor.add(new MetricName("process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
            this.processTimeSensor.add(new MetricName("process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
            this.processTimeSensor.add(new MetricName("process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

            this.punctuateTimeSensor = metrics.sensor("punctuate-time");
            this.punctuateTimeSensor.add(new MetricName("punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
            this.punctuateTimeSensor.add(new MetricName("punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
            this.punctuateTimeSensor.add(new MetricName("punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

            this.taskCreationSensor = metrics.sensor("task-creation");
            this.taskCreationSensor.add(new MetricName("task-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

            this.taskDestructionSensor = metrics.sensor("task-destruction");
            this.taskDestructionSensor.add(new MetricName("task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));
        }
    }
}
