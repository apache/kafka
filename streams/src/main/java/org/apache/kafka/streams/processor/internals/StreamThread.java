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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.StreamingMetrics;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static final AtomicInteger STREAMING_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    public final PartitionGrouper partitionGrouper;
    public final UUID clientUUID;

    protected final StreamingConfig config;
    protected final TopologyBuilder builder;
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    protected final Consumer<byte[], byte[]> restoreConsumer;

    private final AtomicBoolean running;
    private final Map<TaskId, StreamTask> activeTasks;
    private final Map<TaskId, StandbyTask> standbyTasks;
    private final Set<TaskId> prevTasks;
    private final String clientId;
    private final Time time;
    private final File stateDir;
    private final long pollTimeMs;
    private final long cleanTimeMs;
    private final long commitTimeMs;
    private final long totalRecordsToProcess;
    private final StreamingMetricsImpl sensors;

    private long lastClean;
    private long lastCommit;
    private long recordsProcessed;

    final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
            addStreamTasks(assignment);
            addStandbyTasks();
            lastClean = time.milliseconds(); // start the cleaning cycle
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> assignment) {
            commitAll();
            removeStreamTasks();
            removeStandbyTasks();
            lastClean = Long.MAX_VALUE; // stop the cleaning cycle until partitions are assigned
        }
    };

    public StreamThread(TopologyBuilder builder,
                        StreamingConfig config,
                        String clientId,
                        UUID clientUUID,
                        Metrics metrics,
                        Time time) throws Exception {
        this(builder, config, null , null, null, clientId, clientUUID, metrics, time);
    }

    StreamThread(TopologyBuilder builder,
                 StreamingConfig config,
                 Producer<byte[], byte[]> producer,
                 Consumer<byte[], byte[]> consumer,
                 Consumer<byte[], byte[]> restoreConsumer,
                 String clientId,
                 UUID clientUUID,
                 Metrics metrics,
                 Time time) throws Exception {
        super("StreamThread-" + STREAMING_THREAD_ID_SEQUENCE.getAndIncrement());

        this.config = config;
        this.builder = builder;
        this.clientId = clientId;
        this.clientUUID = clientUUID;
        this.partitionGrouper = config.getConfiguredInstance(StreamingConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);
        this.partitionGrouper.topicGroups(builder.topicGroups());

        // set the producer and consumer clients
        this.producer = (producer != null) ? producer : createProducer();
        this.consumer = (consumer != null) ? consumer : createConsumer();
        this.restoreConsumer = (restoreConsumer != null) ? restoreConsumer : createRestoreConsumer();

        // initialize the task list
        this.activeTasks = new HashMap<>();
        this.standbyTasks = new HashMap<>();
        this.prevTasks = new HashSet<>();

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

        this.sensors = new StreamingMetricsImpl(metrics);

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
        return new KafkaConsumer<>(config.getConsumerConfigs(this),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }

    private Consumer<byte[], byte[]> createRestoreConsumer() {
        log.info("Creating restore consumer client for stream thread [" + this.getName() + "]");
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

    public Map<TaskId, StreamTask> tasks() {
        return Collections.unmodifiableMap(activeTasks);
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
            restoreConsumer.close();
        } catch (Throwable e) {
            log.error("Failed to close restore consumer in thread [" + this.getName() + "]: ", e);
        }
        try {
            removeStreamTasks();
            removeStandbyTasks();
        } catch (Throwable e) {
            // already logged in removePartition()
        }

        log.info("Stream thread shutdown complete [" + this.getName() + "]");
    }

    private void runLoop() {
        try {
            int totalNumBuffered = 0;
            boolean requiresPoll = true;

            ensureCopartitioning(builder.copartitionGroups());

            consumer.subscribe(new ArrayList<>(builder.sourceTopics()), rebalanceListener);

            while (stillRunning()) {
                // try to fetch some records if necessary
                if (requiresPoll) {
                    long startPoll = time.milliseconds();

                    ConsumerRecords<byte[], byte[]> records = consumer.poll(totalNumBuffered == 0 ? this.pollTimeMs : 0);

                    if (!records.isEmpty()) {
                        for (StreamTask task : activeTasks.values()) {
                            for (TopicPartition partition : task.partitions()) {
                                task.addRecords(partition, records.records(partition));
                            }
                        }
                    }

                    long endPoll = time.milliseconds();
                    sensors.pollTimeSensor.record(endPoll - startPoll);
                }

                totalNumBuffered = 0;

                if (!activeTasks.isEmpty()) {
                    // try to process one record from each task
                    requiresPoll = false;

                    for (StreamTask task : activeTasks.values()) {
                        long startProcess = time.milliseconds();

                        totalNumBuffered += task.process();
                        requiresPoll = requiresPoll || task.requiresPoll();

                        sensors.processTimeSensor.record(time.milliseconds() - startProcess);
                    }

                    maybePunctuate();
                    maybeCommit();
                } else {
                    // even when no task is assigned, we must poll to get a task.
                    requiresPoll = true;
                }

                if (!standbyTasks.isEmpty()) {
                    updateStandbyTasks();
                }

                maybeClean();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private void updateStandbyTasks() {
        ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(0);

        if (!records.isEmpty()) {
            for (StandbyTask task : standbyTasks.values()) {
                for (TopicPartition partition : task.changeLogPartitions()) {
                    task.update(partition, records.records(partition));
                }
            }
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
        for (StreamTask task : activeTasks.values()) {
            try {
                long now = time.milliseconds();

                if (task.maybePunctuate(now))
                    sensors.punctuateTimeSensor.record(time.milliseconds() - now);

            } catch (Exception e) {
                log.error("Failed to commit active task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
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
            for (StreamTask task : activeTasks.values()) {
                try {
                    if (task.commitNeeded())
                        commitOne(task, time.milliseconds());
                } catch (Exception e) {
                    log.error("Failed to commit active task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
                    throw e;
                }
            }
        }
    }

    /**
     * Commit the states of all its tasks
     */
    private void commitAll() {
        for (StreamTask task : activeTasks.values()) {
            commitOne(task, time.milliseconds());
        }
        for (StandbyTask task : standbyTasks.values()) {
            commitOne(task, time.milliseconds());
        }
    }

    /**
     * Commit the state of a task
     */
    private void commitOne(AbstractTask task, long now) {
        try {
            task.commit();
        } catch (Exception e) {
            log.error("Failed to commit " + task.getClass().getSimpleName() + " #" + task.id() + " in thread [" + this.getName() + "]: ", e);
            throw e;
        }

        sensors.commitTimeSensor.record(time.milliseconds() - now);
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
                        TaskId id = TaskId.parse(dir.getName());

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
                    } catch (TaskId.TaskIdFormatException e) {
                        // there may be some unknown files that sits in the same directory,
                        // we should ignore these files instead trying to delete them as well
                    }
                }
            }

            lastClean = now;
        }
    }

    /**
     * Returns ids of tasks that were being executed before the rebalance.
     */
    public Set<TaskId> prevTasks() {
        return Collections.unmodifiableSet(prevTasks);
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage.
     */
    public Set<TaskId> cachedTasks() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        HashSet<TaskId> tasks = new HashSet<>();

        File[] stateDirs = stateDir.listFiles();
        if (stateDirs != null) {
            for (File dir : stateDirs) {
                try {
                    TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, ProcessorStateManager.CHECKPOINT_FILE_NAME).exists())
                        tasks.add(id);

                } catch (TaskId.TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitionsForTask) {
        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        return new StreamTask(id, consumer, producer, restoreConsumer, partitionsForTask, topology, config, sensors);
    }

    private void addStreamTasks(Collection<TopicPartition> assignment) {
        HashMap<TaskId, Set<TopicPartition>> partitionsForTask = new HashMap<>();

        for (TopicPartition partition : assignment) {
            Set<TaskId> taskIds = partitionGrouper.taskIds(partition);
            for (TaskId taskId : taskIds) {
                Set<TopicPartition> partitions = partitionsForTask.get(taskId);
                if (partitions == null) {
                    partitions = new HashSet<>();
                    partitionsForTask.put(taskId, partitions);
                }
                partitions.add(partition);
            }
        }

        // create the tasks
        for (TaskId taskId : partitionsForTask.keySet()) {
            try {
                activeTasks.put(taskId, createStreamTask(taskId, partitionsForTask.get(taskId)));
            } catch (Exception e) {
                log.error("Failed to create an active task #" + taskId + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
        }

        lastClean = time.milliseconds();
    }

    private void removeStreamTasks() {
        // TODO: change this clearing tasks behavior
        for (StreamTask task : activeTasks.values()) {
            closeOne(task);
        }

        prevTasks.clear();
        prevTasks.addAll(activeTasks.keySet());

        activeTasks.clear();
    }

    private void closeOne(AbstractTask task) {
        log.info("Removing a task {}", task.id());
        try {
            task.close();
        } catch (Exception e) {
            log.error("Failed to close a " + task.getClass().getSimpleName() + " #" + task.id() + " in thread [" + this.getName() + "]: ", e);
            throw e;
        }
        sensors.taskDestructionSensor.record();
    }

    protected StandbyTask createStandbyTask(TaskId id) {
        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        if (!topology.stateStoreSuppliers().isEmpty()) {
            return new StandbyTask(id, restoreConsumer, topology, config, sensors);
        } else {
            return null;
        }
    }

    private void addStandbyTasks() {
        Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();

        for (TaskId taskId : partitionGrouper.standbyTasks()) {
            StandbyTask task = createStandbyTask(taskId);
            if (task != null) {
                standbyTasks.put(taskId, task);
                checkpointedOffsets.putAll(task.checkpointedOffsets());
            }
        }

        restoreConsumer.assign(new ArrayList<>(checkpointedOffsets.keySet()));

        for (Map.Entry<TopicPartition, Long> entry : checkpointedOffsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue();
            if (offset >= 0) {
                restoreConsumer.seek(partition, offset);
            } else {
                restoreConsumer.seekToBeginning(partition);
            }
        }
    }


    private void removeStandbyTasks() {
        for (StandbyTask task : standbyTasks.values()) {
            closeOne(task);
        }
        // un-assign the change log partitions
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());

        standbyTasks.clear();
    }

    private void ensureCopartitioning(Collection<Set<String>> copartitionGroups) {
        for (Set<String> copartitionGroup : copartitionGroups) {
            ensureCopartitioning(copartitionGroup);
        }
    }

    private void ensureCopartitioning(Set<String> copartitionGroup) {
        int numPartitions = -1;

        for (String topic : copartitionGroup) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);

            if (infos == null)
                throw new KafkaException("topic not found: " + topic);

            if (numPartitions == -1) {
                numPartitions = infos.size();
            } else if (numPartitions != infos.size()) {
                String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                Arrays.sort(topics);
                throw new KafkaException("topics not copartitioned: [" + Utils.mkString(Arrays.asList(topics), ",") + "]");
            }
        }
    }

    private class StreamingMetricsImpl implements StreamingMetrics {
        final Metrics metrics;
        final String metricGrpName;
        final Map<String, String> metricTags;

        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreationSensor;
        final Sensor taskDestructionSensor;

        public StreamingMetricsImpl(Metrics metrics) {

            this.metrics = metrics;
            this.metricGrpName = "streaming-metrics";
            this.metricTags = new LinkedHashMap<>();
            this.metricTags.put("client-id", clientId + "-" + getName());

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

        @Override
        public void recordLatency(Sensor sensor, long startNs, long endNs) {
            sensor.record((endNs - startNs) / 1000000, endNs);
        }

        @Override
        public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
            // extract the additional tags if there are any
            Map<String, String> tagMap = new HashMap<>(this.metricTags);
            if ((tags.length % 2) != 0)
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

            for (int i = 0; i < tags.length; i += 2)
                tagMap.put(tags[i], tags[i + 1]);

            // first add the global operation metrics if not yet, with the global tags only
            Sensor parent = metrics.sensor(operationName);
            addLatencyMetrics(this.metricGrpName, parent, "all", operationName, this.metricTags);

            // add the store operation metrics with additional tags
            Sensor sensor = metrics.sensor(entityName + "-" + operationName, parent);
            addLatencyMetrics("streaming-" + scopeName + "-metrics", sensor, entityName, operationName, tagMap);

            return sensor;
        }

        private void addLatencyMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
            maybeAddMetric(sensor, new MetricName(opName + "-avg-latency-ms", metricGrpName,
                "The average latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Avg());
            maybeAddMetric(sensor, new MetricName(opName + "-max-latency-ms", metricGrpName,
                "The max latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Max());
            maybeAddMetric(sensor, new MetricName(opName + "-qps", metricGrpName,
                "The average number of occurrence of " + entityName + " " + opName + " operation per second.", tags), new Rate(new Count()));
        }

        private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
            if (!metrics.metrics().containsKey(name))
                sensor.add(name, stat);
        }
    }
}
