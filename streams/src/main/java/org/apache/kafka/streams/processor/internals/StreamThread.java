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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
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
    private static final AtomicInteger STREAM_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    public final PartitionGrouper partitionGrouper;
    public final String jobId;
    public final String clientId;
    public final UUID processId;

    protected final StreamsConfig config;
    protected final TopologyBuilder builder;
    protected final Set<String> sourceTopics;
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    protected final Consumer<byte[], byte[]> restoreConsumer;

    private final AtomicBoolean running;
    private final Map<TaskId, StreamTask> activeTasks;
    private final Map<TaskId, StandbyTask> standbyTasks;
    private final Map<TopicPartition, StreamTask> activeTasksByPartition;
    private final Map<TopicPartition, StandbyTask> standbyTasksByPartition;
    private final Set<TaskId> prevTasks;
    private final Time time;
    private final File stateDir;
    private final long pollTimeMs;
    private final long cleanTimeMs;
    private final long commitTimeMs;
    private final long totalRecordsToProcess;
    private final StreamsMetricsImpl sensors;

    private StreamPartitionAssignor partitionAssignor = null;

    private long lastClean;
    private long lastCommit;
    private long recordsProcessed;
    private Throwable rebalanceException = null;

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords;
    private boolean processStandbyRecords = false;

    static File makeStateDir(String jobId, String baseDirName) {
        File baseDir = new File(baseDirName);
        if (!baseDir.exists())
            baseDir.mkdir();

        File stateDir = new File(baseDir, jobId);
        if (!stateDir.exists())
            stateDir.mkdir();

        return stateDir;
    }

    final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
            try {
                addStreamTasks(assignment);
                addStandbyTasks();
                lastClean = time.milliseconds(); // start the cleaning cycle
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> assignment) {
            try {
                commitAll();
                lastClean = Long.MAX_VALUE; // stop the cleaning cycle until partitions are assigned
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            } finally {
                // TODO: right now upon partition revocation, we always remove all the tasks;
                // this behavior can be optimized to only remove affected tasks in the future
                removeStreamTasks();
                removeStandbyTasks();
            }
        }
    };

    public StreamThread(TopologyBuilder builder,
                        StreamsConfig config,
                        String jobId,
                        String clientId,
                        UUID processId,
                        Metrics metrics,
                        Time time) {
        this(builder, config, null , null, null, jobId, clientId, processId, metrics, time);
    }

    StreamThread(TopologyBuilder builder,
                 StreamsConfig config,
                 Producer<byte[], byte[]> producer,
                 Consumer<byte[], byte[]> consumer,
                 Consumer<byte[], byte[]> restoreConsumer,
                 String jobId,
                 String clientId,
                 UUID processId,
                 Metrics metrics,
                 Time time) {
        super("StreamThread-" + STREAM_THREAD_ID_SEQUENCE.getAndIncrement());

        this.jobId = jobId;
        this.config = config;
        this.builder = builder;
        this.sourceTopics = builder.sourceTopics();
        this.clientId = clientId;
        this.processId = processId;
        this.partitionGrouper = config.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);

        // set the producer and consumer clients
        this.producer = (producer != null) ? producer : createProducer();
        this.consumer = (consumer != null) ? consumer : createConsumer();
        this.restoreConsumer = (restoreConsumer != null) ? restoreConsumer : createRestoreConsumer();

        // initialize the task list
        this.activeTasks = new HashMap<>();
        this.standbyTasks = new HashMap<>();
        this.activeTasksByPartition = new HashMap<>();
        this.standbyTasksByPartition = new HashMap<>();
        this.prevTasks = new HashSet<>();

        // standby ktables
        this.standbyRecords = new HashMap<>();

        // read in task specific config values
        this.stateDir = makeStateDir(this.jobId, this.config.getString(StreamsConfig.STATE_DIR_CONFIG));
        this.pollTimeMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.cleanTimeMs = config.getLong(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG);
        this.totalRecordsToProcess = config.getLong(StreamsConfig.TOTAL_RECORDS_TO_PROCESS);

        this.lastClean = Long.MAX_VALUE; // the cleaning cycle won't start until partition assignment
        this.lastCommit = time.milliseconds();
        this.recordsProcessed = 0;
        this.time = time;

        this.sensors = new StreamsMetricsImpl(metrics);

        this.running = new AtomicBoolean(true);
    }

    public void partitionAssignor(StreamPartitionAssignor partitionAssignor) {
        this.partitionAssignor = partitionAssignor;
    }

    private Producer<byte[], byte[]> createProducer() {
        log.info("Creating producer client for stream thread [" + this.getName() + "]");
        return new KafkaProducer<>(config.getProducerConfigs(this.clientId),
                new ByteArraySerializer(),
                new ByteArraySerializer());
    }

    private Consumer<byte[], byte[]> createConsumer() {
        log.info("Creating consumer client for stream thread [" + this.getName() + "]");
        return new KafkaConsumer<>(config.getConsumerConfigs(this, this.jobId, this.clientId),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }

    private Consumer<byte[], byte[]> createRestoreConsumer() {
        log.info("Creating restore consumer client for stream thread [" + this.getName() + "]");
        return new KafkaConsumer<>(config.getRestoreConsumerConfigs(this.clientId),
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
        } catch (KafkaException e) {
            // just re-throw the exception as it should be logged already
            throw e;
        } catch (Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("Streams application error during processing in thread [" + this.getName() + "]: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    /**
     * Shutdown this stream thread.
     */
    public void close() {
        running.set(false);
    }

    public Map<TaskId, StreamTask> tasks() {
        return Collections.unmodifiableMap(activeTasks);
    }

    private void shutdown() {
        log.info("Shutting down stream thread [" + this.getName() + "]");

        // Exceptions should not prevent this call from going through all shutdown steps
        try {
            commitAll();
        } catch (Throwable e) {
            // already logged in commitAll()
        }

        // Close standby tasks before closing the restore consumer since closing standby tasks uses the restore consumer.
        removeStandbyTasks();

        // We need to first close the underlying clients before closing the state
        // manager, for example we need to make sure producer's message sends
        // have all been acked before the state manager records
        // changelog sent offsets
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

        removeStreamTasks();

        log.info("Stream thread shutdown complete [" + this.getName() + "]");
    }

    private void runLoop() {
        int totalNumBuffered = 0;
        long lastPoll = 0L;
        boolean requiresPoll = true;

        ensureCopartitioning(builder.copartitionGroups());

        consumer.subscribe(new ArrayList<>(sourceTopics), rebalanceListener);

        while (stillRunning()) {
            // try to fetch some records if necessary
            if (requiresPoll) {
                requiresPoll = false;

                long startPoll = time.milliseconds();

                ConsumerRecords<byte[], byte[]> records = consumer.poll(totalNumBuffered == 0 ? this.pollTimeMs : 0);
                lastPoll = time.milliseconds();

                if (rebalanceException != null)
                    throw new StreamsException("Failed to rebalance", rebalanceException);

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        StreamTask task = activeTasksByPartition.get(partition);
                        task.addRecords(partition, records.records(partition));
                    }
                }

                long endPoll = time.milliseconds();
                sensors.pollTimeSensor.record(endPoll - startPoll);
            }

            totalNumBuffered = 0;

            if (!activeTasks.isEmpty()) {
                // try to process one record from each task
                for (StreamTask task : activeTasks.values()) {
                    long startProcess = time.milliseconds();

                    totalNumBuffered += task.process();
                    requiresPoll = requiresPoll || task.requiresPoll();

                    sensors.processTimeSensor.record(time.milliseconds() - startProcess);
                }

                maybePunctuate();

                // if pollTimeMs has passed since the last poll, we poll to respond to a possible rebalance
                // even when we paused all partitions.
                if (lastPoll + this.pollTimeMs < time.milliseconds())
                    requiresPoll = true;

            } else {
                // even when no task is assigned, we must poll to get a task.
                requiresPoll = true;
            }
            maybeCommit();
            maybeUpdateStandbyTasks();

            maybeClean();
        }
    }

    private void maybeUpdateStandbyTasks() {
        if (!standbyTasks.isEmpty()) {
            if (processStandbyRecords) {
                if (!standbyRecords.isEmpty()) {
                    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingStandbyRecords = new HashMap<>();

                    for (TopicPartition partition : standbyRecords.keySet()) {
                        List<ConsumerRecord<byte[], byte[]>> remaining = standbyRecords.get(partition);
                        if (remaining != null) {
                            StandbyTask task = standbyTasksByPartition.get(partition);
                            remaining = task.update(partition, remaining);
                            if (remaining != null) {
                                remainingStandbyRecords.put(partition, remaining);
                            } else {
                                restoreConsumer.resume(partition);
                            }
                        }
                    }

                    standbyRecords = remainingStandbyRecords;
                }
                processStandbyRecords = false;
            }

            ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(0);

            if (!records.isEmpty()) {
                for (TopicPartition partition : records.partitions()) {
                    StandbyTask task = standbyTasksByPartition.get(partition);

                    if (task == null) {
                        log.error("missing standby task for partition {}", partition);
                        throw new StreamsException("missing standby task for partition " + partition);
                    }

                    List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                    if (remaining != null) {
                        restoreConsumer.pause(partition);
                        standbyRecords.put(partition, remaining);
                    }
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

            } catch (KafkaException e) {
                log.error("Failed to punctuate active task #" + task.id() + " in thread [" + this.getName() + "]: ", e);
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

            processStandbyRecords = true;
        } else {
            for (StreamTask task : activeTasks.values()) {
                try {
                    if (task.commitNeeded())
                        commitOne(task, time.milliseconds());
                } catch (KafkaException e) {
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
        } catch (CommitFailedException e) {
            // commit failed. Just log it.
            log.warn("Failed to commit " + task.getClass().getSimpleName() + " #" + task.id() + " in thread [" + this.getName() + "]: ", e);
        } catch (KafkaException e) {
            // commit failed due to an unexpected exception. Log it and rethrow the exception.
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
                        String dirName = dir.getName();
                        TaskId id = TaskId.parse(dirName.substring(dirName.lastIndexOf("-") + 1));

                        // try to acquire the exclusive lock on the state directory
                        if (dir.exists()) {
                            FileLock directoryLock = null;
                            try {
                                directoryLock = ProcessorStateManager.lockStateDirectory(dir);
                                if (directoryLock != null) {
                                    log.info("Deleting obsolete state directory {} for task {} after delayed {} ms.", dir.getAbsolutePath(), id, cleanTimeMs);
                                    Utils.delete(dir);
                                }
                            } catch (FileNotFoundException e) {
                                // the state directory may be deleted by another thread
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
                        }
                    } catch (TaskIdFormatException e) {
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

                } catch (TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    protected StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitions) {
        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        return new StreamTask(id, jobId, partitions, topology, consumer, producer, restoreConsumer, config, sensors);
    }

    private void addStreamTasks(Collection<TopicPartition> assignment) {
        if (partitionAssignor == null)
            throw new IllegalStateException("Partition assignor has not been initialized while adding stream tasks: this should not happen.");

        HashMap<TaskId, Set<TopicPartition>> partitionsForTask = new HashMap<>();

        for (TopicPartition partition : assignment) {
            Set<TaskId> taskIds = partitionAssignor.tasksForPartition(partition);
            for (TaskId taskId : taskIds) {
                Set<TopicPartition> partitions = partitionsForTask.get(taskId);
                if (partitions == null) {
                    partitions = new HashSet<>();
                    partitionsForTask.put(taskId, partitions);
                }
                partitions.add(partition);
            }
        }

        // create the active tasks
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            TaskId taskId = entry.getKey();
            Set<TopicPartition> partitions = entry.getValue();

            try {
                StreamTask task = createStreamTask(taskId, partitions);
                activeTasks.put(taskId, task);

                for (TopicPartition partition : partitions)
                    activeTasksByPartition.put(partition, task);
            } catch (StreamsException e) {
                log.error("Failed to create an active task #" + taskId + " in thread [" + this.getName() + "]: ", e);
                throw e;
            }
        }
    }

    private void removeStreamTasks() {
        try {
            for (StreamTask task : activeTasks.values()) {
                closeOne(task);
            }
            prevTasks.clear();
            prevTasks.addAll(activeTasks.keySet());

            activeTasks.clear();
            activeTasksByPartition.clear();

        } catch (Exception e) {
            log.error("Failed to remove stream tasks in thread [" + this.getName() + "]: ", e);
        }
    }

    private void closeOne(AbstractTask task) {
        log.info("Removing a task {}", task.id());
        try {
            task.close();
        } catch (StreamsException e) {
            log.error("Failed to close a " + task.getClass().getSimpleName() + " #" + task.id() + " in thread [" + this.getName() + "]: ", e);
        }
        sensors.taskDestructionSensor.record();
    }

    protected StandbyTask createStandbyTask(TaskId id, Collection<TopicPartition> partitions) {
        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        if (!topology.stateStoreSuppliers().isEmpty()) {
            return new StandbyTask(id, jobId, partitions, topology, consumer, restoreConsumer, config, sensors);
        } else {
            return null;
        }
    }

    private void addStandbyTasks() {
        if (partitionAssignor == null)
            throw new IllegalStateException("Partition assignor has not been initialized while adding standby tasks: this should not happen.");

        Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();

        // create the standby tasks
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : partitionAssignor.standbyTasks().entrySet()) {
            TaskId taskId = entry.getKey();
            Set<TopicPartition> partitions = entry.getValue();
            StandbyTask task = createStandbyTask(taskId, partitions);
            if (task != null) {
                standbyTasks.put(taskId, task);
                for (TopicPartition partition : partitions) {
                    standbyTasksByPartition.put(partition, task);
                }
                // collect checked pointed offsets to position the restore consumer
                // this include all partitions from which we restore states
                for (TopicPartition partition : task.checkpointedOffsets().keySet()) {
                    standbyTasksByPartition.put(partition, task);
                }
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
        try {
            for (StandbyTask task : standbyTasks.values()) {
                closeOne(task);
            }
            standbyTasks.clear();
            standbyTasksByPartition.clear();
            standbyRecords.clear();

            // un-assign the change log partitions
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());

        } catch (Exception e) {
            log.error("Failed to remove standby tasks in thread [" + this.getName() + "]: ", e);
        }
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
                throw new TopologyBuilderException("Topic not found: " + topic);

            if (numPartitions == -1) {
                numPartitions = infos.size();
            } else if (numPartitions != infos.size()) {
                String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                Arrays.sort(topics);
                throw new TopologyBuilderException("Topics not copartitioned: [" + Utils.mkString(Arrays.asList(topics), ",") + "]");
            }
        }
    }

    private class StreamsMetricsImpl implements StreamsMetrics {
        final Metrics metrics;
        final String metricGrpName;
        final Map<String, String> metricTags;

        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreationSensor;
        final Sensor taskDestructionSensor;

        public StreamsMetricsImpl(Metrics metrics) {

            this.metrics = metrics;
            this.metricGrpName = "stream-metrics";
            this.metricTags = new LinkedHashMap<>();
            this.metricTags.put("client-id", clientId + "-" + getName());

            this.commitTimeSensor = metrics.sensor("commit-time");
            this.commitTimeSensor.add(metrics.metricName("commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.commitTimeSensor.add(metrics.metricName("commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.commitTimeSensor.add(metrics.metricName("commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.pollTimeSensor = metrics.sensor("poll-time");
            this.pollTimeSensor.add(metrics.metricName("poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
            this.pollTimeSensor.add(metrics.metricName("poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
            this.pollTimeSensor.add(metrics.metricName("poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

            this.processTimeSensor = metrics.sensor("process-time");
            this.processTimeSensor.add(metrics.metricName("process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
            this.processTimeSensor.add(metrics.metricName("process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
            this.processTimeSensor.add(metrics.metricName("process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

            this.punctuateTimeSensor = metrics.sensor("punctuate-time");
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

            this.taskCreationSensor = metrics.sensor("task-creation");
            this.taskCreationSensor.add(metrics.metricName("task-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

            this.taskDestructionSensor = metrics.sensor("task-destruction");
            this.taskDestructionSensor.add(metrics.metricName("task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));
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

            String metricGroupName = "stream-" + scopeName + "-metrics";

            // first add the global operation metrics if not yet, with the global tags only
            Sensor parent = metrics.sensor(scopeName + "-" + operationName);
            addLatencyMetrics(metricGroupName, parent, "all", operationName, this.metricTags);

            // add the store operation metrics with additional tags
            Sensor sensor = metrics.sensor(scopeName + "-" + entityName + "-" + operationName, parent);
            addLatencyMetrics(metricGroupName, sensor, entityName, operationName, tagMap);

            return sensor;
        }

        private void addLatencyMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg-latency-ms", metricGrpName,
                "The average latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Avg());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max-latency-ms", metricGrpName,
                "The max latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Max());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-qps", metricGrpName,
                "The average number of occurrence of " + entityName + " " + opName + " operation per second.", tags), new Rate(new Count()));
        }

        private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
            if (!metrics.metrics().containsKey(name))
                sensor.add(name, stat);
        }
    }
}
