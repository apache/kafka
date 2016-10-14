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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

public class StreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static final AtomicInteger STREAM_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    public final PartitionGrouper partitionGrouper;
    private final StreamsMetadataState streamsMetadataState;
    public final String applicationId;
    public final String clientId;
    public final UUID processId;

    protected final StreamsConfig config;
    protected final TopologyBuilder builder;
    protected final Set<String> sourceTopics;
    protected final Pattern topicPattern;
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    protected final Consumer<byte[], byte[]> restoreConsumer;

    private final String logPrefix;
    private final String threadClientId;
    private final AtomicBoolean running;
    private final Map<TaskId, StreamTask> activeTasks;
    private final Map<TaskId, StandbyTask> standbyTasks;
    private final Map<TopicPartition, StreamTask> activeTasksByPartition;
    private final Map<TopicPartition, StandbyTask> standbyTasksByPartition;
    private final Set<TaskId> prevTasks;
    private final Time time;
    private final long pollTimeMs;
    private final long cleanTimeMs;
    private final long commitTimeMs;
    private final StreamsMetricsImpl sensors;
    final StateDirectory stateDirectory;

    private StreamPartitionAssignor partitionAssignor = null;

    private long timerStartedMs;
    private long lastCleanMs;
    private long lastCommitMs;
    private Throwable rebalanceException = null;

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords;
    private boolean processStandbyRecords = false;
    private AtomicBoolean initialized = new AtomicBoolean(false);

    private final long cacheSizeBytes;
    private ThreadCache cache;

    final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
            try {
                log.info("stream-thread [{}] New partitions [{}] assigned at the end of consumer rebalance.",
                        StreamThread.this.getName(), assignment);

                addStreamTasks(assignment);
                addStandbyTasks();
                lastCleanMs = time.milliseconds(); // start the cleaning cycle
                streamsMetadataState.onChange(partitionAssignor.getPartitionsByHostState(), partitionAssignor.clusterMetadata());
                initialized.set(true);
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> assignment) {
            try {
                log.info("stream-thread [{}] partitions [{}] revoked at the beginning of consumer rebalance.",
                        StreamThread.this.getName(), assignment);

                initialized.set(false);
                lastCleanMs = Long.MAX_VALUE; // stop the cleaning cycle until partitions are assigned
                shutdownTasksAndState(true);
            } catch (Throwable t) {
                rebalanceException = t;
                throw t;
            } finally {
                // TODO: right now upon partition revocation, we always remove all the tasks;
                // this behavior can be optimized to only remove affected tasks in the future
                streamsMetadataState.onChange(Collections.<HostInfo, Set<TopicPartition>>emptyMap(), partitionAssignor.clusterMetadata());
                removeStreamTasks();
                removeStandbyTasks();
            }
        }
    };

    public boolean isInitialized() {
        return initialized.get();
    }

    public StreamThread(TopologyBuilder builder,
                        StreamsConfig config,
                        KafkaClientSupplier clientSupplier,
                        String applicationId,
                        String clientId,
                        UUID processId,
                        Metrics metrics,
                        Time time,
                        StreamsMetadataState streamsMetadataState) {
        super("StreamThread-" + STREAM_THREAD_ID_SEQUENCE.getAndIncrement());

        this.applicationId = applicationId;
        String threadName = getName();
        this.config = config;
        this.builder = builder;
        this.sourceTopics = builder.sourceTopics();
        this.topicPattern = builder.sourceTopicPattern();
        this.clientId = clientId;
        this.processId = processId;
        this.partitionGrouper = config.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);
        this.streamsMetadataState = streamsMetadataState;
        threadClientId = clientId + "-" + threadName;
        this.sensors = new StreamsMetricsImpl(metrics);
        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) < 0) {
            log.warn("Negative cache size passed in thread [{}]. Reverting to cache size of 0 bytes.", threadName);
        }
        this.cacheSizeBytes = Math.max(0, config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) /
            config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG));
        this.cache = new ThreadCache(threadClientId, cacheSizeBytes, this.sensors);


        this.logPrefix = String.format("stream-thread [%s]", threadName);

        // set the producer and consumer clients
        log.info("{} Creating producer client", logPrefix);
        this.producer = clientSupplier.getProducer(config.getProducerConfigs(threadClientId));
        log.info("{} Creating consumer client", logPrefix);
        this.consumer = clientSupplier.getConsumer(config.getConsumerConfigs(this, applicationId, threadClientId));
        log.info("{} Creating restore consumer client", logPrefix);
        this.restoreConsumer = clientSupplier.getRestoreConsumer(config.getRestoreConsumerConfigs(threadClientId));

        // initialize the task list
        // activeTasks needs to be concurrent as it can be accessed
        // by QueryableState
        this.activeTasks = new ConcurrentHashMap<>();
        this.standbyTasks = new HashMap<>();
        this.activeTasksByPartition = new HashMap<>();
        this.standbyTasksByPartition = new HashMap<>();
        this.prevTasks = new HashSet<>();

        // standby ktables
        this.standbyRecords = new HashMap<>();

        this.stateDirectory = new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG));
        this.pollTimeMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.cleanTimeMs = config.getLong(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG);

        this.time = time;
        this.timerStartedMs = time.milliseconds();
        this.lastCleanMs = Long.MAX_VALUE; // the cleaning cycle won't start until partition assignment
        this.lastCommitMs = timerStartedMs;


        this.running = new AtomicBoolean(true);
    }

    public void partitionAssignor(StreamPartitionAssignor partitionAssignor) {
        this.partitionAssignor = partitionAssignor;
    }

    /**
     * Execute the stream processors
     * @throws KafkaException for any Kafka-related exceptions
     * @throws Exception for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("{} Starting", logPrefix);

        try {
            runLoop();
        } catch (KafkaException e) {
            // just re-throw the exception as it should be logged already
            throw e;
        } catch (Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("{} Streams application error during processing: ", logPrefix, e);
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
        log.info("{} Shutting down", logPrefix);
        shutdownTasksAndState(false);

        // close all embedded clients
        try {
            producer.close();
        } catch (Throwable e) {
            log.error("{} Failed to close producer: ", logPrefix, e);
        }
        try {
            consumer.close();
        } catch (Throwable e) {
            log.error("{} Failed to close consumer: ", logPrefix, e);
        }
        try {
            restoreConsumer.close();
        } catch (Throwable e) {
            log.error("{} Failed to close restore consumer: ", logPrefix, e);
        }

        // remove all tasks
        removeStreamTasks();
        removeStandbyTasks();

        log.info("{} Stream thread shutdown complete", logPrefix);
    }

    private void shutdownTasksAndState(final boolean rethrowExceptions) {
        // Commit first as there may be cached records that have not been flushed yet.
        commitOffsets(rethrowExceptions);
        // Close all processors in topology order
        closeAllTasks();
        // flush state
        flushAllState(rethrowExceptions);
        // flush out any extra data sent during close
        producer.flush();
        // Close all task state managers
        closeAllStateManagers(rethrowExceptions);
        try {
            // un-assign the change log partitions
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        } catch (Exception e) {
            log.error("{} Failed to un-assign change log partitions: ", logPrefix, e);
            if (rethrowExceptions) {
                throw e;
            }
        }
    }

    interface AbstractTaskAction {
        void apply(final AbstractTask task);
    }

    private void performOnAllTasks(final AbstractTaskAction action,
                                   final String exceptionMessage,
                                   final boolean throwExceptions) {
        final List<AbstractTask> allTasks = new ArrayList<AbstractTask>(activeTasks.values());
        allTasks.addAll(standbyTasks.values());
        for (final AbstractTask task : allTasks) {
            try {
                action.apply(task);
            } catch (KafkaException e) {
                log.error("{} Failed while executing {} {} duet to {}: ",
                        StreamThread.this.logPrefix,
                        task.getClass().getSimpleName(),
                        task.id(),
                        exceptionMessage,
                        e);
                if (throwExceptions) {
                    throw e;
                }
            }
        }
    }

    private void closeAllStateManagers(final boolean throwExceptions) {
        performOnAllTasks(new AbstractTaskAction() {
            @Override
            public void apply(final AbstractTask task) {
                log.info("{} Closing the state manager of task {}", StreamThread.this.logPrefix, task.id());
                task.closeStateManager();
            }
        }, "close state manager", throwExceptions);
    }

    private void commitOffsets(final boolean throwExceptions) {
        // Exceptions should not prevent this call from going through all shutdown steps
        performOnAllTasks(new AbstractTaskAction() {
            @Override
            public void apply(final AbstractTask task) {
                log.info("{} Committing consumer offsets of task {}", StreamThread.this.logPrefix, task.id());
                task.commitOffsets();
            }
        }, "commit consumer offsets", throwExceptions);
    }

    private void flushAllState(final boolean throwExceptions) {
        performOnAllTasks(new AbstractTaskAction() {
            @Override
            public void apply(final AbstractTask task) {
                log.info("{} Flushing state stores of task {}", StreamThread.this.logPrefix, task.id());
                task.flushState();
            }
        }, "flush state", throwExceptions);
    }

    /**
     * Compute the latency based on the current marked timestamp,
     * and update the marked timestamp with the current system timestamp.
     *
     * @return latency
     */
    private long computeLatency() {
        long previousTimeMs = this.timerStartedMs;
        this.timerStartedMs = time.milliseconds();

        return Math.max(this.timerStartedMs - previousTimeMs, 0);
    }

    private void runLoop() {
        int totalNumBuffered = 0;
        boolean requiresPoll = true;
        boolean polledRecords = false;

        if (topicPattern != null) {
            consumer.subscribe(topicPattern, rebalanceListener);
        } else {
            consumer.subscribe(new ArrayList<>(sourceTopics), rebalanceListener);
        }

        while (stillRunning()) {
            this.timerStartedMs = time.milliseconds();

            // try to fetch some records if necessary
            if (requiresPoll) {
                requiresPoll = false;

                boolean longPoll = totalNumBuffered == 0;

                ConsumerRecords<byte[], byte[]> records = consumer.poll(longPoll ? this.pollTimeMs : 0);

                if (rebalanceException != null)
                    throw new StreamsException(logPrefix + " Failed to rebalance", rebalanceException);

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        StreamTask task = activeTasksByPartition.get(partition);
                        task.addRecords(partition, records.records(partition));
                    }
                    polledRecords = true;
                } else {
                    polledRecords = false;
                }

                // only record poll latency is long poll is required
                if (longPoll) {
                    sensors.pollTimeSensor.record(computeLatency());
                }
            }

            // try to process one fetch record from each task via the topology, and also trigger punctuate
            // functions if necessary, which may result in more records going through the topology in this loop
            if (totalNumBuffered > 0 || polledRecords) {
                totalNumBuffered = 0;

                if (!activeTasks.isEmpty()) {
                    for (StreamTask task : activeTasks.values()) {

                        totalNumBuffered += task.process();

                        requiresPoll = requiresPoll || task.requiresPoll();

                        sensors.processTimeSensor.record(computeLatency());

                        maybePunctuate(task);

                        if (task.commitNeeded())
                            commitOne(task);
                    }

                } else {
                    // even when no task is assigned, we must poll to get a task.
                    requiresPoll = true;
                }

            } else {
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
                                restoreConsumer.resume(singleton(partition));
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
                        throw new StreamsException(logPrefix + " Missing standby task for partition " + partition);
                    }

                    List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                    if (remaining != null) {
                        restoreConsumer.pause(singleton(partition));
                        standbyRecords.put(partition, remaining);
                    }
                }
            }
        }
    }

    private boolean stillRunning() {
        if (!running.get()) {
            log.debug("{} Shutting down at user request", logPrefix);
            return false;
        }

        return true;
    }

    private void maybePunctuate(StreamTask task) {
        try {
            // check whether we should punctuate based on the task's partition group timestamp;
            // which are essentially based on record timestamp.
            if (task.maybePunctuate())
                sensors.punctuateTimeSensor.record(computeLatency());

        } catch (KafkaException e) {
            log.error("{} Failed to punctuate active task {}: ", logPrefix, task.id(), e);
            throw e;
        }
    }

    /**
     * Commit all tasks owned by this thread if specified interval time has elapsed
     */
    protected void maybeCommit() {
        long now = time.milliseconds();

        if (commitTimeMs >= 0 && lastCommitMs + commitTimeMs < now) {
            log.info("{} Committing all tasks because the commit interval {}ms has elapsed", logPrefix, commitTimeMs);

            commitAll();
            lastCommitMs = now;

            processStandbyRecords = true;
        }
    }

    /**
     * Cleanup any states of the tasks that have been removed from this thread
     */
    protected void maybeClean() {
        long now = time.milliseconds();

        if (now > lastCleanMs + cleanTimeMs) {
            stateDirectory.cleanRemovedTasks();
            lastCleanMs = now;
        }
    }

    /**
     * Commit the states of all its tasks
     */
    private void commitAll() {
        for (StreamTask task : activeTasks.values()) {
            commitOne(task);
        }
        for (StandbyTask task : standbyTasks.values()) {
            commitOne(task);
        }
    }

    /**
     * Commit the state of a task
     */
    private void commitOne(AbstractTask task) {
        log.info("{} Committing task {}", logPrefix, task.id());

        try {
            task.commit();
        } catch (CommitFailedException e) {
            // commit failed. Just log it.
            log.warn("{} Failed to commit {} {} state: ", logPrefix, task.getClass().getSimpleName(), task.id(), e);
        } catch (KafkaException e) {
            // commit failed due to an unexpected exception. Log it and rethrow the exception.
            log.error("{} Failed to commit {} {} state: ", logPrefix, task.getClass().getSimpleName(), task.id(), e);
            throw e;
        }

        sensors.commitTimeSensor.record(computeLatency());
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

        File[] stateDirs = stateDirectory.listTaskDirectories();
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
        log.info("{} Creating active task {} with assigned partitions [{}]", logPrefix, id, partitions);

        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        return new StreamTask(id, applicationId, partitions, topology, consumer, producer, restoreConsumer, config, sensors, stateDirectory, cache);
    }

    private void addStreamTasks(Collection<TopicPartition> assignment) {
        if (partitionAssignor == null)
            throw new IllegalStateException(logPrefix + " Partition assignor has not been initialized while adding stream tasks: this should not happen.");

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
                log.error("{} Failed to create an active task %s: ", logPrefix, taskId, e);
                throw e;
            }
        }
    }

    private StandbyTask createStandbyTask(TaskId id, Collection<TopicPartition> partitions) {
        log.info("{} Creating new standby task {} with assigned partitions [{}]", logPrefix, id, partitions);

        sensors.taskCreationSensor.record();

        ProcessorTopology topology = builder.build(id.topicGroupId);

        if (!topology.stateStores().isEmpty()) {
            return new StandbyTask(id, applicationId, partitions, topology, consumer, restoreConsumer, config, sensors, stateDirectory);
        } else {
            return null;
        }
    }

    private void addStandbyTasks() {
        if (partitionAssignor == null)
            throw new IllegalStateException(logPrefix + " Partition assignor has not been initialized while adding standby tasks: this should not happen.");

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
                restoreConsumer.seekToBeginning(singleton(partition));
            }
        }
    }

    private void removeStreamTasks() {
        log.info("{} Removing all active tasks [{}]", logPrefix, activeTasks.keySet());

        try {
            prevTasks.clear();
            prevTasks.addAll(activeTasks.keySet());

            activeTasks.clear();
            activeTasksByPartition.clear();

        } catch (Exception e) {
            log.error("{} Failed to remove stream tasks: ", logPrefix, e);
        }
    }

    private void removeStandbyTasks() {
        log.info("{} Removing all standby tasks [{}]", logPrefix, standbyTasks.keySet());

        standbyTasks.clear();
        standbyTasksByPartition.clear();
        standbyRecords.clear();
    }

    private void closeAllTasks() {
        performOnAllTasks(new AbstractTaskAction() {
            @Override
            public void apply(final AbstractTask task) {
                log.info("{} Closing a task {}", StreamThread.this.logPrefix, task.id());
                task.close();
                sensors.taskDestructionSensor.record();
            }
        }, "close", false);
    }

    /**
     * Produces a string representation contain useful information about a StreamThread.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("StreamsThread appId:" + this.applicationId + "\n");
        sb.append("\tStreamsThread clientId:" + clientId + "\n");
        sb.append("\tStreamsThread threadId:" + this.getName() + "\n");

        // iterate and print active tasks
        if (activeTasks != null) {
            sb.append("\tActive tasks:\n");
            for (TaskId tId : activeTasks.keySet()) {
                StreamTask task = activeTasks.get(tId);
                sb.append("\t\t" + task.toString());
            }
        }

        // iterate and print standby tasks
        if (standbyTasks != null) {
            sb.append("\tStandby tasks:\n");
            for (TaskId tId : standbyTasks.keySet()) {
                StandbyTask task = standbyTasks.get(tId);
                sb.append("\t\t" + task.toString());
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    private class StreamsMetricsImpl implements StreamsMetrics, ThreadCacheMetrics {
        final Metrics metrics;
        final String metricGrpName;
        final String sensorNamePrefix;
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
            this.sensorNamePrefix = "thread." + threadClientId;
            this.metricTags = Collections.singletonMap("client-id", threadClientId);

            this.commitTimeSensor = metrics.sensor(sensorNamePrefix + ".commit-time");
            this.commitTimeSensor.add(metrics.metricName("commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.commitTimeSensor.add(metrics.metricName("commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.commitTimeSensor.add(metrics.metricName("commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.pollTimeSensor = metrics.sensor(sensorNamePrefix + ".poll-time");
            this.pollTimeSensor.add(metrics.metricName("poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
            this.pollTimeSensor.add(metrics.metricName("poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
            this.pollTimeSensor.add(metrics.metricName("poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

            this.processTimeSensor = metrics.sensor(sensorNamePrefix + ".process-time");
            this.processTimeSensor.add(metrics.metricName("process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
            this.processTimeSensor.add(metrics.metricName("process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
            this.processTimeSensor.add(metrics.metricName("process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

            this.punctuateTimeSensor = metrics.sensor(sensorNamePrefix + ".punctuate-time");
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

            this.taskCreationSensor = metrics.sensor(sensorNamePrefix + ".task-creation");
            this.taskCreationSensor.add(metrics.metricName("task-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

            this.taskDestructionSensor = metrics.sensor(sensorNamePrefix + ".task-destruction");
            this.taskDestructionSensor.add(metrics.metricName("task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));
        }

        @Override
        public void recordLatency(Sensor sensor, long startNs, long endNs) {
            sensor.record(endNs - startNs, timerStartedMs);
        }

        @Override
        public void recordCacheSensor(Sensor sensor, double count) {
            sensor.record(count);
        }

        /**
         * @throws IllegalArgumentException if tags is not constructed in key-value pairs
         */
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
            Sensor parent = metrics.sensor(sensorNamePrefix + "." + scopeName + "-" + operationName);
            addLatencyMetrics(metricGroupName, parent, "all", operationName, this.metricTags);

            // add the store operation metrics with additional tags
            Sensor sensor = metrics.sensor(sensorNamePrefix + "." + scopeName + "-" + entityName + "-" + operationName, parent);
            addLatencyMetrics(metricGroupName, sensor, entityName, operationName, tagMap);

            return sensor;
        }

        @Override
        public Sensor addCacheSensor(String entityName, String operationName, String... tags) {
            // extract the additional tags if there are any
            Map<String, String> tagMap = new HashMap<>(this.metricTags);
            if ((tags.length % 2) != 0)
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

            for (int i = 0; i < tags.length; i += 2)
                tagMap.put(tags[i], tags[i + 1]);

            String metricGroupName = "stream-thread-cache-metrics";

            Sensor sensor = metrics.sensor(sensorNamePrefix + "-" + entityName + "-" + operationName);
            addCacheMetrics(metricGroupName, sensor, entityName, operationName, tagMap);
            return sensor;

        }

        private void addCacheMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg", metricGrpName,
                "The current count of " + entityName + " " + opName + " operation.", tags), new Avg());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-min", metricGrpName,
                "The current count of " + entityName + " " + opName + " operation.", tags), new Min());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max", metricGrpName,
                "The current count of " + entityName + " " + opName + " operation.", tags), new Max());
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
