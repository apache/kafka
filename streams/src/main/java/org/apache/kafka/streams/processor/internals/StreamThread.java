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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Sum;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;

public class StreamThread extends Thread implements ThreadDataProvider {

    private static final Logger log = LoggerFactory.getLogger(StreamThread.class);
    private static final AtomicInteger STREAM_THREAD_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * Stream thread states are the possible states that a stream thread can be in.
     * A thread must only be in one state at a time
     * The expected state transitions with the following defined states is:
     *
     * <pre>
     *                +-------------+
     *          +<--- | Created     |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Running     | <----+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          +<--- | Partitions  | <-+  |
     *          |     | Revoked     | --+  |
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          +<--- | Assigning   |      |
     *          |     | Partitions  | ---->+
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *          |     | Shutdown    |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Dead        |
     *                +-------------+
     * </pre>
     *
     * Note the following:
     * - Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.
     * - Any state can go to DEAD. That is because exceptions can happen at any other state,
     *   leading to the stream thread terminating.
     * - A streams thread can stay in PARTITIONS_REVOKED indefinitely, in the corner case when
     *   the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
     *
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 4, 5), RUNNING(2, 4, 5), PARTITIONS_REVOKED(2, 3, 4, 5), ASSIGNING_PARTITIONS(1, 4, 5), PENDING_SHUTDOWN(5), DEAD;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return !equals(PENDING_SHUTDOWN) && !equals(CREATED) && !equals(DEAD);
        }

        @Override
        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         * @param thread       thread changing state
         * @param newState     current state
         * @param oldState     previous state
         */
        void onChange(final Thread thread, final ThreadStateTransitionValidator newState, final ThreadStateTransitionValidator oldState);
    }


    static class RebalanceListener implements ConsumerRebalanceListener {
        private final Time time;
        private final TaskManager taskManager;
        private final StreamThread streamThread;
        private final String logPrefix;

        RebalanceListener(final Time time,
                          final TaskManager taskManager,
                          final StreamThread streamThread,
                          final String logPrefix) {
            this.time = time;
            this.taskManager = taskManager;
            this.streamThread = streamThread;
            this.logPrefix = logPrefix;
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> assignment) {
            final long start = time.milliseconds();
            try {
                streamThread.setState(State.ASSIGNING_PARTITIONS);
                taskManager.createTasks(assignment);
                final RuntimeException exception = streamThread.unAssignChangeLogPartitions();
                if (exception != null) {
                    throw exception;
                }
                streamThread.refreshMetadataState();
                streamThread.setState(State.RUNNING);
            } catch (final Throwable t) {
                streamThread.setRebalanceException(t);
                throw t;
            } finally {
                log.info("{} partition assignment took {} ms.\n" +
                                 "\tcurrent active tasks: {}\n" +
                                 "\tcurrent standby tasks: {}\n" +
                                 "\tprevious active tasks: {}\n",
                         logPrefix,
                         time.milliseconds() - start,
                         taskManager.activeTaskIds(),
                         taskManager.standbyTaskIds(),
                         taskManager.prevActiveTaskIds());
            }
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> assignment) {
            log.debug("{} at state {}: partitions {} revoked at the beginning of consumer rebalance.\n" +
                    "\tcurrent assigned active tasks: {}\n" +
                    "\tcurrent assigned standby tasks: {}\n",
                logPrefix,
                streamThread.state,
                assignment,
                taskManager.activeTaskIds(),
                taskManager.standbyTaskIds());

            final long start = time.milliseconds();
            try {
                streamThread.setState(State.PARTITIONS_REVOKED);
                // suspend active tasks
                taskManager.suspendTasksAndState();
            } catch (final Throwable t) {
                streamThread.setRebalanceException(t);
                throw t;
            } finally {
                streamThread.refreshMetadataState();
                taskManager.removeTasks();
                streamThread.clearStandbyRecords();

                log.info("{} partition revocation took {} ms.\n" +
                        "\tsuspended active tasks: {}\n" +
                        "\tsuspended standby tasks: {}",
                    logPrefix,
                    time.milliseconds() - start,
                    taskManager.suspendedActiveTaskIds(),
                    taskManager.suspendedStandbyTaskIds());
            }
        }
    }


    static abstract class AbstractTaskCreator {
        final static long MAX_BACKOFF_TIME_MS = 1000L;
        private final long rebalanceTimeoutMs;
        final String applicationId;
        final InternalTopologyBuilder builder;
        final StreamsConfig config;
        final StreamsMetrics streamsMetrics;
        final StateDirectory stateDirectory;
        final Sensor taskCreatedSensor;
        final ChangelogReader storeChangelogReader;
        final Time time;
        final String logPrefix;

        AbstractTaskCreator(final InternalTopologyBuilder builder,
                            final StreamsConfig config,
                            final StreamsMetrics streamsMetrics,
                            final StateDirectory stateDirectory,
                            final Sensor taskCreatedSensor,
                            final ChangelogReader storeChangelogReader,
                            final Time time,
                            final long rebalanceTimeoutMs,
                            final String logPrefix) {
            this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            this.builder = builder;
            this.config = config;
            this.streamsMetrics = streamsMetrics;
            this.stateDirectory = stateDirectory;
            this.taskCreatedSensor = taskCreatedSensor;
            this.storeChangelogReader = storeChangelogReader;
            this.time = time;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.logPrefix = logPrefix;
        }

        Map<Task, Set<TopicPartition>> retryWithBackoff(final Consumer<byte[], byte[]> consumer, final Map<TaskId, Set<TopicPartition>> tasksToBeCreated, final long start) {
            long backoffTimeMs = 50L;
            final Set<TaskId> retryingTasks = new HashSet<>();
            final Map<Task, Set<TopicPartition>> createdTasks = new HashMap<>();
            while (true) {
                final Iterator<Map.Entry<TaskId, Set<TopicPartition>>> it = tasksToBeCreated.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions = it.next();
                    final TaskId taskId = newTaskAndPartitions.getKey();
                    final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();

                    try {
                        final Task task = createTask(consumer, taskId, partitions);
                        if (task != null) {
                            log.trace("{} Created task {} with assigned partitions {}", logPrefix, taskId, partitions);
                            createdTasks.put(task, partitions);
                        }
                        it.remove();
                        backoffTimeMs = 50L;
                        retryingTasks.remove(taskId);
                    } catch (final LockException e) {
                        // ignore and retry
                        if (!retryingTasks.contains(taskId)) {
                            log.warn("{} Could not create task {} due to {}; will retry", logPrefix, taskId, e);
                            retryingTasks.add(taskId);
                        }
                    }
                }

                if (tasksToBeCreated.isEmpty() || time.milliseconds() - start > rebalanceTimeoutMs) {
                    break;
                }

                try {
                    Thread.sleep(backoffTimeMs);
                    backoffTimeMs <<= 1;
                    backoffTimeMs = Math.min(backoffTimeMs, MAX_BACKOFF_TIME_MS);
                } catch (final InterruptedException e) {
                    // ignore
                }
            }
            return createdTasks;
        }

        abstract Task createTask(final Consumer<byte[], byte[]> consumer, final TaskId id, final Set<TopicPartition> partitions);

        public void close() {}
    }

    static class TaskCreator extends AbstractTaskCreator {
        private final ThreadCache cache;
        private final KafkaClientSupplier clientSupplier;
        private final String threadClientId;
        private final Producer<byte[], byte[]> threadProducer;


        TaskCreator(final InternalTopologyBuilder builder,
                    final StreamsConfig config,
                    final StreamsMetrics streamsMetrics,
                    final StateDirectory stateDirectory,
                    final Sensor taskCreatedSensor,
                    final ChangelogReader storeChangelogReader,
                    final ThreadCache cache,
                    final Time time,
                    final KafkaClientSupplier clientSupplier,
                    final Producer<byte[], byte[]> threadProducer,
                    final String threadClientId,
                    final long rebalanceTimeoutMs,
                    final String logPrefix) {
            super(
                    builder,
                  config,
                  streamsMetrics,
                  stateDirectory,
                  taskCreatedSensor,
                  storeChangelogReader,
                  time,
                  rebalanceTimeoutMs,
                  logPrefix);
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
        }

        @Override
        StreamTask createTask(final Consumer<byte[], byte[]> consumer, final TaskId taskId, final Set<TopicPartition> partitions) {
            taskCreatedSensor.record();

            return new StreamTask(
                    taskId,
                    applicationId,
                    partitions,
                    builder.build(taskId.topicGroupId),
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory,
                    cache,
                    time,
                    createProducer(taskId));

        }

        @Override
        public void close() {
            if (threadProducer != null) {
                try {
                    threadProducer.close();
                } catch (final Throwable e) {
                    log.error("{} Failed to close producer due to the following error:", logPrefix, e);
                }
            }
        }

        private Producer<byte[], byte[]> createProducer(final TaskId id) {
            // eos
            if (threadProducer == null) {
                final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId + "-" + id);
                log.info("{} Creating producer client for task {}", logPrefix, id);
                producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + id);
                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;

        }
    }

    static class StandbyTaskCreator extends AbstractTaskCreator {
        StandbyTaskCreator(final InternalTopologyBuilder builder,
                           final StreamsConfig config,
                           final StreamsMetrics streamsMetrics,
                           final StateDirectory stateDirectory,
                           final Sensor taskCreatedSensor,
                           final ChangelogReader storeChangelogReader,
                           final Time time,
                           final long rebalanceTimeoutMs,
                           final String logPrefix) {
            super(builder,
                  config,
                  streamsMetrics,
                  stateDirectory,
                  taskCreatedSensor,
                  storeChangelogReader,
                  time,
                  rebalanceTimeoutMs,
                  logPrefix);
        }

        @Override
        StandbyTask createTask(final Consumer<byte[], byte[]> consumer, final TaskId taskId, final Set<TopicPartition> partitions) {
            taskCreatedSensor.record();

            final ProcessorTopology topology = builder.build(taskId.topicGroupId);

            if (!topology.stateStores().isEmpty()) {
                return new StandbyTask(taskId, applicationId, partitions, topology, consumer, storeChangelogReader, config, streamsMetrics, stateDirectory);
            } else {
                log.trace("{} Skipped standby task {} with assigned partitions {} since it does not have any state stores to materialize", logPrefix, taskId, partitions);

                return null;
            }
        }

    }

    /**
     * This class extends {@link StreamsMetricsImpl(Metrics, String, String, Map)} and
     * overrides one of its functions for efficiency
     */
    static class StreamsMetricsThreadImpl extends StreamsMetricsImpl {
        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreatedSensor;
        final Sensor tasksClosedSensor;
        final Sensor skippedRecordsSensor;

        StreamsMetricsThreadImpl(final Metrics metrics, final String groupName, final String prefix, final Map<String, String> tags) {
            super(metrics, groupName, tags);
            commitTimeSensor = metrics.sensor(prefix + ".commit-latency", Sensor.RecordingLevel.INFO);
            commitTimeSensor.add(metrics.metricName("commit-latency-avg", this.groupName, "The average commit time in ms", this.tags), new Avg());
            commitTimeSensor.add(metrics.metricName("commit-latency-max", this.groupName, "The maximum commit time in ms", this.tags), new Max());
            commitTimeSensor.add(metrics.metricName("commit-rate", this.groupName, "The average per-second number of commit calls", this.tags), new Rate(new Count()));

            pollTimeSensor = metrics.sensor(prefix + ".poll-latency", Sensor.RecordingLevel.INFO);
            pollTimeSensor.add(metrics.metricName("poll-latency-avg", this.groupName, "The average poll time in ms", this.tags), new Avg());
            pollTimeSensor.add(metrics.metricName("poll-latency-max", this.groupName, "The maximum poll time in ms", this.tags), new Max());
            pollTimeSensor.add(metrics.metricName("poll-rate", this.groupName, "The average per-second number of record-poll calls", this.tags), new Rate(new Count()));

            processTimeSensor = metrics.sensor(prefix + ".process-latency", Sensor.RecordingLevel.INFO);
            processTimeSensor.add(metrics.metricName("process-latency-avg", this.groupName, "The average process time in ms", this.tags), new Avg());
            processTimeSensor.add(metrics.metricName("process-latency-max", this.groupName, "The maximum process time in ms", this.tags), new Max());
            processTimeSensor.add(metrics.metricName("process-rate", this.groupName, "The average per-second number of process calls", this.tags), new Rate(new Count()));

            punctuateTimeSensor = metrics.sensor(prefix + ".punctuate-latency", Sensor.RecordingLevel.INFO);
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-avg", this.groupName, "The average punctuate time in ms", this.tags), new Avg());
            punctuateTimeSensor.add(metrics.metricName("punctuate-latency-max", this.groupName, "The maximum punctuate time in ms", this.tags), new Max());
            punctuateTimeSensor.add(metrics.metricName("punctuate-rate", this.groupName, "The average per-second number of punctuate calls", this.tags), new Rate(new Count()));

            taskCreatedSensor = metrics.sensor(prefix + ".task-created", Sensor.RecordingLevel.INFO);
            taskCreatedSensor.add(metrics.metricName("task-created-rate", this.groupName, "The average per-second number of newly created tasks", this.tags), new Rate(new Count()));

            tasksClosedSensor = metrics.sensor(prefix + ".task-closed", Sensor.RecordingLevel.INFO);
            tasksClosedSensor.add(metrics.metricName("task-closed-rate", this.groupName, "The average per-second number of closed tasks", this.tags), new Rate(new Count()));

            skippedRecordsSensor = metrics.sensor(prefix + ".skipped-records");
            skippedRecordsSensor.add(metrics.metricName("skipped-records-rate", this.groupName, "The average per-second number of skipped records.", this.tags), new Rate(new Sum()));

        }

        void removeAllSensors() {
            removeSensor(commitTimeSensor);
            removeSensor(pollTimeSensor);
            removeSensor(processTimeSensor);
            removeSensor(punctuateTimeSensor);
            removeSensor(taskCreatedSensor);
            removeSensor(tasksClosedSensor);
            removeSensor(skippedRecordsSensor);

        }
    }


    private final Object stateLock = new Object();
    private final StreamsMetadataState streamsMetadataState;
    private final String logPrefix;
    private final TaskManager taskManager;
    private final Time time;
    private final long pollTimeMs;
    private final long commitTimeMs;
    private final PartitionGrouper partitionGrouper;
    private final UUID processId;
    private final StateDirectory stateDirectory;
    private final StreamsMetricsThreadImpl streamsMetrics;

    private long lastCommitMs;
    private String originalReset;
    private ThreadMetadataProvider metadataProvider;
    private boolean processStandbyRecords = false;
    private Throwable rebalanceException;
    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords = new HashMap<>();
    private StreamThread.StateListener stateListener;
    private volatile State state = State.CREATED;
    private long timerStartedMs;

    final StreamsConfig config;
    final ConsumerRebalanceListener rebalanceListener;
    final Consumer<byte[], byte[]> restoreConsumer;

    protected final Consumer<byte[], byte[]> consumer;
    protected final InternalTopologyBuilder builder;

    public final String applicationId;
    public final String clientId;

    private final static int UNLIMITED_RECORDS = -1;

    public StreamThread(final InternalTopologyBuilder builder,
                        final String clientId,
                        final String threadClientId,
                        final StreamsConfig config,
                        final UUID processId,
                        final Time time,
                        final StreamsMetadataState streamsMetadataState,
                        final TaskManager taskManager,
                        final StreamsMetricsThreadImpl streamsMetrics,
                        final KafkaClientSupplier clientSupplier,
                        final Consumer<byte[], byte[]> restoreConsumer,
                        final StateDirectory stateDirectory) {
        super(threadClientId);
        this.builder = builder;
        this.clientId = clientId;
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.pollTimeMs = config.getLong(StreamsConfig.POLL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.processId = processId;
        this.time = time;
        this.streamsMetadataState = streamsMetadataState;
        this.taskManager = taskManager;
        this.logPrefix = logPrefix(threadClientId);
        this.streamsMetrics = streamsMetrics;
        this.restoreConsumer = restoreConsumer;
        this.stateDirectory = stateDirectory;
        this.rebalanceListener = new RebalanceListener(time, taskManager, this, logPrefix);
        this.config = config;
        this.partitionGrouper = config.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);
        log.info("{} Creating consumer client", logPrefix);
        final Map<String, Object> consumerConfigs = config.getConsumerConfigs(this, applicationId, threadClientId);

        if (!builder.latestResetTopicsPattern().pattern().equals("") || !builder.earliestResetTopicsPattern().pattern().equals("")) {
            originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            log.info("{} Custom offset resets specified updating configs original auto offset reset {}", logPrefix, originalReset);
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }
        this.consumer = clientSupplier.getConsumer(consumerConfigs);
        taskManager.setConsumer(consumer);
    }

    @SuppressWarnings("ConstantConditions")
    public static StreamThread create(final InternalTopologyBuilder builder,
                                      final StreamsConfig config,
                                      final KafkaClientSupplier clientSupplier,
                                      final UUID processId,
                                      final String clientId,
                                      final Metrics metrics,
                                      final Time time,
                                      final StreamsMetadataState streamsMetadataState,
                                      final long cacheSizeBytes,
                                      final StateDirectory stateDirectory,
                                      final StateRestoreListener stateRestoreListener) {

        final String threadClientId = clientId + "-StreamThread-" + STREAM_THREAD_ID_SEQUENCE.getAndIncrement();
        final StreamsMetricsThreadImpl streamsMetrics = new StreamsMetricsThreadImpl(metrics,
                                                                                     "stream-metrics",
                                                                                     "thread." + threadClientId,
                                                                                     Collections.singletonMap("client-id",
                                                                                                              threadClientId));

        final String logPrefix = logPrefix(threadClientId);
        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) < 0) {
            log.warn("{} Negative cache size passed in thread. Reverting to cache size of 0 bytes", logPrefix);
        }
        final ThreadCache cache = new ThreadCache(threadClientId, cacheSizeBytes, streamsMetrics);

        final boolean eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));

        log.info("{} Creating restore consumer client", logPrefix);
        final Map<String, Object> consumerConfigs = config.getRestoreConsumerConfigs(threadClientId);
        final Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(consumerConfigs);


        final Object maxPollInterval = consumerConfigs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        final int rebalanceTimeoutMs = (Integer) ConfigDef.parseType(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval, Type.INT);

        final StoreChangelogReader changelogReader = new StoreChangelogReader(threadClientId,
                                                                              restoreConsumer,
                                                                              time,
                                                                              config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                                                                              stateRestoreListener);

        Producer<byte[], byte[]> threadProducer = null;
        if (!eosEnabled) {
            final Map<String, Object> producerConfigs = config.getProducerConfigs(threadClientId);
            log.info("{} Creating shared producer client", logPrefix);
            threadProducer = clientSupplier.getProducer(producerConfigs);
        }

        final AbstractTaskCreator activeTaskCreator = new TaskCreator(builder,
                                                                      config,
                                                                      streamsMetrics,
                                                                      stateDirectory,
                                                                      streamsMetrics.taskCreatedSensor,
                                                                      changelogReader,
                                                                      cache,
                                                                      time,
                                                                      clientSupplier,
                                                                      threadProducer,
                                                                      threadClientId,
                                                                      rebalanceTimeoutMs,
                                                                      logPrefix);
        final AbstractTaskCreator standbyTaskCreator = new StandbyTaskCreator(builder,
                                                                              config,
                                                                              streamsMetrics,
                                                                              stateDirectory,
                                                                              streamsMetrics.taskCreatedSensor,
                                                                              changelogReader,
                                                                              time,
                                                                              rebalanceTimeoutMs,
                                                                              logPrefix);
        final TaskManager taskManager = new TaskManager(changelogReader, time, logPrefix, restoreConsumer, activeTaskCreator, standbyTaskCreator);

        return new StreamThread(builder,
                                clientId,
                                threadClientId,
                                config,
                                processId,
                                time,
                                streamsMetadataState,
                                taskManager,
                                streamsMetrics,
                                clientSupplier,
                                restoreConsumer,
                                stateDirectory);


    }

    private static String logPrefix(final String threadClientId) {
        return String.format("stream-thread [%s]", threadClientId);
    }

    /**
     * Execute the stream processors
     *
     * @throws KafkaException for any Kafka-related exceptions
     * @throws Exception      for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("{} Starting", logPrefix);
        setState(State.RUNNING);
        boolean cleanRun = false;
        try {
            runLoop();
            cleanRun = true;
        } catch (final KafkaException e) {
            // just re-throw the exception as it should be logged already
            throw e;
        } catch (final Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("{} Encountered the following error during processing:", logPrefix, e);
            throw e;
        } finally {
            shutdown(cleanRun);
        }
    }

    void setRebalanceException(final Throwable rebalanceException) {
        this.rebalanceException = rebalanceException;
    }

    /**
     * Main event loop for polling, and processing records through topologies.
     */
    private void runLoop() {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        consumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);
        lastCommitMs = time.milliseconds();
        while (stillRunning()) {
            timerStartedMs = time.milliseconds();

            // try to fetch some records if necessary
            final ConsumerRecords<byte[], byte[]> records = pollRequests();
            if (records != null && !records.isEmpty() && taskManager.hasActiveTasks()) {
                streamsMetrics.pollTimeSensor.record(computeLatency(), timerStartedMs);
                addRecordsToTasks(records);
                final long totalProcessed = processAndPunctuateStreamTime(taskManager.activeTasks(), recordsProcessedBeforeCommit);
                if (totalProcessed > 0) {
                    final long processLatency = computeLatency();
                    streamsMetrics.processTimeSensor.record(processLatency / (double) totalProcessed,
                        timerStartedMs);
                    recordsProcessedBeforeCommit = adjustRecordsProcessedBeforeCommit(recordsProcessedBeforeCommit, totalProcessed,
                        processLatency, commitTimeMs);
                }
            }

            maybePunctuateSystemTime();
            maybeCommit(timerStartedMs);
            maybeUpdateStandbyTasks(timerStartedMs);
        }
        log.info("{} Shutting down at user request", logPrefix);
    }

    /**
     * Get the next batch of records by polling.
     * @return Next batch of records or null if no records available.
     */
    private ConsumerRecords<byte[], byte[]> pollRequests() {
        ConsumerRecords<byte[], byte[]> records = null;

        try {
            records = consumer.poll(pollTimeMs);
        } catch (final InvalidOffsetException e) {
            resetInvalidOffsets(e);
        }

        if (rebalanceException != null) {
            if (!(rebalanceException instanceof ProducerFencedException)) {
                throw new StreamsException(logPrefix + " Failed to rebalance.", rebalanceException);
            }
        }

        return records;
    }

    private void resetInvalidOffsets(final InvalidOffsetException e) {
        final Set<TopicPartition> partitions = e.partitions();
        final Set<String> loggedTopics = new HashSet<>();
        final Set<TopicPartition> seekToBeginning = new HashSet<>();
        final Set<TopicPartition> seekToEnd = new HashSet<>();

        for (final TopicPartition partition : partitions) {
            if (builder.earliestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToBeginning, "{} Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "{} Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if (originalReset == null || (!originalReset.equals("earliest") && !originalReset.equals("latest"))) {
                    final String errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                        " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                        "policy via KStreamBuilder#stream(StreamsConfig.AutoOffsetReset offsetReset, ...) or KStreamBuilder#table(StreamsConfig.AutoOffsetReset offsetReset, ...)";
                    throw new StreamsException(String.format(errorMessage, partition.topic(), partition.partition()), e);
                }

                if (originalReset.equals("earliest")) {
                    addToResetList(partition, seekToBeginning, "{} No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else if (originalReset.equals("latest")) {
                    addToResetList(partition, seekToEnd, "{} No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                }
            }
        }

        if (!seekToBeginning.isEmpty()) {
            consumer.seekToBeginning(seekToBeginning);
        }
        if (!seekToEnd.isEmpty()) {
            consumer.seekToEnd(seekToEnd);
        }
    }

    private void addToResetList(final TopicPartition partition, final Set<TopicPartition> partitions, final String logMessage, final String resetPolicy, final Set<String> loggedTopics) {
        final String topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, logPrefix, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    /**
     * Take records and add them to each respective task
     * @param records Records, can be null
     */
    private void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        if (records != null && !records.isEmpty()) {
            int numAddedRecords = 0;

            for (final TopicPartition partition : records.partitions()) {
                final Task task = taskManager.activeTask(partition);
                numAddedRecords += task.addRecords(partition, records.records(partition));
            }
            streamsMetrics.skippedRecordsSensor.record(records.count() - numAddedRecords, timerStartedMs);
        }
    }

    /**
     * Schedule the records processing by selecting which record is processed next. Commits may
     * happen as records are processed.
     * @param tasks The tasks that have records.
     * @param recordsProcessedBeforeCommit number of records to be processed before commit is called.
     *                                     if UNLIMITED_RECORDS, then commit is never called
     * @return Number of records processed since last commit.
     */
    private long processAndPunctuateStreamTime(final Map<TaskId, Task> tasks,
                                               final long recordsProcessedBeforeCommit) {

        long totalProcessedEachRound;
        long totalProcessedSinceLastMaybeCommit = 0;
        // Round-robin scheduling by taking one record from each task repeatedly
        // until no task has any records left
        do {
            totalProcessedEachRound = 0;
            final Iterator<Map.Entry<TaskId, Task>> it = tasks.entrySet().iterator();
            while (it.hasNext()) {
                final Task task = it.next().getValue();
                try {
                    // we processed one record,
                    // if more are buffered waiting for the next round

                    // TODO: We should check for stream time punctuation right after each process call
                    //       of the task instead of only calling it after all records being processed
                    if (task.process()) {
                        totalProcessedEachRound++;
                        totalProcessedSinceLastMaybeCommit++;
                    }
                } catch (final ProducerFencedException e) {
                    taskManager.closeZombieTask(task);
                    it.remove();
                }
            }

            if (recordsProcessedBeforeCommit != UNLIMITED_RECORDS &&
                totalProcessedSinceLastMaybeCommit >= recordsProcessedBeforeCommit) {
                totalProcessedSinceLastMaybeCommit = 0;
                final long processLatency = computeLatency();
                streamsMetrics.processTimeSensor.record(processLatency / (double) totalProcessedSinceLastMaybeCommit,
                    timerStartedMs);
                maybeCommit(timerStartedMs);
            }
        } while (totalProcessedEachRound != 0);

        // go over the tasks again to punctuate or commit
        final RuntimeException e = taskManager.performOnActiveTasks(new TaskManager.TaskAction() {
            private String name;
            @Override
            public String name() {
                return name;
            }

            @Override
            public void apply(final Task task) {
                name = "punctuate";
                maybePunctuateStreamTime(task);
                if (task.commitNeeded()) {
                    name = "commit";

                    long beforeCommitMs = time.milliseconds();

                    commitOne(task);

                    if (log.isDebugEnabled()) {
                        log.debug("{} Committed active task {} per user request in {}ms",
                                logPrefix, task.id(), timerStartedMs - beforeCommitMs);
                    }
                }
            }
        });

        if (e != null) {
            throw e;
        }

        return totalProcessedSinceLastMaybeCommit;
    }

    private void maybePunctuateStreamTime(final Task task) {
        try {
            // check whether we should punctuate based on the task's partition group timestamp;
            // which are essentially based on record timestamp.
            if (task.maybePunctuateStreamTime()) {
                streamsMetrics.punctuateTimeSensor.record(computeLatency(), timerStartedMs);
            }
        } catch (final KafkaException e) {
            log.error("{} Failed to punctuate active task {} due to the following error:", logPrefix, task.id(), e);
            throw e;
        }
    }

    private void maybePunctuateSystemTime() {
        final RuntimeException e = taskManager.performOnActiveTasks(new TaskManager.TaskAction() {
            @Override
            public String name() {
                return "punctuate";
            }

            @Override
            public void apply(final Task task) {
                try {
                    // check whether we should punctuate based on system timestamp
                    if (task.maybePunctuateSystemTime()) {
                        streamsMetrics.punctuateTimeSensor.record(computeLatency(), timerStartedMs);
                    }
                } catch (final KafkaException e) {
                    log.error("{} Failed to punctuate active task {} due to the following error:", logPrefix, task.id(), e);
                    throw e;
                }
            }
        });

        if (e != null) {
            throw e;
        }
    }

    /**
     * Adjust the number of records that should be processed by scheduler. This avoids
     * scenarios where the processing time is higher than the commit time.
     * @param prevRecordsProcessedBeforeCommit Previous number of records processed by scheduler.
     * @param totalProcessed Total number of records processed in this last round.
     * @param processLatency Total processing latency in ms processed in this last round.
     * @param commitTime Desired commit time in ms.
     * @return An adjusted number of records to be processed in the next round.
     */
    private long adjustRecordsProcessedBeforeCommit(final long prevRecordsProcessedBeforeCommit, final long totalProcessed,
                                                    final long processLatency, final long commitTime) {
        long recordsProcessedBeforeCommit = UNLIMITED_RECORDS;
        // check if process latency larger than commit latency
        // note that once we set recordsProcessedBeforeCommit, it will never be UNLIMITED_RECORDS again, so
        // we will never process all records again. This might be an issue if the initial measurement
        // was off due to a slow start.
        if (processLatency > 0 && processLatency > commitTime) {
            // push down
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("{} processing latency {} > commit time {} for {} records. Adjusting down recordsProcessedBeforeCommit={}",
                logPrefix, processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        } else if (prevRecordsProcessedBeforeCommit != UNLIMITED_RECORDS && processLatency > 0) {
            // push up
            recordsProcessedBeforeCommit = Math.max(1, (commitTime * totalProcessed) / processLatency);
            log.debug("{} processing latency {} < commit time {} for {} records. Adjusting up recordsProcessedBeforeCommit={}",
                logPrefix, processLatency, commitTime, totalProcessed, recordsProcessedBeforeCommit);
        }

        return recordsProcessedBeforeCommit;
    }

    /**
     * Commit all tasks owned by this thread if specified interval time has elapsed
     */
    protected void maybeCommit(final long now) {
        if (commitTimeMs >= 0 && lastCommitMs + commitTimeMs < now) {
            if (log.isTraceEnabled()) {
                log.trace("{} Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                        logPrefix, taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            commitAll();

            if (log.isDebugEnabled()) {
                log.info("{} Committed all active tasks {} and standby tasks {} in {}ms",
                        logPrefix,  taskManager.activeTaskIds(), taskManager.standbyTaskIds(), timerStartedMs - now);
            }

            lastCommitMs = now;

            processStandbyRecords = true;
        }
    }

    /**
     * Commit the states of all its tasks
     */
    private void commitAll() {
        final TaskManager.TaskAction commitAction = new TaskManager.TaskAction() {
            @Override
            public String name() {
                return "commit";
            }

            @Override
            public void apply(final Task task) {
                commitOne(task);
            }
        };
        final RuntimeException e = taskManager.performOnActiveTasks(commitAction);
        if (e != null) {
            throw e;
        }

        final RuntimeException standbyTaskCommitException = taskManager.performOnStandbyTasks(commitAction);
        if (standbyTaskCommitException != null) {
            throw standbyTaskCommitException;
        }
    }

    /**
     * Commit the state of a task
     */
    private void commitOne(final Task task) {
        try {
            task.commit();
        } catch (final CommitFailedException e) {
            // commit failed. This is already logged inside the task as WARN and we can just log it again here.
            log.warn("{} Failed to commit {} {} state due to CommitFailedException; this task may be no longer owned by the thread", logPrefix, task.getClass().getSimpleName(), task.id());
        } catch (final KafkaException e) {
            // commit failed due to an unexpected exception. Log it and rethrow the exception.
            log.error("{} Failed to commit {} {} state due to the following error:", logPrefix, task.getClass().getSimpleName(), task.id(), e);
            throw e;
        }

        streamsMetrics.commitTimeSensor.record(computeLatency(), timerStartedMs);
    }

    private void maybeUpdateStandbyTasks(final long now) {
        if (taskManager.hasStandbyTasks()) {
            if (processStandbyRecords) {
                if (!standbyRecords.isEmpty()) {
                    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingStandbyRecords = new HashMap<>();

                    for (final Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : standbyRecords.entrySet()) {
                        final TopicPartition partition = entry.getKey();
                        List<ConsumerRecord<byte[], byte[]>> remaining = entry.getValue();
                        if (remaining != null) {
                            final Task task = taskManager.standbyTask(partition);
                            remaining = task.update(partition, remaining);
                            if (remaining != null) {
                                remainingStandbyRecords.put(partition, remaining);
                            } else {
                                restoreConsumer.resume(singleton(partition));
                            }
                        }
                    }

                    standbyRecords = remainingStandbyRecords;

                    log.debug("{} Updated standby tasks {} in {}ms", logPrefix, taskManager.standbyTaskIds(), time.milliseconds() - now);
                }
                processStandbyRecords = false;
            }

            final ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(0);

            if (!records.isEmpty()) {
                for (final TopicPartition partition : records.partitions()) {
                    final Task task = taskManager.standbyTask(partition);

                    if (task == null) {
                        throw new StreamsException(logPrefix + " Missing standby task for partition " + partition);
                    }

                    final List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                    if (remaining != null) {
                        restoreConsumer.pause(singleton(partition));
                        standbyRecords.put(partition, remaining);
                    }
                }
            }
        }
    }

    /**
     * Compute the latency based on the current marked timestamp, and update the marked timestamp
     * with the current system timestamp.
     *
     * @return latency
     */
    private long computeLatency() {
        final long previousTimeMs = timerStartedMs;
        timerStartedMs = time.milliseconds();

        return Math.max(timerStartedMs - previousTimeMs, 0);
    }

    /**
     * Shutdown this stream thread.
     * Note that there is nothing to prevent this function from being called multiple times
     * (e.g., in testing), hence the state is set only the first time
     */
    public synchronized void close() {
        log.info("{} Informed thread to shut down", logPrefix);
        setState(State.PENDING_SHUTDOWN);
    }

    public synchronized boolean isInitialized() {
        synchronized (stateLock) {
            return state == State.RUNNING;
        }
    }

    public synchronized boolean stillRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
    }

    public Map<TaskId, Task> tasks() {
        return Collections.unmodifiableMap(taskManager.activeTasks());
    }

    /**
     * Returns ids of tasks that were being executed before the rebalance.
     */
    public Set<TaskId> prevActiveTasks() {
        return taskManager.prevActiveTaskIds();
    }

    @Override
    public InternalTopologyBuilder builder() {
        return builder;
    }

    @Override
    public String name() {
        return getName();
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage.
     */
    public Set<TaskId> cachedTasks() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final HashSet<TaskId> tasks = new HashSet<>();

        final File[] stateDirs = stateDirectory.listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, ProcessorStateManager.CHECKPOINT_FILE_NAME).exists()) {
                        tasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    @Override
    public UUID processId() {
        return processId;
    }

    @Override
    public StreamsConfig config() {
        return config;
    }

    @Override
    public PartitionGrouper partitionGrouper() {
        return partitionGrouper;
    }

    /**
     * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
     * Kafka Streams and is not intended to be used by an external application.
     */
    public void setStateListener(final StreamThread.StateListener listener) {
        stateListener = listener;
    }

    /**
     * @return The state this instance is in
     */
    public State state() {
        synchronized (stateLock) {
            return state;
        }
    }


    /**
     * Sets the state
     * @param newState New state
     */
    void setState(final State newState) {
        synchronized (stateLock) {
            final State oldState = state;

            // there are cases when we shouldn't check if a transition is valid, e.g.,
            // when, for testing, a thread is closed multiple times. We could either
            // check here and immediately return for those cases, or add them to the transition
            // diagram (but then the diagram would be confusing and have transitions like
            // PENDING_SHUTDOWN->PENDING_SHUTDOWN).
            if (newState != State.DEAD && (state == State.PENDING_SHUTDOWN || state == State.DEAD)) {
                return;
            }

            if (!state.isValidTransition(newState)) {
                log.warn("{} Unexpected state transition from {} to {}", logPrefix, oldState, newState);
                throw new StreamsException(logPrefix + " Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("{} State transition from {} to {}.", logPrefix, oldState, newState);
            }

            state = newState;
            if (stateListener != null) {
                stateListener.onChange(this, state, oldState);
            }
        }
    }

    /**
     * Produces a string representation containing useful information about a StreamThread.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamThread instance.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder()
            .append(indent).append("StreamsThread appId: ").append(applicationId).append("\n")
            .append(indent).append("\tStreamsThread clientId: ").append(clientId).append("\n")
            .append(indent).append("\tStreamsThread threadId: ").append(getName()).append("\n");

        // iterate and print active tasks
        final TaskManager.TaskAction printAction = new TaskManager.TaskAction() {
            @Override
            public String name() {
                return "print";
            }

            @Override
            public void apply(final Task task) {
                sb.append(indent).append(task.toString(indent + "\t\t"));
            }
        };

        sb.append(indent).append("\tActive tasks:\n");
        taskManager.performOnActiveTasks(printAction);
        sb.append(indent).append("\tStandby tasks:\n");
        taskManager.performOnStandbyTasks(printAction);
        return sb.toString();
    }

    String threadClientId() {
        return getName();
    }

    public void setThreadMetadataProvider(final ThreadMetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
        taskManager.setThreadMetadataProvider(metadataProvider);
    }

    private void shutdown(final boolean cleanRun) {
        log.info("{} Shutting down", logPrefix);
        taskManager.shutdown(cleanRun);

        // close all embedded clients
        taskManager.closeProducer();

        try {
            consumer.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close consumer due to the following error:", logPrefix, e);
        }
        try {
            restoreConsumer.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close restore consumer due to the following error:", logPrefix, e);
        }

        taskManager.removeTasks();
        log.info("{} Stream thread shutdown complete", logPrefix);
        setState(State.DEAD);
        streamsMetrics.removeAllSensors();
    }


    private RuntimeException unAssignChangeLogPartitions() {
        try {
            // un-assign the change log partitions
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        } catch (final RuntimeException e) {
            log.error("{} Failed to un-assign change log partitions due to the following error:", logPrefix, e);
            return e;
        }
        return null;
    }

    private void clearStandbyRecords() {
        standbyRecords.clear();
    }

    private void refreshMetadataState() {
        streamsMetadataState.onChange(metadataProvider.getPartitionsByHostState(), metadataProvider.clusterMetadata());
    }
}
