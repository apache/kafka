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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

public class StreamThread extends Thread {

    private final Admin adminClient;

    /**
     * Stream thread states are the possible states that a stream thread can be in.
     * A thread must only be in one state at a time
     * The expected state transitions with the following defined states is:
     *
     * <pre>
     *                 +-------------+
     *          +<---- | Created (0) |
     *          |      +-----+-------+
     *          |            |
     *          |            v
     *          |      +-----+-------+
     *          +<---- | Starting (1)|----->+
     *          |      +-----+-------+      |
     *          |            |              |
     *          |            |              |
     *          |            v              |
     *          |      +-----+-------+      |
     *          +<---- | Partitions  |      |
     *          |      | Revoked (2) | <----+
     *          |      +-----+-------+      |
     *          |           |  ^            |
     *          |           |  |            |
     *          |           v  |            |
     *          |      +-----+-------+      |
     *          +<---- | Partitions  |      |
     *          |      | Assigned (3)| <----+
     *          |      +-----+-------+      |
     *          |            |              |
     *          |            |              |
     *          |            v              |
     *          |      +-----+-------+      |
     *          |      | Running (4) | ---->+
     *          |      +-----+-------+
     *          |            |
     *          |            |
     *          |            v
     *          |      +-----+-------+
     *          +----> | Pending     |
     *                 | Shutdown (5)|
     *                 +-----+-------+
     *                       |
     *                       v
     *                 +-----+-------+
     *                 | Dead (6)    |
     *                 +-------------+
     * </pre>
     *
     * Note the following:
     * <ul>
     *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
     *     <li>
     *         State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
     *         in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
     *         In this case we will forbid the transition but will not treat as an error.
     *     </li>
     *     <li>
     *         State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
     *         the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
     *         Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
     *         In this case we will allow the transition but it will be a no-op as the set of revoked partitions
     *         should be empty.
     *     </li>
     * </ul>
     */
    public enum State implements ThreadStateTransitionValidator {

        CREATED(1, 5),                    // 0
        STARTING(2, 3, 5),                // 1
        PARTITIONS_REVOKED(2, 3, 5),      // 2
        PARTITIONS_ASSIGNED(2, 3, 4, 5),  // 3
        RUNNING(2, 3, 5),                 // 4
        PENDING_SHUTDOWN(6),              // 5
        DEAD;                             // 6

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isAlive() {
            return equals(RUNNING) || equals(STARTING) || equals(PARTITIONS_REVOKED) || equals(PARTITIONS_ASSIGNED);
        }

        @Override
        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            final State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         *
         * @param thread   thread changing state
         * @param newState current state
         * @param oldState previous state
         */
        void onChange(final Thread thread, final ThreadStateTransitionValidator newState, final ThreadStateTransitionValidator oldState);
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
        // we do not need to use the state lock since the variable is volatile
        return state;
    }

    /**
     * Sets the state
     *
     * @param newState New state
     * @return The state prior to the call to setState, or null if the transition is invalid
     */
    State setState(final State newState) {
        final State oldState;

        synchronized (stateLock) {
            oldState = state;

            if (state == State.PENDING_SHUTDOWN && newState != State.DEAD) {
                log.debug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                              "only DEAD state is a valid next state", newState);
                // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                // refused but we do not throw exception here
                return null;
            } else if (state == State.DEAD) {
                log.debug("Ignoring request to transit from DEAD to {}: " +
                              "no valid next state after DEAD", newState);
                // when the state is already in NOT_RUNNING, all its transitions
                // will be refused but we do not throw exception here
                return null;
            } else if (!state.isValidTransition(newState)) {
                log.error("Unexpected state transition from {} to {}", oldState, newState);
                throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("State transition from {} to {}", oldState, newState);
            }

            state = newState;
            if (newState == State.RUNNING) {
                updateThreadMetadata(taskManager.activeTaskMap(), taskManager.standbyTaskMap());
            } else {
                updateThreadMetadata(Collections.emptyMap(), Collections.emptyMap());
            }
        }

        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }

        return oldState;
    }

    public boolean isRunning() {
        synchronized (stateLock) {
            return state.isAlive();
        }
    }

    int getAssignmentErrorCode() {
        return assignmentErrorCode.get();
    }

    static abstract class AbstractTaskCreator<T extends Task> {
        final String applicationId;
        final InternalTopologyBuilder builder;
        final StreamsConfig config;
        final StreamsMetricsImpl streamsMetrics;
        final StateDirectory stateDirectory;
        final ChangelogReader storeChangelogReader;
        final Time time;
        final Logger log;

        AbstractTaskCreator(final InternalTopologyBuilder builder,
                            final StreamsConfig config,
                            final StreamsMetricsImpl streamsMetrics,
                            final StateDirectory stateDirectory,
                            final ChangelogReader storeChangelogReader,
                            final Time time,
                            final Logger log) {
            this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            this.builder = builder;
            this.config = config;
            this.streamsMetrics = streamsMetrics;
            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
            this.time = time;
            this.log = log;
        }

        public InternalTopologyBuilder builder() {
            return builder;
        }

        public StateDirectory stateDirectory() {
            return stateDirectory;
        }

        Collection<T> createTasks(final Consumer<byte[], byte[]> consumer,
                                  final Map<TaskId, Set<TopicPartition>> tasksToBeCreated) {
            final List<T> createdTasks = new ArrayList<>();
            for (final Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
                final TaskId taskId = newTaskAndPartitions.getKey();
                final Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
                final T task = createTask(consumer, taskId, partitions);
                if (task != null) {
                    log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                    createdTasks.add(task);
                }

            }
            return createdTasks;
        }

        abstract T createTask(final Consumer<byte[], byte[]> consumer, final TaskId id, final Set<TopicPartition> partitions);

        void close() {}
    }

    static class TaskCreator extends AbstractTaskCreator<StreamTask> {
        private final String threadId;
        private final ThreadCache cache;
        private final Producer<byte[], byte[]> threadProducer;
        private final KafkaClientSupplier clientSupplier;
        final Map<TaskId, Producer<byte[], byte[]>> taskProducers;
        private final Sensor createTaskSensor;

        TaskCreator(final InternalTopologyBuilder builder,
                    final StreamsConfig config,
                    final StreamsMetricsImpl streamsMetrics,
                    final StateDirectory stateDirectory,
                    final ChangelogReader storeChangelogReader,
                    final ThreadCache cache,
                    final Time time,
                    final KafkaClientSupplier clientSupplier,
                    final Map<TaskId, Producer<byte[], byte[]>> taskProducers,
                    final String threadId,
                    final Logger log) {
            super(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log);

            final boolean eosEnabled = EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
            if (!eosEnabled) {
                final Map<String, Object> producerConfigs = config.getProducerConfigs(getThreadProducerClientId(threadId));
                log.info("Creating thread producer client");
                this.threadProducer = clientSupplier.getProducer(producerConfigs);
            } else {
                this.threadProducer = null;
            }
            this.taskProducers = taskProducers;

            this.cache = cache;
            this.threadId = threadId;
            this.clientSupplier = clientSupplier;

            this.createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        }

        @Override
        StreamTask createTask(final Consumer<byte[], byte[]> mainConsumer,
                              final TaskId taskId,
                              final Set<TopicPartition> partitions) {
            createTaskSensor.record();

            final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
            final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "task", taskId);
            final LogContext logContext = new LogContext(logPrefix);

            final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

            final ProcessorStateManager stateManager = new ProcessorStateManager(
                taskId,
                partitions,
                Task.TaskType.ACTIVE,
                stateDirectory,
                topology.storeToChangelogTopic(),
                storeChangelogReader,
                logContext);

            if (threadProducer == null) {
                // create one producer per task for EOS
                // TODO: after KIP-447 this would be removed
                final Map<String, Object> producerConfigs = config.getProducerConfigs(getTaskProducerClientId(threadId, taskId));
                producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + taskId);
                log.info("Creating producer client for task {}", taskId);
                taskProducers.put(taskId, clientSupplier.getProducer(producerConfigs));
            }
            final RecordCollector recordCollector = new RecordCollectorImpl(
                logContext,
                taskId,
                mainConsumer,
                threadProducer != null ?
                    new StreamsProducer(logContext, threadProducer) :
                    new StreamsProducer(logContext, taskProducers.get(taskId), applicationId, taskId),
                config.defaultProductionExceptionHandler(),
                EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
                streamsMetrics);

            return new StreamTask(
                taskId,
                partitions,
                topology,
                mainConsumer,
                config,
                streamsMetrics,
                stateDirectory,
                cache,
                time,
                stateManager,
                recordCollector);
        }

        public void close() {
            if (threadProducer != null) {
                try {
                    threadProducer.close();
                } catch (final Throwable e) {
                    log.error("Failed to close producer due to the following error:", e);
                }
            }
        }
    }

    static class StandbyTaskCreator extends AbstractTaskCreator<StandbyTask> {
        private final Sensor createTaskSensor;

        StandbyTaskCreator(final InternalTopologyBuilder builder,
                           final StreamsConfig config,
                           final StreamsMetricsImpl streamsMetrics,
                           final StateDirectory stateDirectory,
                           final ChangelogReader storeChangelogReader,
                           final Time time,
                           final String threadId,
                           final Logger log) {
            super(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log);
            createTaskSensor = ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        }

        @Override
        StandbyTask createTask(final Consumer<byte[], byte[]> consumer,
                               final TaskId taskId,
                               final Set<TopicPartition> partitions) {
            createTaskSensor.record();

            final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
            final String logPrefix = threadIdPrefix + String.format("%s [%s] ", "standby-task", taskId);
            final LogContext logContext = new LogContext(logPrefix);

            final ProcessorTopology topology = builder.buildSubtopology(taskId.topicGroupId);

            if (topology.hasStateWithChangelogs()) {
                final ProcessorStateManager stateManager = new ProcessorStateManager(
                    taskId,
                    partitions,
                    Task.TaskType.STANDBY,
                    stateDirectory,
                    topology.storeToChangelogTopic(),
                    storeChangelogReader,
                    logContext);

                return new StandbyTask(
                    taskId,
                    partitions,
                    topology,
                    config,
                    streamsMetrics,
                    stateManager,
                    stateDirectory);
            } else {
                log.trace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
                return null;
            }
        }
    }

    private final Time time;
    private final Logger log;
    private final String logPrefix;
    private final Object stateLock;
    private final Duration pollTime;
    private final long commitTimeMs;
    private final int maxPollTimeMs;
    private final String originalReset;
    private final TaskManager taskManager;
    private final AtomicInteger assignmentErrorCode;

    private final StreamsMetricsImpl streamsMetrics;
    private final Sensor commitSensor;
    private final Sensor pollSensor;
    private final Sensor punctuateSensor;
    private final Sensor processLatencySensor;
    private final Sensor processRateSensor;

    private long now;
    private long lastPollMs;
    private long lastCommitMs;
    private int numIterations;
    private volatile State state = State.CREATED;
    private volatile ThreadMetadata threadMetadata;
    private StreamThread.StateListener stateListener;

    private final ChangelogReader changelogReader;

    // package-private for testing
    final ConsumerRebalanceListener rebalanceListener;
    final Consumer<byte[], byte[]> mainConsumer;
    final Consumer<byte[], byte[]> restoreConsumer;
    final Producer<byte[], byte[]> threadProducer;
    final Map<TaskId, Producer<byte[], byte[]>> taskProducers;
    final InternalTopologyBuilder builder;

    public static StreamThread create(final InternalTopologyBuilder builder,
                                      final StreamsConfig config,
                                      final KafkaClientSupplier clientSupplier,
                                      final Admin adminClient,
                                      final UUID processId,
                                      final String clientId,
                                      final StreamsMetricsImpl streamsMetrics,
                                      final Time time,
                                      final StreamsMetadataState streamsMetadataState,
                                      final long cacheSizeBytes,
                                      final StateDirectory stateDirectory,
                                      final StateRestoreListener userStateRestoreListener,
                                      final int threadIdx) {
        final String threadId = clientId + "-StreamThread-" + threadIdx;

        final String logPrefix = String.format("stream-thread [%s] ", threadId);
        final LogContext logContext = new LogContext(logPrefix);
        final Logger log = logContext.logger(StreamThread.class);

        log.info("Creating restore consumer client");
        final Map<String, Object> restoreConsumerConfigs = config.getRestoreConsumerConfigs(getRestoreConsumerClientId(threadId));
        final Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);

        final StoreChangelogReader changelogReader = new StoreChangelogReader(
            time,
            config,
            logContext,
            restoreConsumer,
            userStateRestoreListener);

        final ThreadCache cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);

        final Map<TaskId, Producer<byte[], byte[]>> taskProducers = new HashMap<>();

        // TODO: refactor `TaskCreator` into `TaskManager`;
        //  this will allow to reduce the surface area of `taskProducers` that is passed to many classes atm
        final TaskCreator activeTaskCreator = new TaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            cache,
            time,
            clientSupplier,
            taskProducers,
            threadId,
            log);
        final StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            time,
            threadId,
            log);
        final TaskManager taskManager = new TaskManager(
            changelogReader,
            processId,
            logPrefix,
            streamsMetrics,
            activeTaskCreator,
            standbyTaskCreator,
            taskProducers,
            builder,
            adminClient
        );

        log.info("Creating consumer client");
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final Map<String, Object> consumerConfigs = config.getMainConsumerConfigs(applicationId, getConsumerClientId(threadId), threadIdx);
        consumerConfigs.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        consumerConfigs.put(StreamsConfig.InternalConfig.STREAMS_METADATA_STATE_FOR_PARTITION_ASSIGNOR, streamsMetadataState);
        consumerConfigs.put(StreamsConfig.InternalConfig.STREAMS_ADMIN_CLIENT, adminClient);
        final AtomicInteger assignmentErrorCode = new AtomicInteger();
        consumerConfigs.put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode);
        String originalReset = null;
        if (!builder.latestResetTopicsPattern().pattern().equals("") || !builder.earliestResetTopicsPattern().pattern().equals("")) {
            originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        final Consumer<byte[], byte[]> mainConsumer = clientSupplier.getConsumer(consumerConfigs);
        changelogReader.setMainConsumer(mainConsumer);
        taskManager.setMainConsumer(mainConsumer);

        final StreamThread streamThread = new StreamThread(
            time,
            config,
            activeTaskCreator.threadProducer,
            taskProducers,
            adminClient,
            mainConsumer,
            restoreConsumer,
            changelogReader,
            originalReset,
            taskManager,
            streamsMetrics,
            builder,
            threadId,
            logContext,
            assignmentErrorCode);

        return streamThread.updateThreadMetadata(getSharedAdminClientId(clientId));
    }

    public StreamThread(final Time time,
                        final StreamsConfig config,
                        final Producer<byte[], byte[]> threadProducer,
                        final Map<TaskId, Producer<byte[], byte[]>> taskProducers,
                        final Admin adminClient,
                        final Consumer<byte[], byte[]> mainConsumer,
                        final Consumer<byte[], byte[]> restoreConsumer,
                        final ChangelogReader changelogReader,
                        final String originalReset,
                        final TaskManager taskManager,
                        final StreamsMetricsImpl streamsMetrics,
                        final InternalTopologyBuilder builder,
                        final String threadId,
                        final LogContext logContext,
                        final AtomicInteger assignmentErrorCode) {
        super(threadId);
        this.stateLock = new Object();

        this.adminClient = adminClient;
        this.streamsMetrics = streamsMetrics;
        this.commitSensor = ThreadMetrics.commitSensor(threadId, streamsMetrics);
        this.pollSensor = ThreadMetrics.pollSensor(threadId, streamsMetrics);
        this.processLatencySensor = ThreadMetrics.processLatencySensor(threadId, streamsMetrics);
        this.processRateSensor = ThreadMetrics.processRateSensor(threadId, streamsMetrics);
        this.punctuateSensor = ThreadMetrics.punctuateSensor(threadId, streamsMetrics);

        // The following sensors are created here but their references are not stored in this object, since within
        // this object they are not recorded. The sensors are created here so that the stream threads starts with all
        // its metrics initialised. Otherwise, those sensors would have been created during processing, which could
        // lead to missing metrics. For instance, if no task were created, the metrics for created and closed
        // tasks would never be added to the metrics.
        ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            ThreadMetrics.skipRecordSensor(threadId, streamsMetrics);
            ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
        }

        this.time = time;
        this.builder = builder;
        this.logPrefix = logContext.logPrefix();
        this.log = logContext.logger(StreamThread.class);
        this.rebalanceListener = new StreamsRebalanceListener(time, taskManager, this, this.log);
        this.taskManager = taskManager;
        this.restoreConsumer = restoreConsumer;
        this.mainConsumer = mainConsumer;
        this.threadProducer = threadProducer;
        this.taskProducers = taskProducers;
        this.changelogReader = changelogReader;
        this.originalReset = originalReset;
        this.assignmentErrorCode = assignmentErrorCode;

        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        final int dummyThreadIdx = 1;
        this.maxPollTimeMs = new InternalConsumerConfig(config.getMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
            .getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        this.numIterations = 1;
    }

    private static final class InternalConsumerConfig extends ConsumerConfig {
        private InternalConsumerConfig(final Map<String, Object> props) {
            super(ConsumerConfig.addDeserializerToConfig(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), false);
        }
    }

    private static String getTaskProducerClientId(final String threadClientId, final TaskId taskId) {
        return threadClientId + "-" + taskId + "-producer";
    }

    private static String getThreadProducerClientId(final String threadClientId) {
        return threadClientId + "-producer";
    }

    private static String getConsumerClientId(final String threadClientId) {
        return threadClientId + "-consumer";
    }

    private static String getRestoreConsumerClientId(final String threadClientId) {
        return threadClientId + "-restore-consumer";
    }

    // currently admin client is shared among all threads
    public static String getSharedAdminClientId(final String clientId) {
        return clientId + "-admin";
    }

    /**
     * Execute the stream processors
     *
     * @throws KafkaException   for any Kafka-related exceptions
     * @throws RuntimeException for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("Starting");
        if (setState(State.STARTING) == null) {
            log.info("StreamThread already shutdown. Not running");
            return;
        }
        boolean cleanRun = false;
        try {
            runLoop();
            cleanRun = true;
        } catch (final Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("Encountered the following exception during processing " +
                "and the thread is going to shut down: ", e);
            throw e;
        } finally {
            completeShutdown(cleanRun);
        }
    }

    /**
     * Main event loop for polling, and processing records through topologies.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      if the store's change log does not contain the partition
     */
    private void runLoop() {
        subscribeConsumer();

        // if the thread is still in the middle of a rebalance, we should keep polling
        // until the rebalance is completed before we close and commit the tasks
        while (isRunning() || taskManager.isRebalanceInProgress()) {
            try {
                runOnce();
                if (assignmentErrorCode.get() == AssignorError.VERSION_PROBING.code()) {
                    log.info("Version probing detected. Rejoining the consumer group to trigger a new rebalance.");

                    assignmentErrorCode.set(AssignorError.NONE.code());
                    mainConsumer.enforceRebalance();
                }
            } catch (final TaskCorruptedException e) {
                log.warn("Detected the states of tasks {} are corrupted. " +
                    "Will close the task as dirty and re-create and bootstrap from scratch.", e.corruptedTaskWithChangelogs());

                taskManager.handleCorruption(e.corruptedTaskWithChangelogs());
            } catch (final TaskMigratedException e) {
                log.warn("Detected that the thread is being fenced. " +
                    "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                    "Will close out all assigned tasks and rejoin the consumer group.");

                taskManager.handleLostAll();
                mainConsumer.enforceRebalance();
            }
        }
    }
    
    private void subscribeConsumer() {
        if (builder.usesPatternSubscription()) {
            mainConsumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);
        } else {
            mainConsumer.subscribe(builder.sourceTopicCollection(), rebalanceListener);
        }
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      If the store's change log does not contain the partition
     * @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
     *                               or if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // Visible for testing
    void runOnce() {
        final ConsumerRecords<byte[], byte[]> records;
        now = time.milliseconds();

        if (state == State.PARTITIONS_ASSIGNED) {
            // try to fetch some records with zero poll millis
            // to unblock the restoration as soon as possible
            records = pollRequests(Duration.ZERO);
        } else if (state == State.PARTITIONS_REVOKED) {
            // try to fetch som records with zero poll millis to unblock
            // other useful work while waiting for the join response
            records = pollRequests(Duration.ZERO);
        } else if (state == State.RUNNING || state == State.STARTING) {
            // try to fetch some records with normal poll time
            // in order to get long polling
            records = pollRequests(pollTime);
        } else if (state == State.PENDING_SHUTDOWN) {
            // we are only here because there's rebalance in progress,
            // just poll with zero to complete it
            records = pollRequests(Duration.ZERO);
        } else {
            // any other state should not happen
            log.error("Unexpected state {} during normal iteration", state);
            throw new StreamsException(logPrefix + "Unexpected state " + state + " during normal iteration");
        }

        final long pollLatency = advanceNowAndComputeLatency();

        if (records != null && !records.isEmpty()) {
            pollSensor.record(pollLatency, now);
            addRecordsToTasks(records);
        }

        // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
        // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
        // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
        // could affect the task manager state beyond this point within #runOnce().
        if (!isRunning()) {
            log.debug("State already transits to {}, skipping the run once call after poll request", state);
            return;
        }

        // only try to initialize the assigned tasks
        // if the state is still in PARTITION_ASSIGNED after the poll call
        if (state == State.PARTITIONS_ASSIGNED) {
            if (taskManager.tryToCompleteRestoration()) {
                changelogReader.transitToUpdateStandby();

                setState(State.RUNNING);
            } else {
                // transit to restore active is idempotent so we can call it multiple times
                changelogReader.transitToRestoreActive();
            }
        }

        // we can always let changelog reader try restoring in order to initialize the changelogs;
        // if there's no active restoring or standby updating it would not try to fetch any data
        changelogReader.restore();

        advanceNowAndComputeLatency();

        if (state == State.RUNNING) {
            /*
             * Within an iteration, after N (N initialized as 1 upon start up) round of processing one-record-each on the applicable tasks, check the current time:
             *  1. If it is time to commit, do it;
             *  2. If it is time to punctuate, do it;
             *  3. If elapsed time is close to consumer's max.poll.interval.ms, end the current iteration immediately.
             *  4. If none of the the above happens, increment N.
             *  5. If one of the above happens, half the value of N.
             */
            int processed = 0;
            long timeSinceLastPoll;

            do {
                for (int i = 0; i < numIterations; i++) {
                    advanceNowAndComputeLatency();
                    processed = taskManager.process(now);

                    if (processed > 0) {
                        // It makes no difference to the outcome of these metrics when we record "0",
                        // so we can just avoid the method call when we didn't process anything.
                        processRateSensor.record(processed, now);

                        // This metric is scaled to represent the _average_ processing time of _each_
                        // task. Note, it's hard to interpret this as defined, but we would need a KIP
                        // to change it to simply report the overall time spent processing all tasks.
                        final long processLatency = advanceNowAndComputeLatency();
                        processLatencySensor.record(processLatency / (double) processed, now);

                        // commit any tasks that have requested a commit
                        final int committed = taskManager.maybeCommitActiveTasksPerUserRequested();

                        if (committed > 0) {
                            final long commitLatency = advanceNowAndComputeLatency();
                            commitSensor.record(commitLatency / (double) committed, now);
                        }
                    } else {
                        // if there is no records to be processed, exit immediately
                        break;
                    }
                }

                timeSinceLastPoll = Math.max(now - lastPollMs, 0);

                if (maybePunctuate() || maybeCommit()) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                } else if (timeSinceLastPoll > maxPollTimeMs / 2) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                    break;
                } else if (processed > 0) {
                    numIterations++;
                }
            } while (processed > 0);

            maybeCommit();
        }
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private ConsumerRecords<byte[], byte[]> pollRequests(final Duration pollTime) {
        ConsumerRecords<byte[], byte[]> records = null;

        lastPollMs = now;

        try {
            records = mainConsumer.poll(pollTime);
        } catch (final InvalidOffsetException e) {
            resetInvalidOffsets(e);
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
                addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if (originalReset == null || (!originalReset.equals("earliest") && !originalReset.equals("latest"))) {
                    final String errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                        " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                        "policy via StreamsBuilder#stream(..., Consumed.with(Topology.AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.with(Topology.AutoOffsetReset))";
                    throw new StreamsException(String.format(errorMessage, partition.topic(), partition.partition()), e);
                }

                if (originalReset.equals("earliest")) {
                    addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else { // can only be "latest"
                    addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                }
            }
        }

        if (!seekToBeginning.isEmpty()) {
            mainConsumer.seekToBeginning(seekToBeginning);
        }
        if (!seekToEnd.isEmpty()) {
            mainConsumer.seekToEnd(seekToEnd);
        }
    }

    private void addToResetList(final TopicPartition partition, final Set<TopicPartition> partitions, final String logMessage, final String resetPolicy, final Set<String> loggedTopics) {
        final String topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    /**
     * Take records and add them to each respective task
     *
     * @param records Records, can be null
     */
    private void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        for (final TopicPartition partition : records.partitions()) {
            final Task task = taskManager.taskForInputPartition(partition);

            if (task == null) {
                log.error("Unable to locate active task for received-record partition {}. Current tasks: {}",
                    partition, taskManager.toString(">"));
                throw new NullPointerException("Task was unexpectedly missing for partition " + partition);
            }

            task.addRecords(partition, records.records(partition));
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private boolean maybePunctuate() {
        final int punctuated = taskManager.punctuate();
        if (punctuated > 0) {
            final long punctuateLatency = advanceNowAndComputeLatency();
            punctuateSensor.record(punctuateLatency / (double) punctuated, now);
        }

        return punctuated > 0;
    }

    /**
     * Try to commit all active tasks owned by this thread.
     *
     * Visible for testing.
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    boolean maybeCommit() {
        final int committed;

        if (now - lastCommitMs > commitTimeMs) {
            if (log.isTraceEnabled()) {
                log.trace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                          taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            committed = taskManager.commitAll();
            if (committed > 0) {
                final long intervalCommitLatency = advanceNowAndComputeLatency();
                commitSensor.record(intervalCommitLatency / (double) committed, now);

                // try to purge the committed records for repartition topics if possible
                taskManager.maybePurgeCommittedRecords();

                if (log.isDebugEnabled()) {
                    log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                              taskManager.activeTaskIds(), taskManager.standbyTaskIds(), intervalCommitLatency);
                }
            }

            if (committed == -1) {
                log.trace("Unable to commit as we are in the middle of a rebalance, will try again when it completes.");
            } else {
                lastCommitMs = now;
            }
        } else {
            committed = taskManager.maybeCommitActiveTasksPerUserRequested();
            if (committed > 0) {
                final long requestCommitLatency = advanceNowAndComputeLatency();
                commitSensor.record(requestCommitLatency / (double) committed, now);
            }
        }

        return committed > 0;
    }

    /**
     * Compute the latency based on the current marked timestamp, and update the marked timestamp
     * with the current system timestamp.
     *
     * @return latency
     */
    private long advanceNowAndComputeLatency() {
        final long previous = now;
        now = time.milliseconds();

        return Math.max(now - previous, 0);
    }

    /**
     * Shutdown this stream thread.
     * <p>
     * Note that there is nothing to prevent this function from being called multiple times
     * (e.g., in testing), hence the state is set only the first time
     */
    public void shutdown() {
        log.info("Informed to shut down");
        final State oldState = setState(State.PENDING_SHUTDOWN);
        if (oldState == State.CREATED) {
            // The thread may not have been started. Take responsibility for shutting down
            completeShutdown(true);
        }
    }

    private void completeShutdown(final boolean cleanRun) {
        // set the state to pending shutdown first as it may be called due to error;
        // its state may already be PENDING_SHUTDOWN so it will return false but we
        // intentionally do not check the returned flag
        setState(State.PENDING_SHUTDOWN);

        log.info("Shutting down");

        try {
            taskManager.shutdown(cleanRun);
        } catch (final Throwable e) {
            log.error("Failed to close task manager due to the following error:", e);
        }
        try {
            changelogReader.clear();
        } catch (final Throwable e) {
            log.error("Failed to close changelog reader due to the following error:", e);
        }
        try {
            mainConsumer.close();
        } catch (final Throwable e) {
            log.error("Failed to close consumer due to the following error:", e);
        }
        try {
            restoreConsumer.close();
        } catch (final Throwable e) {
            log.error("Failed to close restore consumer due to the following error:", e);
        }
        streamsMetrics.removeAllThreadLevelSensors(getName());

        setState(State.DEAD);
        log.info("Shutdown complete");
    }

    /**
     * Return information about the current {@link StreamThread}.
     *
     * @return {@link ThreadMetadata}.
     */
    public final ThreadMetadata threadMetadata() {
        return threadMetadata;
    }

    // package-private for testing only
    StreamThread updateThreadMetadata(final String adminClientId) {

        threadMetadata = new ThreadMetadata(
            this.getName(),
            this.state().name(),
            getConsumerClientId(this.getName()),
            getRestoreConsumerClientId(this.getName()),
            threadProducer == null ?
                Collections.emptySet() :
                Collections.singleton(getThreadProducerClientId(this.getName())),
            adminClientId,
            Collections.emptySet(),
            Collections.emptySet());

        return this;
    }

    private void updateThreadMetadata(final Map<TaskId, Task> activeTasks,
                                      final Map<TaskId, Task> standbyTasks) {
        final Set<String> producerClientIds = new HashSet<>();
        final Set<TaskMetadata> activeTasksMetadata = new HashSet<>();
        for (final Map.Entry<TaskId, Task> task : activeTasks.entrySet()) {
            activeTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().inputPartitions()));
            producerClientIds.add(getTaskProducerClientId(getName(), task.getKey()));
        }
        final Set<TaskMetadata> standbyTasksMetadata = new HashSet<>();
        for (final Map.Entry<TaskId, Task> task : standbyTasks.entrySet()) {
            standbyTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().inputPartitions()));
        }

        final String adminClientId = threadMetadata.adminClientId();
        threadMetadata = new ThreadMetadata(
            this.getName(),
            this.state().name(),
            getConsumerClientId(this.getName()),
            getRestoreConsumerClientId(this.getName()),
            threadProducer == null ?
                producerClientIds :
                Collections.singleton(getThreadProducerClientId(this.getName())),
            adminClientId,
            activeTasksMetadata,
            standbyTasksMetadata);
    }

    public Map<TaskId, Task> activeTaskMap() {
        return taskManager.activeTaskMap();
    }

    public List<Task> activeTasks() {
        return taskManager.activeTaskIterable();
    }

    public Map<TaskId, Task> allTasks() {
        return taskManager.tasks();
    }

    /**
     * Produces a string representation containing useful information about a StreamThread.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    public String toString(final String indent) {
        return indent + "\tStreamsThread threadId: " + getName() + "\n" + taskManager.toString(indent);
    }

    public Map<MetricName, Metric> producerMetrics() {
        final LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        if (threadProducer != null) {
            final Map<MetricName, ? extends Metric> producerMetrics = threadProducer.metrics();
            if (producerMetrics != null) {
                result.putAll(producerMetrics);
            }
        } else {
            // When EOS is turned on, each task will have its own producer client
            // and the producer object passed in here will be null. We would then iterate through
            // all the active tasks and add their metrics to the output metrics map.
            for (final StreamTask task : taskManager.fixmeStreamTasks().values()) {
                final Map<MetricName, ? extends Metric> taskProducerMetrics = taskProducers.get(task.id).metrics();
                result.putAll(taskProducerMetrics);
            }
        }
        return result;
    }

    public Map<MetricName, Metric> consumerMetrics() {
        final Map<MetricName, ? extends Metric> consumerMetrics = mainConsumer.metrics();
        final Map<MetricName, ? extends Metric> restoreConsumerMetrics = restoreConsumer.metrics();
        final LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        result.putAll(consumerMetrics);
        result.putAll(restoreConsumerMetrics);
        return result;
    }

    public Map<MetricName, Metric> adminClientMetrics() {
        final Map<MetricName, ? extends Metric> adminClientMetrics = adminClient.metrics();
        return new LinkedHashMap<>(adminClientMetrics);
    }

    // the following are for testing only
    void setNow(final long now) {
        this.now = now;
    }

    TaskManager taskManager() {
        return taskManager;
    }

    int currentNumIterations() {
        return numIterations;
    }
}
