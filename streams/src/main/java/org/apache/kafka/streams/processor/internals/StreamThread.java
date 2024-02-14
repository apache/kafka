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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.metrics.ClientMetrics;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager.DefaultTaskExecutorCreator;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.util.HashMap;
import java.util.Queue;
import java.util.function.BiConsumer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.eosEnabled;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.processingMode;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getConsumerClientId;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getRestoreConsumerClientId;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getSharedAdminClientId;

public class StreamThread extends Thread implements ProcessingThread {

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
     *          |                           |
     *          |            +<----------+  |
     *          |            |           |  |
     *          |            v           |  |
     *          |      +-----+-------+   |  |
     *          +<---- | Partitions  | --+  |
     *          |      | Revoked (2) | <----+
     *          |      +-----+-------+      |
     *          |           |  ^            |
     *          |           v  |            |
     *          |      +-----+-------+      |
     *          +<---- | Partitions  |      |
     *          |      | Assigned (3)| <----+
     *          |      +-----+-------+      |
     *          |            |              |
     *          |            +<----------+  |
     *          |            |           |  |
     *          |            v           |  |
     *          |      +-----+-------+   |  |
     *          |      |             | --+  |
     *          |      | Running (4) | ---->+
     *          |      +-----+-------+
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
        RUNNING(2, 3, 4, 5),              // 4
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

    public StreamThread.StateListener getStateListener() {
        return stateListener;
    }

    /**
     * @return The state this instance is in
     */
    public State state() {
        // we do not need to use the state lock since the variable is volatile
        return state;
    }

    void setPartitionAssignedTime(final long lastPartitionAssignedMs) {
        this.lastPartitionAssignedMs = lastPartitionAssignedMs;
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
            }

            stateLock.notifyAll();
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

    public boolean isStartingRunningOrPartitionAssigned() {
        synchronized (stateLock) {
            return state.equals(State.RUNNING) || state.equals(State.STARTING) || state.equals(State.PARTITIONS_ASSIGNED);
        }
    }

    private final Time time;
    private final Logger log;
    private final String logPrefix;
    public final Object stateLock;
    private final Duration pollTime;
    private final long commitTimeMs;
    private final long purgeTimeMs;
    private final int maxPollTimeMs;
    private final String originalReset;
    private final TaskManager taskManager;
    private final StateUpdater stateUpdater;

    private final StreamsMetricsImpl streamsMetrics;
    private final Sensor commitSensor;
    private final Sensor pollSensor;
    private final Sensor pollRecordsSensor;
    private final Sensor punctuateSensor;
    private final Sensor processRecordsSensor;
    private final Sensor processLatencySensor;
    private final Sensor processRateSensor;
    private final Sensor pollRatioSensor;
    private final Sensor processRatioSensor;
    private final Sensor punctuateRatioSensor;
    private final Sensor commitRatioSensor;
    private final Sensor failedStreamThreadSensor;

    private static final long LOG_SUMMARY_INTERVAL_MS = 2 * 60 * 1000L; // log a summary of processing every 2 minutes
    private long lastLogSummaryMs = -1L;
    private long totalRecordsProcessedSinceLastSummary = 0L;
    private long totalPunctuatorsSinceLastSummary = 0L;
    private long totalCommittedSinceLastSummary = 0L;

    private long now;
    private long lastPollMs;
    private long lastCommitMs;
    private long lastPurgeMs;
    private long lastPartitionAssignedMs = -1L;
    private int numIterations;
    private volatile State state = State.CREATED;
    private volatile ThreadMetadata threadMetadata;
    private StreamThread.StateListener stateListener;
    private final Optional<String> getGroupInstanceID;

    private final ChangelogReader changelogReader;
    private final ConsumerRebalanceListener rebalanceListener;
    private final Consumer<byte[], byte[]> mainConsumer;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final Admin adminClient;
    private final TopologyMetadata topologyMetadata;
    private final java.util.function.Consumer<Long> cacheResizer;

    private BiConsumer<Throwable, Boolean> streamsUncaughtExceptionHandler;
    private final Runnable shutdownErrorHook;

    // These must be Atomic references as they are shared and used to signal between the assignor and the stream thread
    private final AtomicInteger assignmentErrorCode;
    private final AtomicLong nextProbingRebalanceMs;
    // recoverable errors (don't require killing thread) that we need to invoke the exception
    // handler for, eg MissingSourceTopicException with named topologies
    private final Queue<StreamsException> nonFatalExceptionsToHandle;

    // These are used to signal from outside the stream thread, but the variables themselves are internal to the thread
    private final AtomicLong cacheResizeSize = new AtomicLong(-1L);
    private final AtomicBoolean leaveGroupRequested = new AtomicBoolean(false);
    private final boolean eosEnabled;
    private final StreamsConfigUtils.ProcessingMode processingMode;
    private final boolean stateUpdaterEnabled;
    private final boolean processingThreadsEnabled;

    private volatile long fetchDeadlineClientInstanceId = -1;
    private volatile KafkaFutureImpl<Uuid> mainConsumerInstanceIdFuture = new KafkaFutureImpl<>();
    private volatile KafkaFutureImpl<Uuid> restoreConsumerInstanceIdFuture = new KafkaFutureImpl<>();
    private volatile KafkaFutureImpl<Map<String, KafkaFuture<Uuid>>> producerInstanceIdFuture = new KafkaFutureImpl<>();
    private volatile KafkaFutureImpl<Uuid> threadProducerInstanceIdFuture = new KafkaFutureImpl<>();

    public static StreamThread create(final TopologyMetadata topologyMetadata,
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
                                      final StandbyUpdateListener userStandbyUpdateListener,
                                      final int threadIdx,
                                      final Runnable shutdownErrorHook,
                                      final BiConsumer<Throwable, Boolean> streamsUncaughtExceptionHandler) {
        final String threadId = clientId + "-StreamThread-" + threadIdx;

        final String logPrefix = String.format("stream-thread [%s] ", threadId);
        final LogContext logContext = new LogContext(logPrefix);
        final Logger log = logContext.logger(StreamThread.class);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        referenceContainer.adminClient = adminClient;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        referenceContainer.clientTags = config.getClientTags();

        log.info("Creating restore consumer client");
        final Map<String, Object> restoreConsumerConfigs = config.getRestoreConsumerConfigs(getRestoreConsumerClientId(threadId));
        final Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);

        final StoreChangelogReader changelogReader = new StoreChangelogReader(
            time,
            config,
            logContext,
            adminClient,
            restoreConsumer,
            userStateRestoreListener,
            userStandbyUpdateListener
        );

        final ThreadCache cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);

        final boolean stateUpdaterEnabled = InternalConfig.getStateUpdaterEnabled(config.originals());
        final boolean proceessingThreadsEnabled = InternalConfig.getProcessingThreadsEnabled(config.originals());
        final ActiveTaskCreator activeTaskCreator = new ActiveTaskCreator(
            topologyMetadata,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            cache,
            time,
            clientSupplier,
            threadId,
            processId,
            log,
            stateUpdaterEnabled,
            proceessingThreadsEnabled
        );
        final StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
            topologyMetadata,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            threadId,
            log,
            stateUpdaterEnabled);

        final Tasks tasks = new Tasks(new LogContext(logPrefix));
        final boolean processingThreadsEnabled =
            InternalConfig.getProcessingThreadsEnabled(config.originals());

        final DefaultTaskManager schedulingTaskManager =
            maybeCreateSchedulingTaskManager(processingThreadsEnabled, stateUpdaterEnabled, topologyMetadata, time, threadId, tasks);
        final StateUpdater stateUpdater =
            maybeCreateAndStartStateUpdater(
                stateUpdaterEnabled,
                streamsMetrics,
                config,
                restoreConsumer,
                changelogReader,
                topologyMetadata,
                time,
                clientId,
                threadIdx
            );

        final TaskManager taskManager = new TaskManager(
            time,
            changelogReader,
            processId,
            logPrefix,
            activeTaskCreator,
            standbyTaskCreator,
            tasks,
            topologyMetadata,
            adminClient,
            stateDirectory,
            stateUpdater,
            schedulingTaskManager
        );
        referenceContainer.taskManager = taskManager;

        log.info("Creating consumer client");
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final Map<String, Object> consumerConfigs = config.getMainConsumerConfigs(applicationId, getConsumerClientId(threadId), threadIdx);
        consumerConfigs.put(StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);

        final String originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        // If there are any overrides, we never fall through to the consumer, but only handle offset management ourselves.
        if (topologyMetadata.hasOffsetResetOverrides()) {
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        final Consumer<byte[], byte[]> mainConsumer = clientSupplier.getConsumer(consumerConfigs);
        taskManager.setMainConsumer(mainConsumer);
        referenceContainer.mainConsumer = mainConsumer;

        final StreamThread streamThread = new StreamThread(
            time,
            config,
            adminClient,
            mainConsumer,
            restoreConsumer,
            changelogReader,
            originalReset,
            taskManager,
            stateUpdater,
            streamsMetrics,
            topologyMetadata,
            threadId,
            logContext,
            referenceContainer.assignmentErrorCode,
            referenceContainer.nextScheduledRebalanceMs,
            referenceContainer.nonFatalExceptionsToHandle,
            shutdownErrorHook,
            streamsUncaughtExceptionHandler,
            cache::resize
        );

        return streamThread.updateThreadMetadata(getSharedAdminClientId(clientId));
    }

    private static DefaultTaskManager maybeCreateSchedulingTaskManager(final boolean processingThreadsEnabled,
                                                                       final boolean stateUpdaterEnabled,
                                                                       final TopologyMetadata topologyMetadata,
                                                                       final Time time,
                                                                       final String threadId,
                                                                       final Tasks tasks) {
        if (processingThreadsEnabled) {
            if (!stateUpdaterEnabled) {
                throw new IllegalStateException("Processing threads require the state updater to be enabled");
            }

            final DefaultTaskManager defaultTaskManager = new DefaultTaskManager(
                time,
                threadId,
                tasks,
                new DefaultTaskExecutorCreator(),
                topologyMetadata.taskExecutionMetadata(),
                1
            );
            defaultTaskManager.startTaskExecutors();
            return defaultTaskManager;
        }
        return null;
    }

    private static StateUpdater maybeCreateAndStartStateUpdater(final boolean stateUpdaterEnabled,
                                                                final StreamsMetricsImpl streamsMetrics,
                                                                final StreamsConfig streamsConfig,
                                                                final Consumer<byte[], byte[]> restoreConsumer,
                                                                final ChangelogReader changelogReader,
                                                                final TopologyMetadata topologyMetadata,
                                                                final Time time,
                                                                final String clientId,
                                                                final int threadIdx) {
        if (stateUpdaterEnabled) {
            final String name = clientId + "-StateUpdater-" + threadIdx;
            final StateUpdater stateUpdater = new DefaultStateUpdater(
                name,
                streamsMetrics.metricsRegistry(),
                streamsConfig,
                restoreConsumer,
                changelogReader,
                topologyMetadata,
                time
            );
            stateUpdater.start();
            return stateUpdater;
        } else {
            return null;
        }
    }

    @SuppressWarnings("this-escape")
    public StreamThread(final Time time,
                        final StreamsConfig config,
                        final Admin adminClient,
                        final Consumer<byte[], byte[]> mainConsumer,
                        final Consumer<byte[], byte[]> restoreConsumer,
                        final ChangelogReader changelogReader,
                        final String originalReset,
                        final TaskManager taskManager,
                        final StateUpdater stateUpdater,
                        final StreamsMetricsImpl streamsMetrics,
                        final TopologyMetadata topologyMetadata,
                        final String threadId,
                        final LogContext logContext,
                        final AtomicInteger assignmentErrorCode,
                        final AtomicLong nextProbingRebalanceMs,
                        final Queue<StreamsException> nonFatalExceptionsToHandle,
                        final Runnable shutdownErrorHook,
                        final BiConsumer<Throwable, Boolean> streamsUncaughtExceptionHandler,
                        final java.util.function.Consumer<Long> cacheResizer
                        ) {
        super(threadId);
        this.stateLock = new Object();
        this.adminClient = adminClient;
        this.streamsMetrics = streamsMetrics;
        this.commitSensor = ThreadMetrics.commitSensor(threadId, streamsMetrics);
        this.pollSensor = ThreadMetrics.pollSensor(threadId, streamsMetrics);
        this.pollRecordsSensor = ThreadMetrics.pollRecordsSensor(threadId, streamsMetrics);
        this.pollRatioSensor = ThreadMetrics.pollRatioSensor(threadId, streamsMetrics);
        this.processLatencySensor = ThreadMetrics.processLatencySensor(threadId, streamsMetrics);
        this.processRecordsSensor = ThreadMetrics.processRecordsSensor(threadId, streamsMetrics);
        this.processRateSensor = ThreadMetrics.processRateSensor(threadId, streamsMetrics);
        this.processRatioSensor = ThreadMetrics.processRatioSensor(threadId, streamsMetrics);
        this.punctuateSensor = ThreadMetrics.punctuateSensor(threadId, streamsMetrics);
        this.punctuateRatioSensor = ThreadMetrics.punctuateRatioSensor(threadId, streamsMetrics);
        this.commitRatioSensor = ThreadMetrics.commitRatioSensor(threadId, streamsMetrics);
        this.failedStreamThreadSensor = ClientMetrics.failedStreamThreadSensor(streamsMetrics);
        this.assignmentErrorCode = assignmentErrorCode;
        this.shutdownErrorHook = shutdownErrorHook;
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
        this.cacheResizer = cacheResizer;

        // The following sensors are created here but their references are not stored in this object, since within
        // this object they are not recorded. The sensors are created here so that the stream threads starts with all
        // its metrics initialised. Otherwise, those sensors would have been created during processing, which could
        // lead to missing metrics. If no task were created, the metrics for created and closed
        // tasks would never be added to the metrics.
        ThreadMetrics.createTaskSensor(threadId, streamsMetrics);
        ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);

        ThreadMetrics.addThreadStartTimeMetric(
            threadId,
            streamsMetrics,
            time.milliseconds()
        );
        ThreadMetrics.addThreadBlockedTimeMetric(
            threadId,
            new StreamThreadTotalBlockedTime(
                mainConsumer,
                restoreConsumer,
                taskManager::totalProducerBlockedTime
            ),
            streamsMetrics
        );

        this.time = time;
        this.topologyMetadata = topologyMetadata;
        this.topologyMetadata.registerThread(getName());
        this.logPrefix = logContext.logPrefix();
        this.log = logContext.logger(StreamThread.class);
        this.rebalanceListener = new StreamsRebalanceListener(time, taskManager, this, this.log, this.assignmentErrorCode);
        this.taskManager = taskManager;
        this.stateUpdater = stateUpdater;
        this.restoreConsumer = restoreConsumer;
        this.mainConsumer = mainConsumer;
        this.changelogReader = changelogReader;
        this.originalReset = originalReset;
        this.nextProbingRebalanceMs = nextProbingRebalanceMs;
        this.nonFatalExceptionsToHandle = nonFatalExceptionsToHandle;
        this.getGroupInstanceID = mainConsumer.groupMetadata().groupInstanceId();

        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        final int dummyThreadIdx = 1;
        this.maxPollTimeMs = new InternalConsumerConfig(config.getMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
            .getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.purgeTimeMs = config.getLong(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG);

        this.numIterations = 1;
        this.eosEnabled = eosEnabled(config);
        this.processingMode = processingMode(config);
        this.stateUpdaterEnabled = InternalConfig.getStateUpdaterEnabled(config.originals());
        this.processingThreadsEnabled = InternalConfig.getProcessingThreadsEnabled(config.originals());
    }

    private static final class InternalConsumerConfig extends ConsumerConfig {
        private InternalConsumerConfig(final Map<String, Object> props) {
            super(ConsumerConfig.appendDeserializerToConfig(props, new ByteArrayDeserializer(),
                    new ByteArrayDeserializer()), false);
        }
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
            cleanRun = runLoop();
        } catch (final Throwable e) {
            failedStreamThreadSensor.record();
            requestLeaveGroupDuringShutdown();
            streamsUncaughtExceptionHandler.accept(e, false);
            // Note: the above call currently rethrows the exception, so nothing below this line will be executed
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
    @SuppressWarnings("deprecation") // Needed to include StreamsConfig.EXACTLY_ONCE_BETA in error log for UnsupportedVersionException
    boolean runLoop() {
        subscribeConsumer();

        // if the thread is still in the middle of a rebalance, we should keep polling
        // until the rebalance is completed before we close and commit the tasks
        while (isRunning() || taskManager.rebalanceInProgress()) {
            try {
                checkForTopologyUpdates();
                // If we received the shutdown signal while waiting for a topology to be added, we can
                // stop polling regardless of the rebalance status since we know there are no tasks left
                if (!isRunning() && topologyMetadata.isEmpty()) {
                    log.info("Shutting down thread with empty topology.");
                    break;
                }

                maybeSendShutdown();
                final long size = cacheResizeSize.getAndSet(-1L);
                if (size != -1L) {
                    cacheResizer.accept(size);
                }
                if (processingThreadsEnabled) {
                    runOnceWithProcessingThreads();
                } else {
                    runOnceWithoutProcessingThreads();
                }

                maybeGetClientInstanceIds();

                // Check for a scheduled rebalance but don't trigger it until the current rebalance is done
                if (!taskManager.rebalanceInProgress() && nextProbingRebalanceMs.get() < time.milliseconds()) {
                    log.info("Triggering the followup rebalance scheduled for {}.", Utils.toLogDateTimeFormat(nextProbingRebalanceMs.get()));
                    mainConsumer.enforceRebalance("triggered followup rebalance scheduled for " + nextProbingRebalanceMs.get());
                    nextProbingRebalanceMs.set(Long.MAX_VALUE);
                }
            } catch (final TaskCorruptedException e) {
                log.warn("Detected the states of tasks " + e.corruptedTasks() + " are corrupted. " +
                         "Will close the task as dirty and re-create and bootstrap from scratch.", e);
                try {
                    // check if any active task got corrupted. We will trigger a rebalance in that case.
                    // once the task corruptions have been handled
                    final boolean enforceRebalance = taskManager.handleCorruption(e.corruptedTasks());
                    if (enforceRebalance && eosEnabled) {
                        log.info("Active task(s) got corrupted. Triggering a rebalance.");
                        mainConsumer.enforceRebalance("Active tasks corrupted");
                    }
                } catch (final TaskMigratedException taskMigrated) {
                    handleTaskMigrated(taskMigrated);
                }
            } catch (final TaskMigratedException e) {
                handleTaskMigrated(e);
            } catch (final UnsupportedVersionException e) {
                final String errorMessage = e.getMessage();
                if (errorMessage != null &&
                    errorMessage.startsWith("Broker unexpectedly doesn't support requireStable flag on version ")) {

                    log.error("Shutting down because the Kafka cluster seems to be on a too old version. " +
                              "Setting {}=\"{}\"/\"{}\" requires broker version 2.5 or higher.",
                          StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                          StreamsConfig.EXACTLY_ONCE_V2, StreamsConfig.EXACTLY_ONCE_BETA);
                }
                failedStreamThreadSensor.record();
                this.streamsUncaughtExceptionHandler.accept(new StreamsException(e), false);
                return false;
            } catch (final StreamsException e) {
                throw e;
            } catch (final Exception e) {
                throw new StreamsException(e);
            }
        }
        return true;
    }

    // visible for testing
    void maybeGetClientInstanceIds() {
        // we pass in a timeout of zero into each `clientInstanceId()` call
        // to just trigger the "get instance id" background RPC;
        // we don't want to block the stream thread that can do useful work in the meantime

        if (fetchDeadlineClientInstanceId != -1) {
            if (!mainConsumerInstanceIdFuture.isDone()) {
                if (fetchDeadlineClientInstanceId >= time.milliseconds()) {
                    try {
                        mainConsumerInstanceIdFuture.complete(mainConsumer.clientInstanceId(Duration.ZERO));
                    } catch (final IllegalStateException disabledError) {
                        // if telemetry is disabled on a client, we swallow the error,
                        // to allow returning a partial result for all other clients
                        mainConsumerInstanceIdFuture.complete(null);
                    } catch (final TimeoutException swallow) {
                        // swallow
                    } catch (final Exception error) {
                        mainConsumerInstanceIdFuture.completeExceptionally(error);
                    }
                } else {
                    mainConsumerInstanceIdFuture.completeExceptionally(
                        new TimeoutException("Could not retrieve main consumer client instance id.")
                    );
                }
            }


            if (!stateUpdaterEnabled && !restoreConsumerInstanceIdFuture.isDone()) {
                if (fetchDeadlineClientInstanceId >= time.milliseconds()) {
                    try {
                        restoreConsumerInstanceIdFuture.complete(restoreConsumer.clientInstanceId(Duration.ZERO));
                    } catch (final IllegalStateException disabledError) {
                        // if telemetry is disabled on a client, we swallow the error,
                        // to allow returning a partial result for all other clients
                        restoreConsumerInstanceIdFuture.complete(null);
                    } catch (final TimeoutException swallow) {
                        // swallow
                    } catch (final Exception error) {
                        restoreConsumerInstanceIdFuture.completeExceptionally(error);
                    }
                } else {
                    restoreConsumerInstanceIdFuture.completeExceptionally(
                        new TimeoutException("Could not retrieve restore consumer client instance id.")
                    );
                }
            }

            if (!processingMode.equals(StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA) &&
                    !threadProducerInstanceIdFuture.isDone()) {

                if (fetchDeadlineClientInstanceId >= time.milliseconds()) {
                    try {
                        threadProducerInstanceIdFuture.complete(
                            taskManager.threadProducer().kafkaProducer().clientInstanceId(Duration.ZERO)
                        );
                    } catch (final IllegalStateException disabledError) {
                        // if telemetry is disabled on a client, we swallow the error,
                        // to allow returning a partial result for all other clients
                        threadProducerInstanceIdFuture.complete(null);
                    } catch (final TimeoutException swallow) {
                        // swallow
                    } catch (final Exception error) {
                        threadProducerInstanceIdFuture.completeExceptionally(error);
                    }
                } else {
                    threadProducerInstanceIdFuture.completeExceptionally(
                        new TimeoutException("Could not retrieve thread producer client instance id.")
                    );
                }
            }

            maybeResetFetchDeadline();
        }
    }

    private void maybeResetFetchDeadline() {
        boolean reset = mainConsumerInstanceIdFuture.isDone()
            && (!stateUpdaterEnabled && restoreConsumerInstanceIdFuture.isDone());

        if (processingMode.equals(StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA)) {
            throw new UnsupportedOperationException("not implemented yet");
        } else if (!threadProducerInstanceIdFuture.isDone()) {
            reset = false;
        }

        if (reset) {
            fetchDeadlineClientInstanceId = -1L;
        }
    }

    /**
     * Sets the streams uncaught exception handler.
     *
     * @param streamsUncaughtExceptionHandler the user handler wrapped in shell to execute the action
     */
    public void setStreamsUncaughtExceptionHandler(final BiConsumer<Throwable, Boolean> streamsUncaughtExceptionHandler) {
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
    }

    public void maybeSendShutdown() {
        if (assignmentErrorCode.get() == AssignorError.SHUTDOWN_REQUESTED.code()) {
            log.warn("Detected that shutdown was requested. " +
                    "All clients in this app will now begin to shutdown");
            mainConsumer.enforceRebalance("Shutdown requested");
        }
    }

    public boolean waitOnThreadState(final StreamThread.State targetState, final long timeoutMs) {
        final long begin = time.milliseconds();
        synchronized (stateLock) {
            boolean interrupted = false;
            long elapsedMs = 0L;
            try {
                while (state != targetState) {
                    if (timeoutMs >= elapsedMs) {
                        final long remainingMs = timeoutMs - elapsedMs;
                        try {
                            stateLock.wait(remainingMs);
                        } catch (final InterruptedException e) {
                            interrupted = true;
                        }
                    } else {
                        log.debug("Cannot transit to {} within {}ms", targetState, timeoutMs);
                        return false;
                    }
                    elapsedMs = time.milliseconds() - begin;
                }
                return true;
            } finally {
                // Make sure to restore the interruption status before returning.
                // We do not always own the current thread that executes this method, i.e., we do not know the
                // interruption policy of the thread. The least we can do is restore the interruption status before
                // the current thread exits this method.
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void shutdownToError() {
        shutdownErrorHook.run();
    }

    public void sendShutdownRequest(final AssignorError assignorError) {
        assignmentErrorCode.set(assignorError.code());
    }

    private void handleTaskMigrated(final TaskMigratedException e) {
        log.warn("Detected that the thread is being fenced. " +
                     "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                     "Will close out all assigned tasks and rejoin the consumer group.", e);

        taskManager.handleLostAll();
        mainConsumer.unsubscribe();
        subscribeConsumer();
    }

    private void subscribeConsumer() {
        if (topologyMetadata.usesPatternSubscription()) {
            mainConsumer.subscribe(topologyMetadata.sourceTopicPattern(), rebalanceListener);
        } else {
            mainConsumer.subscribe(topologyMetadata.allFullSourceTopicNames(), rebalanceListener);
        }
    }

    public void resizeCache(final long size) {
        cacheResizeSize.set(size);
    }

    /**
     * One iteration of a thread includes the following steps:
     *
     * 1. poll records from main consumer and add to buffer;
     * 2. restore from restore consumer and update standby tasks if necessary;
     * 3. process active tasks from the buffers;
     * 4. punctuate active tasks if necessary;
     * 5. commit all tasks if necessary;
     *
     * Among them, step 3/4/5 is done in batches in which we try to process as much as possible while trying to
     * stop iteration to call the next iteration when it's close to the next main consumer's poll deadline
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      If the store's change log does not contain the partition
     * @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
     *                               or if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // Visible for testing
    void runOnceWithoutProcessingThreads() {
        final long startMs = time.milliseconds();
        now = startMs;

        final long pollLatency;
        taskManager.resumePollingForPartitionsWithAvailableSpace();
        pollLatency = pollPhase();

        // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
        // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
        // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
        // could affect the task manager state beyond this point within #runOnce().
        if (!isRunning()) {
            log.debug("Thread state is already {}, skipping the run once call after poll request", state);
            return;
        }

        if (!stateUpdaterEnabled) {
            initializeAndRestorePhase();
        }

        // TODO: we should record the restore latency and its relative time spent ratio after
        //       we figure out how to move this method out of the stream thread
        advanceNowAndComputeLatency();

        int totalProcessed = 0;
        long totalCommitLatency = 0L;
        long totalProcessLatency = 0L;
        long totalPunctuateLatency = 0L;
        if (state == State.RUNNING
            || (stateUpdaterEnabled && isStartingRunningOrPartitionAssigned())) {

            taskManager.updateLags();

            /*
             * Within an iteration, after processing up to N (N initialized as 1 upon start up) records for each applicable tasks, check the current time:
             *  1. If it is time to punctuate, do it;
             *  2. If it is time to commit, do it, this should be after 1) since punctuate may trigger commit;
             *  3. If there's no records processed, end the current iteration immediately;
             *  4. If we are close to consumer's next poll deadline, end the current iteration immediately;
             *  5. If any of 1), 2) and 4) happens, half N for next iteration;
             *  6. Otherwise, increment N.
             */
            do {

                if (stateUpdaterEnabled) {
                    checkStateUpdater();
                }

                log.debug("Processing tasks with {} iterations.", numIterations);
                final int processed = taskManager.process(numIterations, time);
                final long processLatency = advanceNowAndComputeLatency();
                totalProcessLatency += processLatency;
                if (processed > 0) {
                    // It makes no difference to the outcome of these metrics when we record "0",
                    // so we can just avoid the method call when we didn't process anything.
                    processRateSensor.record(processed, now);

                    // This metric is scaled to represent the _average_ processing time of _each_
                    // task. Note, it's hard to interpret this as defined; the per-task process-ratio
                    // as well as total time ratio spent on processing compared with polling / committing etc
                    // are reported on other metrics.
                    processLatencySensor.record(processLatency / (double) processed, now);

                    totalProcessed += processed;
                    totalRecordsProcessedSinceLastSummary += processed;
                }

                log.debug("Processed {} records with {} iterations; invoking punctuators if necessary",
                          processed,
                          numIterations);

                final int punctuated = taskManager.punctuate();
                totalPunctuatorsSinceLastSummary += punctuated;
                final long punctuateLatency = advanceNowAndComputeLatency();
                totalPunctuateLatency += punctuateLatency;
                if (punctuated > 0) {
                    punctuateSensor.record(punctuateLatency / (double) punctuated, now);
                }

                log.debug("{} punctuators ran.", punctuated);

                final long beforeCommitMs = now;
                final int committed = maybeCommit();
                final long commitLatency = Math.max(now - beforeCommitMs, 0);
                totalCommitLatency += commitLatency;
                if (committed > 0) {
                    totalCommittedSinceLastSummary += committed;
                    commitSensor.record(commitLatency / (double) committed, now);

                    if (log.isDebugEnabled()) {
                        log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                            taskManager.activeRunningTaskIds(), taskManager.standbyTaskIds(), commitLatency);
                    }
                }

                if (processed == 0) {
                    // if there are no records to be processed, exit after punctuate / commit
                    break;
                } else if (Math.max(now - lastPollMs, 0) > maxPollTimeMs / 2) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                    break;
                } else if (punctuated > 0 || committed > 0) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                } else {
                    numIterations++;
                }
            } while (true);

            // we record the ratio out of the while loop so that the accumulated latency spans over
            // multiple iterations with reasonably large max.num.records and hence is less vulnerable to outliers
            taskManager.recordTaskProcessRatio(totalProcessLatency, now);
        }

        now = time.milliseconds();
        final long runOnceLatency = now - startMs;
        processRecordsSensor.record(totalProcessed, now);
        processRatioSensor.record((double) totalProcessLatency / runOnceLatency, now);
        punctuateRatioSensor.record((double) totalPunctuateLatency / runOnceLatency, now);
        pollRatioSensor.record((double) pollLatency / runOnceLatency, now);
        commitRatioSensor.record((double) totalCommitLatency / runOnceLatency, now);

        final boolean logProcessingSummary = now - lastLogSummaryMs > LOG_SUMMARY_INTERVAL_MS;
        if (logProcessingSummary) {
            log.info("Processed {} total records, ran {} punctuators, and committed {} total tasks since the last update",
                 totalRecordsProcessedSinceLastSummary, totalPunctuatorsSinceLastSummary, totalCommittedSinceLastSummary);

            totalRecordsProcessedSinceLastSummary = 0L;
            totalPunctuatorsSinceLastSummary = 0L;
            totalCommittedSinceLastSummary = 0L;
            lastLogSummaryMs = now;
        }
    }

    /**
     * One iteration of a thread includes the following steps:
     *
     * 1. poll records from main consumer and add to buffer;
     * 2. check the task manager for any exceptions to be handled
     * 3. commit all tasks if necessary;
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException      If the store's change log does not contain the partition
     * @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
     *                               or if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // Visible for testing
    void runOnceWithProcessingThreads() {
        final long startMs = time.milliseconds();
        now = startMs;

        final long pollLatency;
        taskManager.resumePollingForPartitionsWithAvailableSpace();
        pollLatency = pollPhase();

        // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
        // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
        // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
        // could affect the task manager state beyond this point within #runOnce().
        if (!isRunning()) {
            log.debug("Thread state is already {}, skipping the run once call after poll request", state);
            return;
        }

        long totalCommitLatency = 0L;
        if (isRunning()) {

            taskManager.updateLags();

            checkStateUpdater();

            taskManager.maybeThrowTaskExceptionsFromProcessingThreads();
            taskManager.signalTaskExecutors();

            final long beforeCommitMs = now;
            final int committed = maybeCommit();
            final long commitLatency = Math.max(now - beforeCommitMs, 0);
            totalCommitLatency += commitLatency;
            if (committed > 0) {
                totalCommittedSinceLastSummary += committed;
                commitSensor.record(commitLatency / (double) committed, now);

                if (log.isDebugEnabled()) {
                    log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), commitLatency);
                }
            }
        }

        now = time.milliseconds();
        final long runOnceLatency = now - startMs;
        pollRatioSensor.record((double) pollLatency / runOnceLatency, now);
        commitRatioSensor.record((double) totalCommitLatency / runOnceLatency, now);

        final boolean logProcessingSummary = now - lastLogSummaryMs > LOG_SUMMARY_INTERVAL_MS;
        if (logProcessingSummary) {
            log.info("Committed {} total tasks since the last update", totalCommittedSinceLastSummary);

            totalCommittedSinceLastSummary = 0L;
            lastLogSummaryMs = now;
        }
    }

    private void initializeAndRestorePhase() {
        final java.util.function.Consumer<Set<TopicPartition>> offsetResetter = partitions -> resetOffsets(partitions, null);
        final State stateSnapshot = state;
        // only try to initialize the assigned tasks
        // if the state is still in PARTITION_ASSIGNED after the poll call
        if (stateSnapshot == State.PARTITIONS_ASSIGNED
            || stateSnapshot == State.RUNNING && taskManager.needsInitializationOrRestoration()) {

            log.debug("State is {}; initializing tasks if necessary", stateSnapshot);

            if (taskManager.tryToCompleteRestoration(now, offsetResetter)) {
                log.info("Restoration took {} ms for all active tasks {}", time.milliseconds() - lastPartitionAssignedMs,
                    taskManager.activeTaskIds());
                setState(State.RUNNING);
            }

            if (log.isDebugEnabled()) {
                log.debug("Initialization call done. State is {}", state);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Idempotently invoking restoration logic in state {}", state);
        }
        // we can always let changelog reader try restoring in order to initialize the changelogs;
        // if there's no active restoring or standby updating it would not try to fetch any data
        // After KAFKA-13873, we only restore the not paused tasks.
        changelogReader.restore(taskManager.notPausedTasks());
        log.debug("Idempotent restore call done. Thread state has not changed.");
    }

    private void checkStateUpdater() {
        final java.util.function.Consumer<Set<TopicPartition>> offsetResetter = partitions -> resetOffsets(partitions, null);
        final State stateSnapshot = state;
        final boolean allRunning = taskManager.checkStateUpdater(now, offsetResetter);
        if (allRunning && stateSnapshot == State.PARTITIONS_ASSIGNED) {
            setState(State.RUNNING);
        }
    }

    // Check if the topology has been updated since we last checked, ie via #addNamedTopology or #removeNamedTopology
    private void checkForTopologyUpdates() {
        if (topologyMetadata.isEmpty() || topologyMetadata.needsUpdate(getName())) {
            log.info("StreamThread has detected an update to the topology");

            taskManager.handleTopologyUpdates();

            topologyMetadata.maybeWaitForNonEmptyTopology(() -> state);

            // We don't need to manually trigger a rebalance to pick up tasks from the new topology, as
            // a rebalance will always occur when the metadata is updated after a change in subscription
            log.info("Updating consumer subscription following topology update");
            subscribeConsumer();
        }
    }

    private long pollPhase() {
        final ConsumerRecords<byte[], byte[]> records;
        log.debug("Invoking poll on main Consumer");

        if (state == State.PARTITIONS_ASSIGNED && !stateUpdaterEnabled) {
            // try to fetch some records with zero poll millis
            // to unblock the restoration as soon as possible
            records = pollRequests(Duration.ZERO);
        } else if (state == State.PARTITIONS_REVOKED) {
            // try to fetch some records with zero poll millis to unblock
            // other useful work while waiting for the join response
            records = pollRequests(Duration.ZERO);
        } else if (state == State.RUNNING || state == State.STARTING || (state == State.PARTITIONS_ASSIGNED && stateUpdaterEnabled)) {
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

        final int numRecords = records.count();

        for (final TopicPartition topicPartition: records.partitions()) {
            records
                .records(topicPartition)
                .stream()
                .max(Comparator.comparing(ConsumerRecord::offset))
                .ifPresent(t -> taskManager.updateTaskEndMetadata(topicPartition, t.offset()));
        }

        log.debug("Main Consumer poll completed in {} ms and fetched {} records from partitions {}",
            pollLatency, numRecords, records.partitions());

        pollSensor.record(pollLatency, now);

        if (!records.isEmpty()) {
            pollRecordsSensor.record(numRecords, now);
            taskManager.addRecordsToTasks(records);
        }

        while (!nonFatalExceptionsToHandle.isEmpty()) {
            streamsUncaughtExceptionHandler.accept(nonFatalExceptionsToHandle.poll(), true);
        }
        return pollLatency;
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private ConsumerRecords<byte[], byte[]> pollRequests(final Duration pollTime) {
        ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();

        lastPollMs = now;

        try {
            records = mainConsumer.poll(pollTime);
        } catch (final InvalidOffsetException e) {
            resetOffsets(e.partitions(), e);
        }

        return records;
    }

    private void resetOffsets(final Set<TopicPartition> partitions, final Exception cause) {
        final Set<String> loggedTopics = new HashSet<>();
        final Set<TopicPartition> seekToBeginning = new HashSet<>();
        final Set<TopicPartition> seekToEnd = new HashSet<>();
        final Set<TopicPartition> notReset = new HashSet<>();

        for (final TopicPartition partition : partitions) {
            final OffsetResetStrategy offsetResetStrategy = topologyMetadata.offsetResetStrategy(partition.topic());

            // This may be null if the task we are currently processing was apart of a named topology that was just removed.
            // TODO KAFKA-13713: keep the StreamThreads and TopologyMetadata view of named topologies in sync until final thread has acked
            if (offsetResetStrategy != null) {
                switch (offsetResetStrategy) {
                    case EARLIEST:
                        addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
                        break;
                    case LATEST:
                        addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
                        break;
                    case NONE:
                        if ("earliest".equals(originalReset)) {
                            addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                        } else if ("latest".equals(originalReset)) {
                            addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                        } else {
                            notReset.add(partition);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unable to locate topic " + partition.topic() + " in the topology");
                }
            }
        }

        if (notReset.isEmpty()) {
            if (!seekToBeginning.isEmpty()) {
                mainConsumer.seekToBeginning(seekToBeginning);
            }

            if (!seekToEnd.isEmpty()) {
                mainConsumer.seekToEnd(seekToEnd);
            }
        } else {
            final String notResetString =
                notReset.stream()
                        .map(TopicPartition::topic)
                        .distinct()
                        .collect(Collectors.joining(","));

            final String format = String.format(
                "No valid committed offset found for input [%s] and no valid reset policy configured." +
                    " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                    "policy via StreamsBuilder#stream(..., Consumed.with(Topology.AutoOffsetReset)) or " +
                    "StreamsBuilder#table(..., Consumed.with(Topology.AutoOffsetReset))",
                notResetString
            );

            if (cause == null) {
                throw new StreamsException(format);
            } else {
                throw new StreamsException(format, cause);
            }
        }
    }

    private void addToResetList(final TopicPartition partition, final Set<TopicPartition> partitions, final String logMessage, final String resetPolicy, final Set<String> loggedTopics) {
        final String topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    // This method is added for usage in tests where mocking the underlying native call is not possible.
    public boolean isThreadAlive() {
        return isAlive();
    }

    // Call method when a topology is resumed
    public void signalResume() {
        taskManager.signalResume();
    }

    /**
     * Try to commit all active tasks owned by this thread.
     *
     * Visible for testing.
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommit() {
        final int committed;
        if (now - lastCommitMs > commitTimeMs) {
            if (log.isDebugEnabled()) {
                log.debug("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                          taskManager.activeRunningTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            committed = taskManager.commit(
                taskManager.allOwnedTasks()
                    .values()
                    .stream()
                    .filter(t -> t.state() == Task.State.RUNNING || t.state() == Task.State.RESTORING)
                    .collect(Collectors.toSet())
            );

            if ((now - lastPurgeMs) > purgeTimeMs) {
                // try to purge the committed records for repartition topics if possible
                taskManager.maybePurgeCommittedRecords();
                lastPurgeMs = now;
            }

            if (committed == -1) {
                log.debug("Unable to commit as we are in the middle of a rebalance, will try again when it completes.");
            } else {
                now = time.milliseconds();
                lastCommitMs = now;
            }
        } else {
            committed = taskManager.maybeCommitActiveTasksPerUserRequested();
        }

        return committed;
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

        log.info("Shutting down {}", cleanRun ? "clean" : "unclean");

        mainConsumerInstanceIdFuture.complete(null);

        try {
            taskManager.shutdown(cleanRun);
        } catch (final Throwable e) {
            log.error("Failed to close task manager due to the following error:", e);
        }
        try {
            topologyMetadata.unregisterThread(threadMetadata.threadName());
        } catch (final Throwable e) {
            log.error("Failed to unregister thread due to the following error:", e);
        }
        try {
            changelogReader.clear();
        } catch (final Throwable e) {
            log.error("Failed to close changelog reader due to the following error:", e);
        }
        try {
            if (leaveGroupRequested.get()) {
                mainConsumer.unsubscribe();
            }
        } catch (final Throwable e) {
            log.error("Failed to unsubscribe due to the following error: ", e);
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
        streamsMetrics.removeAllThreadLevelMetrics(getName());

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

        threadMetadata = new ThreadMetadataImpl(
            getName(),
            state().name(),
            getConsumerClientId(getName()),
            getRestoreConsumerClientId(getName()),
            taskManager.producerClientIds(),
            adminClientId,
            Collections.emptySet(),
            Collections.emptySet());

        return this;
    }

    private void updateThreadMetadata(final Map<TaskId, Task> activeTasks,
                                      final Map<TaskId, Task> standbyTasks) {
        final Set<TaskMetadata> activeTasksMetadata = new HashSet<>();
        for (final Map.Entry<TaskId, Task> task : activeTasks.entrySet()) {
            activeTasksMetadata.add(new TaskMetadataImpl(
                task.getValue().id(),
                task.getValue().inputPartitions(),
                task.getValue().committedOffsets(),
                task.getValue().highWaterMark(),
                task.getValue().timeCurrentIdlingStarted()
            ));
        }
        final Set<TaskMetadata> standbyTasksMetadata = new HashSet<>();
        for (final Map.Entry<TaskId, Task> task : standbyTasks.entrySet()) {
            standbyTasksMetadata.add(new TaskMetadataImpl(
                task.getValue().id(),
                task.getValue().inputPartitions(),
                task.getValue().committedOffsets(),
                task.getValue().highWaterMark(),
                task.getValue().timeCurrentIdlingStarted()
            ));
        }

        final String adminClientId = threadMetadata.adminClientId();
        threadMetadata = new ThreadMetadataImpl(
            getName(),
            state().name(),
            getConsumerClientId(getName()),
            getRestoreConsumerClientId(getName()),
            taskManager.producerClientIds(),
            adminClientId,
            activeTasksMetadata,
            standbyTasksMetadata
        );
    }

    /**
     * Getting the list of current active tasks of the thread;
     * Note that the returned list may be used by other thread than the StreamThread itself,
     * and hence need to be read-only
     */
    public Set<Task> readOnlyActiveTasks() {
        return readyOnlyAllTasks().stream()
            .filter(Task::isActive).collect(Collectors.toSet());
    }

    /**
     * Getting the list of all owned tasks of the thread, including both active and standby;
     * Note that the returned list may be used by other thread than the StreamThread itself,
     * and hence need to be read-only
     */
    public Set<Task> readyOnlyAllTasks() {
        return taskManager.readOnlyAllTasks();
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

    public Optional<String> getGroupInstanceID() {
        return getGroupInstanceID;
    }

    public void requestLeaveGroupDuringShutdown() {
        leaveGroupRequested.set(true);
    }

    public Map<MetricName, Metric> producerMetrics() {
        return taskManager.producerMetrics();
    }

    public Map<MetricName, Metric> consumerMetrics() {
        return ClientUtils.consumerMetrics(mainConsumer, restoreConsumer);
    }

    public Map<MetricName, Metric> adminClientMetrics() {
        return ClientUtils.adminClientMetrics(adminClient);
    }

    public Object getStateLock() {
        return stateLock;
    }

    // this method is NOT thread-safe (we rely on the callee to be `synchronized`)
    public Map<String, KafkaFuture<Uuid>> consumerClientInstanceIds(final Duration timeout) {
        boolean setDeadline = false;

        final Map<String, KafkaFuture<Uuid>> result = new HashMap<>();

        if (mainConsumerInstanceIdFuture.isDone()) {
            if (mainConsumerInstanceIdFuture.isCompletedExceptionally()) {
                mainConsumerInstanceIdFuture = new KafkaFutureImpl<>();
                setDeadline = true;
            }
        } else {
            setDeadline = true;
        }
        result.put(getName() + "-consumer", mainConsumerInstanceIdFuture);

        if (stateUpdaterEnabled) {
            restoreConsumerInstanceIdFuture = stateUpdater.restoreConsumerInstanceId(timeout);
        } else {
            if (restoreConsumerInstanceIdFuture.isDone()) {
                if (restoreConsumerInstanceIdFuture.isCompletedExceptionally()) {
                    restoreConsumerInstanceIdFuture = new KafkaFutureImpl<>();
                    setDeadline = true;
                }
            } else {
                setDeadline = true;
            }
        }
        result.put(getName() + "-restore-consumer", restoreConsumerInstanceIdFuture);

        if (setDeadline) {
            fetchDeadlineClientInstanceId = time.milliseconds() + timeout.toMillis();
        }

        return result;
    }

    // this method is NOT thread-safe (we rely on the callee to be `synchronized`)
    public KafkaFuture<Map<String, KafkaFuture<Uuid>>> producersClientInstanceIds(final Duration timeout) {
        boolean setDeadline = false;

        if (producerInstanceIdFuture.isDone()) {
            if (producerInstanceIdFuture.isCompletedExceptionally()) {
                producerInstanceIdFuture = new KafkaFutureImpl<>();
                setDeadline = true;
            }
        } else {
            setDeadline = true;
        }

        if (processingMode.equals(StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_ALPHA)) {
            throw new UnsupportedOperationException("not yet implemented");
        } else {
            if (threadProducerInstanceIdFuture.isDone()) {
                if (threadProducerInstanceIdFuture.isCompletedExceptionally()) {
                    threadProducerInstanceIdFuture = new KafkaFutureImpl<>();
                    setDeadline = true;
                }
            } else {
                setDeadline = true;
            }
            producerInstanceIdFuture.complete(Collections.singletonMap(getName() + "-producer", threadProducerInstanceIdFuture));

            if (setDeadline) {
                fetchDeadlineClientInstanceId = time.milliseconds() + timeout.toMillis();
            }
        }

        return producerInstanceIdFuture;
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

    ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    Consumer<byte[], byte[]> mainConsumer() {
        return mainConsumer;
    }

    Consumer<byte[], byte[]> restoreConsumer() {
        return restoreConsumer;
    }

    Admin adminClient() {
        return adminClient;
    }
}
