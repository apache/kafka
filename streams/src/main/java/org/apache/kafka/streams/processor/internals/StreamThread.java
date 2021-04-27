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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.metrics.ClientMetrics;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_BETA;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getConsumerClientId;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getRestoreConsumerClientId;
import static org.apache.kafka.streams.processor.internals.ClientUtils.getSharedAdminClientId;

public class StreamThread extends Thread {

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

    private final Time time;
    private final Logger log;
    private final String logPrefix;
    public final Object stateLock;
    private final Duration pollTime;
    private final long commitTimeMs;
    private final int maxPollTimeMs;
    private final String originalReset;
    private final TaskManager taskManager;
    private final AtomicLong nextProbingRebalanceMs;

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
    private final InternalTopologyBuilder builder;
    private final java.util.function.Consumer<Long> cacheResizer;

    private java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler;
    private Runnable shutdownErrorHook;
    private AtomicInteger assignmentErrorCode;
    private AtomicLong cacheResizeSize;
    private final ProcessingMode processingMode;
    private AtomicBoolean leaveGroupRequested;


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
                                      final int threadIdx,
                                      final Runnable shutdownErrorHook,
                                      final java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler) {
        final String threadId = clientId + "-StreamThread-" + threadIdx;

        final String logPrefix = String.format("stream-thread [%s] ", threadId);
        final LogContext logContext = new LogContext(logPrefix);
        final Logger log = logContext.logger(StreamThread.class);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        referenceContainer.adminClient = adminClient;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;

        log.info("Creating restore consumer client");
        final Map<String, Object> restoreConsumerConfigs = config.getRestoreConsumerConfigs(getRestoreConsumerClientId(threadId));
        final Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);

        final StoreChangelogReader changelogReader = new StoreChangelogReader(
            time,
            config,
            logContext,
            adminClient,
            restoreConsumer,
            userStateRestoreListener
        );

        final ThreadCache cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);

        final ActiveTaskCreator activeTaskCreator = new ActiveTaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            cache,
            time,
            clientSupplier,
            threadId,
            processId,
            log
        );
        final StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            changelogReader,
            threadId,
            log
        );
        final TaskManager taskManager = new TaskManager(
            time,
            changelogReader,
            processId,
            logPrefix,
            streamsMetrics,
            activeTaskCreator,
            standbyTaskCreator,
            builder,
            adminClient,
            stateDirectory,
            StreamThread.processingMode(config)
        );
        referenceContainer.taskManager = taskManager;

        log.info("Creating consumer client");
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        final Map<String, Object> consumerConfigs = config.getMainConsumerConfigs(applicationId, getConsumerClientId(threadId), threadIdx);
        consumerConfigs.put(StreamsConfig.InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);

        final String originalReset = (String) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        // If there are any overrides, we never fall through to the consumer, but only handle offset management ourselves.
        if (!builder.latestResetTopicsPattern().pattern().isEmpty() || !builder.earliestResetTopicsPattern().pattern().isEmpty()) {
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        final Consumer<byte[], byte[]> mainConsumer = clientSupplier.getConsumer(consumerConfigs);
        changelogReader.setMainConsumer(mainConsumer);
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
            streamsMetrics,
            builder,
            threadId,
            logContext,
            referenceContainer.assignmentErrorCode,
            referenceContainer.nextScheduledRebalanceMs,
            shutdownErrorHook,
            streamsUncaughtExceptionHandler,
            cache::resize
        );

        return streamThread.updateThreadMetadata(getSharedAdminClientId(clientId));
    }

    public enum ProcessingMode {
        AT_LEAST_ONCE("AT_LEAST_ONCE"),

        EXACTLY_ONCE_ALPHA("EXACTLY_ONCE_ALPHA"),

        EXACTLY_ONCE_BETA("EXACTLY_ONCE_BETA");

        public final String name;

        ProcessingMode(final String name) {
            this.name = name;
        }
    }

    public static ProcessingMode processingMode(final StreamsConfig config) {
        if (EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            return StreamThread.ProcessingMode.EXACTLY_ONCE_ALPHA;
        } else if (EXACTLY_ONCE_BETA.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            return StreamThread.ProcessingMode.EXACTLY_ONCE_BETA;
        } else {
            return StreamThread.ProcessingMode.AT_LEAST_ONCE;
        }
    }

    public static boolean eosEnabled(final StreamsConfig config) {
        final ProcessingMode processingMode = processingMode(config);
        return processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA ||
            processingMode == ProcessingMode.EXACTLY_ONCE_BETA;
    }

    public StreamThread(final Time time,
                        final StreamsConfig config,
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
                        final AtomicInteger assignmentErrorCode,
                        final AtomicLong nextProbingRebalanceMs,
                        final Runnable shutdownErrorHook,
                        final java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler,
                        final java.util.function.Consumer<Long> cacheResizer) {
        super(threadId);
        this.stateLock = new Object();
        this.leaveGroupRequested = new AtomicBoolean(false);
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
        this.cacheResizeSize = new AtomicLong(-1L);
        this.shutdownErrorHook = shutdownErrorHook;
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
        this.cacheResizer = cacheResizer;
        this.processingMode = processingMode(config);

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
        this.rebalanceListener = new StreamsRebalanceListener(time, taskManager, this, this.log, this.assignmentErrorCode);
        this.taskManager = taskManager;
        this.restoreConsumer = restoreConsumer;
        this.mainConsumer = mainConsumer;
        this.changelogReader = changelogReader;
        this.originalReset = originalReset;
        this.nextProbingRebalanceMs = nextProbingRebalanceMs;
        this.getGroupInstanceID = mainConsumer.groupMetadata().groupInstanceId();

        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        final int dummyThreadIdx = 1;
        this.maxPollTimeMs = new InternalConsumerConfig(config.getMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
            .getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        this.numIterations = 1;
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
            this.streamsUncaughtExceptionHandler.accept(e);
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
    boolean runLoop() {
        subscribeConsumer();

        // if the thread is still in the middle of a rebalance, we should keep polling
        // until the rebalance is completed before we close and commit the tasks
        while (isRunning() || taskManager.isRebalanceInProgress()) {
            try {
                maybeSendShutdown();
                final Long size = cacheResizeSize.getAndSet(-1L);
                if (size != -1L) {
                    cacheResizer.accept(size);
                }
                runOnce();
                if (nextProbingRebalanceMs.get() < time.milliseconds()) {
                    log.info("Triggering the followup rebalance scheduled for {} ms.", nextProbingRebalanceMs.get());
                    mainConsumer.enforceRebalance();
                    nextProbingRebalanceMs.set(Long.MAX_VALUE);
                }
            } catch (final TaskCorruptedException e) {
                log.warn("Detected the states of tasks " + e.corruptedTasks() + " are corrupted. " +
                         "Will close the task as dirty and re-create and bootstrap from scratch.", e);
                try {
                    taskManager.handleCorruption(e.corruptedTasks());
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
                              "Setting {}=\"{}\" requires broker version 2.5 or higher.",
                          StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                          EXACTLY_ONCE_BETA);
                }
                failedStreamThreadSensor.record();
                this.streamsUncaughtExceptionHandler.accept(e);
                return false;
            }
        }
        return true;
    }

    /**
     * Sets the streams uncaught exception handler.
     *
     * @param streamsUncaughtExceptionHandler the user handler wrapped in shell to execute the action
     */
    public void setStreamsUncaughtExceptionHandler(final java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler) {
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
    }

    public void maybeSendShutdown() {
        if (assignmentErrorCode.get() == AssignorError.SHUTDOWN_REQUESTED.code()) {
            log.warn("Detected that shutdown was requested. " +
                    "All clients in this app will now begin to shutdown");
            mainConsumer.enforceRebalance();
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
        if (builder.usesPatternSubscription()) {
            mainConsumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);
        } else {
            mainConsumer.subscribe(builder.sourceTopicCollection(), rebalanceListener);
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
    void runOnce() {
        final long startMs = time.milliseconds();
        now = startMs;

        final long pollLatency = pollPhase();

        // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
        // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
        // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
        // could affect the task manager state beyond this point within #runOnce().
        if (!isRunning()) {
            log.info("Thread state is already {}, skipping the run once call after poll request", state);
            return;
        }

        initializeAndRestorePhase();

        // TODO: we should record the restore latency and its relative time spent ratio after
        //       we figure out how to move this method out of the stream thread
        advanceNowAndComputeLatency();

        int totalProcessed = 0;
        long totalCommitLatency = 0L;
        long totalProcessLatency = 0L;
        long totalPunctuateLatency = 0L;
        if (state == State.RUNNING) {
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
                totalCommittedSinceLastSummary += committed;
                final long commitLatency = Math.max(now - beforeCommitMs, 0);
                totalCommitLatency += commitLatency;
                if (committed > 0) {
                    commitSensor.record(commitLatency / (double) committed, now);

                    if (log.isDebugEnabled()) {
                        log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                            taskManager.activeTaskIds(), taskManager.standbyTaskIds(), commitLatency);
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

    private void initializeAndRestorePhase() {
        // only try to initialize the assigned tasks
        // if the state is still in PARTITION_ASSIGNED after the poll call
        final State stateSnapshot = state;
        if (stateSnapshot == State.PARTITIONS_ASSIGNED
            || stateSnapshot == State.RUNNING && taskManager.needsInitializationOrRestoration()) {

            log.debug("State is {}; initializing tasks if necessary", stateSnapshot);

            // transit to restore active is idempotent so we can call it multiple times
            changelogReader.enforceRestoreActive();

            if (taskManager.tryToCompleteRestoration(now, partitions -> resetOffsets(partitions, null))) {
                changelogReader.transitToUpdateStandby();
                log.info("Restoration took {} ms for all tasks {}", time.milliseconds() - lastPartitionAssignedMs,
                    taskManager.tasks().keySet());
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
        changelogReader.restore(taskManager.tasks());
        log.debug("Idempotent restore call done. Thread state has not changed.");
    }

    private long pollPhase() {
        final ConsumerRecords<byte[], byte[]> records;
        log.debug("Invoking poll on main Consumer");

        if (state == State.PARTITIONS_ASSIGNED) {
            // try to fetch some records with zero poll millis
            // to unblock the restoration as soon as possible
            records = pollRequests(Duration.ZERO);
        } else if (state == State.PARTITIONS_REVOKED) {
            // try to fetch some records with zero poll millis to unblock
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

        final int numRecords = records.count();

        log.debug("Main Consumer poll completed in {} ms and fetched {} records", pollLatency, numRecords);

        pollSensor.record(pollLatency, now);

        if (!records.isEmpty()) {
            pollRecordsSensor.record(numRecords, now);
            taskManager.addRecordsToTasks(records);
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
            if (builder.earliestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if ("earliest".equals(originalReset)) {
                    addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else if ("latest".equals(originalReset)) {
                    addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                } else {
                    notReset.add(partition);
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
                          taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            committed = taskManager.commit(
                taskManager.tasks()
                    .values()
                    .stream()
                    .filter(t -> t.state() == Task.State.RUNNING || t.state() == Task.State.RESTORING)
                    .collect(Collectors.toSet())
            );

            if (committed > 0) {
                // try to purge the committed records for repartition topics if possible
                taskManager.maybePurgeCommittedRecords();
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
        if (leaveGroupRequested.get()) {
            mainConsumer.unsubscribe();
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
            activeTasksMetadata.add(new TaskMetadata(
                task.getValue().id().toString(),
                task.getValue().inputPartitions(),
                task.getValue().committedOffsets(),
                task.getValue().highWaterMark(),
                task.getValue().timeCurrentIdlingStarted()
            ));
        }
        final Set<TaskMetadata> standbyTasksMetadata = new HashSet<>();
        for (final Map.Entry<TaskId, Task> task : standbyTasks.entrySet()) {
            standbyTasksMetadata.add(new TaskMetadata(
                task.getValue().id().toString(),
                task.getValue().inputPartitions(),
                task.getValue().committedOffsets(),
                task.getValue().highWaterMark(),
                task.getValue().timeCurrentIdlingStarted()
            ));
        }

        final String adminClientId = threadMetadata.adminClientId();
        threadMetadata = new ThreadMetadata(
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

    public Optional<String> getGroupInstanceID() {
        return getGroupInstanceID;
    }

    public void requestLeaveGroupDuringShutdown() {
        this.leaveGroupRequested.set(true);
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
