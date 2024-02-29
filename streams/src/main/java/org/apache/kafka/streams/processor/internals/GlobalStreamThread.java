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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.CREATED;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.DEAD;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.PENDING_SHUTDOWN;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.RUNNING;

/**
 * This is the thread responsible for keeping all Global State Stores updated.
 * It delegates most of the responsibility to the internal class StateConsumer
 */
public class GlobalStreamThread extends Thread {

    private final Logger log;
    private final LogContext logContext;
    private final StreamsConfig config;
    private final Consumer<byte[], byte[]> globalConsumer;
    private final StateDirectory stateDirectory;
    private final Time time;
    private final ThreadCache cache;
    private final StreamsMetricsImpl streamsMetrics;
    private final ProcessorTopology topology;
    private final AtomicLong cacheSize;
    private volatile StreamsException startupException;
    private java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler;
    private volatile long fetchDeadlineClientInstanceId = -1;
    private volatile KafkaFutureImpl<Uuid> clientInstanceIdFuture = new KafkaFutureImpl<>();

    /**
     * The states that the global stream thread can be in
     *
     * <pre>
     *                +-------------+
     *          +<--- | Created (0) |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Running (1) |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *                | Shutdown (2)|
     *                +-----+-------+
     *                      |
     *                      v
     *                +-----+-------+
     *                | Dead (3)    |
     *                +-------------+
     * </pre>
     *
     * Note the following:
     * <ul>
     *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
     *     <li>State PENDING_SHUTDOWN may want to transit itself. In this case we will forbid the transition but will not treat as an error.</li>
     * </ul>
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 2), RUNNING(2), PENDING_SHUTDOWN(3), DEAD;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return equals(RUNNING);
        }

        public boolean inErrorState() {
            return equals(DEAD) || equals(PENDING_SHUTDOWN);
        }

        @Override
        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            final State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    private volatile State state = State.CREATED;
    private final Object stateLock = new Object();
    private StreamThread.StateListener stateListener = null;
    private final String logPrefix;
    private final StateRestoreListener stateRestoreListener;

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
        // we do not need to use the stat lock since the variable is volatile
        return state;
    }

    /**
     * Sets the state
     *
     * @param newState New state
     */
    private void setState(final State newState) {
        final State oldState = state;

        synchronized (stateLock) {
            if (state == State.PENDING_SHUTDOWN && newState == State.PENDING_SHUTDOWN) {
                // when the state is already in PENDING_SHUTDOWN, its transition to itself
                // will be refused but we do not throw exception here
                return;
            } else if (state == State.DEAD) {
                // when the state is already in NOT_RUNNING, all its transitions
                // will be refused but we do not throw exception here
                return;
            } else if (!state.isValidTransition(newState)) {
                log.error("Unexpected state transition from {} to {}", oldState, newState);
                throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("State transition from {} to {}", oldState, newState);
            }

            state = newState;
        }

        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }
    }

    public boolean stillRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
    }

    public boolean inErrorState() {
        synchronized (stateLock) {
            return state.inErrorState();
        }
    }

    public boolean stillInitializing() {
        synchronized (stateLock) {
            return state.equals(CREATED);
        }
    }

    public GlobalStreamThread(final ProcessorTopology topology,
                              final StreamsConfig config,
                              final Consumer<byte[], byte[]> globalConsumer,
                              final StateDirectory stateDirectory,
                              final long cacheSizeBytes,
                              final StreamsMetricsImpl streamsMetrics,
                              final Time time,
                              final String threadClientId,
                              final StateRestoreListener stateRestoreListener,
                              final java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler) {
        super(threadClientId);
        this.time = time;
        this.config = config;
        this.topology = topology;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.streamsMetrics = streamsMetrics;
        this.logPrefix = String.format("global-stream-thread [%s] ", threadClientId);
        this.logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());
        this.cache = new ThreadCache(logContext, cacheSizeBytes, this.streamsMetrics);
        this.stateRestoreListener = stateRestoreListener;
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
        this.cacheSize = new AtomicLong(-1L);
    }

    static class StateConsumer {
        private final Consumer<byte[], byte[]> globalConsumer;
        private final GlobalStateMaintainer stateMaintainer;
        private final Duration pollTime;
        private final Logger log;

        StateConsumer(final LogContext logContext,
                      final Consumer<byte[], byte[]> globalConsumer,
                      final GlobalStateMaintainer stateMaintainer,
                      final Duration pollTime) {
            this.log = logContext.logger(getClass());
            this.globalConsumer = globalConsumer;
            this.stateMaintainer = stateMaintainer;
            this.pollTime = pollTime;
        }

        /**
         * @throws IllegalStateException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        void initialize() {
            final Map<TopicPartition, Long> partitionOffsets = stateMaintainer.initialize();
            globalConsumer.assign(partitionOffsets.keySet());
            for (final Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                globalConsumer.seek(entry.getKey(), entry.getValue());
            }
        }

        void pollAndUpdate() {
            final ConsumerRecords<byte[], byte[]> received = globalConsumer.poll(pollTime);
            for (final ConsumerRecord<byte[], byte[]> record : received) {
                stateMaintainer.update(record);
            }
            stateMaintainer.maybeCheckpoint();
        }

        public void close(final boolean wipeStateStore) throws IOException {
            try {
                globalConsumer.close();
            } catch (final RuntimeException e) {
                // just log an error if the consumer throws an exception during close
                // so we can always attempt to close the state stores.
                log.error("Failed to close global consumer due to the following error:", e);
            }

            stateMaintainer.close(wipeStateStore);
        }
    }

    @Override
    public void run() {
        final StateConsumer stateConsumer = initialize();

        if (stateConsumer == null) {
            // during initialization, the caller thread would wait for the state consumer
            // to restore the global state store before transiting to RUNNING state and return;
            // if an error happens during the restoration process, the stateConsumer will be null
            // and in this case we will transit the state to PENDING_SHUTDOWN and DEAD immediately.
            // the exception will be thrown in the caller thread during start() function.
            setState(State.PENDING_SHUTDOWN);
            setState(State.DEAD);

            log.error("Error happened during initialization of the global state store; this thread has shutdown.");
            streamsMetrics.removeAllThreadLevelSensors(getName());
            streamsMetrics.removeAllThreadLevelMetrics(getName());

            return;
        }
        setState(RUNNING);

        boolean wipeStateStore = false;
        try {
            while (stillRunning()) {
                final long size = cacheSize.getAndSet(-1L);
                if (size != -1L) {
                    cache.resize(size);
                }
                stateConsumer.pollAndUpdate();

                if (fetchDeadlineClientInstanceId != -1) {
                    if (fetchDeadlineClientInstanceId >= time.milliseconds()) {
                        try {
                            // we pass in a timeout of zero, to just trigger the "get instance id" background RPC,
                            // we don't want to block the global thread that can do useful work in the meantime
                            clientInstanceIdFuture.complete(globalConsumer.clientInstanceId(Duration.ZERO));
                            fetchDeadlineClientInstanceId = -1;
                        } catch (final IllegalStateException disabledError) {
                            // if telemetry is disabled on a client, we swallow the error,
                            // to allow returning a partial result for all other clients
                            clientInstanceIdFuture.complete(null);
                            fetchDeadlineClientInstanceId = -1;
                        } catch (final TimeoutException swallow) {
                            // swallow
                        } catch (final Exception error) {
                            clientInstanceIdFuture.completeExceptionally(error);
                            fetchDeadlineClientInstanceId = -1;
                        }
                    } else {
                        clientInstanceIdFuture.completeExceptionally(
                            new TimeoutException("Could not retrieve global consumer client instance id.")
                        );
                        fetchDeadlineClientInstanceId = -1;
                    }
                }
            }
        } catch (final InvalidOffsetException recoverableException) {
            wipeStateStore = true;
            log.error(
                "Updating global state failed due to inconsistent local state. Will attempt to clean up the local state. You can restart KafkaStreams to recover from this error.",
                recoverableException
            );
            final StreamsException e = new StreamsException(
                "Updating global state failed. You can restart KafkaStreams to launch a new GlobalStreamThread to recover from this error.",
                recoverableException
            );
            this.streamsUncaughtExceptionHandler.accept(e);
        } catch (final Exception e) {
            log.error("Error happened while maintaining global state store. The streams application or client will now close to ERROR.", e);
            this.streamsUncaughtExceptionHandler.accept(e);
        } finally {
            // set the state to pending shutdown first as it may be called due to error;
            // its state may already be PENDING_SHUTDOWN so it will return false but we
            // intentionally do not check the returned flag
            setState(State.PENDING_SHUTDOWN);

            log.info("Shutting down");

            try {
                stateConsumer.close(wipeStateStore);
            } catch (final IOException e) {
                log.error("Failed to close state maintainer due to the following error:", e);
            }

            streamsMetrics.removeAllThreadLevelSensors(getName());
            streamsMetrics.removeAllThreadLevelMetrics(getName());

            setState(DEAD);

            log.info("Shutdown complete");
        }
    }

    public void setUncaughtExceptionHandler(final java.util.function.Consumer<Throwable> streamsUncaughtExceptionHandler) {
        this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
    }

    public void resize(final long cacheSize) {
        this.cacheSize.set(cacheSize);
    }

    private StateConsumer initialize() {
        StateConsumer stateConsumer = null;
        try {
            final GlobalStateManager stateMgr = new GlobalStateManagerImpl(
                logContext,
                time,
                topology,
                globalConsumer,
                stateDirectory,
                stateRestoreListener,
                config
            );

            final GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(
                config,
                stateMgr,
                streamsMetrics,
                cache,
                time
            );
            stateMgr.setGlobalProcessorContext(globalProcessorContext);

            stateConsumer = new StateConsumer(
                logContext,
                globalConsumer,
                new GlobalStateUpdateTask(
                    logContext,
                    topology,
                    globalProcessorContext,
                    stateMgr,
                    config.defaultDeserializationExceptionHandler(),
                    time,
                    config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
                ),
                Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG))
            );

            try {
                stateConsumer.initialize();
            } catch (final InvalidOffsetException recoverableException) {
                log.error(
                    "Bootstrapping global state failed due to inconsistent local state. Will attempt to clean up the local state. You can restart KafkaStreams to recover from this error.",
                    recoverableException
                );

                closeStateConsumer(stateConsumer, true);

                throw new StreamsException(
                    "Bootstrapping global state failed. You can restart KafkaStreams to recover from this error.",
                    recoverableException
                );
            }

            return stateConsumer;
        } catch (final StreamsException fatalException) {
            closeStateConsumer(stateConsumer, false);
            startupException = fatalException;
        } catch (final Exception fatalException) {
            closeStateConsumer(stateConsumer, false);
            startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", fatalException);
        }
        return null;
    }

    private void closeStateConsumer(final StateConsumer stateConsumer, final boolean wipeStateStore) {
        if (stateConsumer != null) {
            try {
                stateConsumer.close(wipeStateStore);
            } catch (final IOException e) {
                log.error("Failed to close state consumer due to the following error:", e);
            }
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        while (stillInitializing()) {
            Utils.sleep(1);
            if (startupException != null) {
                throw startupException;
            }
        }

        if (inErrorState()) {
            throw new IllegalStateException("Initialization for the global stream thread failed");
        }
    }

    public void shutdown() {
        // one could call shutdown() multiple times, so ignore subsequent calls
        // if already shutting down or dead
        setState(PENDING_SHUTDOWN);
    }

    public Map<MetricName, Metric> consumerMetrics() {
        return Collections.unmodifiableMap(globalConsumer.metrics());
    }

    // this method is NOT thread-safe (we rely on the callee to be `synchronized`)
    public KafkaFuture<Uuid> globalConsumerInstanceId(final Duration timeout) {
        boolean setDeadline = false;

        if (clientInstanceIdFuture.isDone()) {
            if (clientInstanceIdFuture.isCompletedExceptionally()) {
                clientInstanceIdFuture = new KafkaFutureImpl<>();
                setDeadline = true;
            }
        } else {
            setDeadline = true;
        }

        if (setDeadline) {
            fetchDeadlineClientInstanceId = time.milliseconds() + timeout.toMillis();
        }

        return clientInstanceIdFuture;
    }
}
