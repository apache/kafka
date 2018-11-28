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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
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

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.DEAD;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.PENDING_SHUTDOWN;

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
    private volatile StreamsException startupException;

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

    public GlobalStreamThread(final ProcessorTopology topology,
                              final StreamsConfig config,
                              final Consumer<byte[], byte[]> globalConsumer,
                              final StateDirectory stateDirectory,
                              final long cacheSizeBytes,
                              final Metrics metrics,
                              final Time time,
                              final String threadClientId,
                              final StateRestoreListener stateRestoreListener) {
        super(threadClientId);
        this.time = time;
        this.config = config;
        this.topology = topology;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId);
        this.logPrefix = String.format("global-stream-thread [%s] ", threadClientId);
        this.logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());
        this.cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);
        this.stateRestoreListener = stateRestoreListener;
    }

    static class StateConsumer {
        private final Consumer<byte[], byte[]> globalConsumer;
        private final GlobalStateMaintainer stateMaintainer;
        private final Time time;
        private final Duration pollTime;
        private final long flushInterval;
        private final Logger log;

        private long lastFlush;

        StateConsumer(final LogContext logContext,
                      final Consumer<byte[], byte[]> globalConsumer,
                      final GlobalStateMaintainer stateMaintainer,
                      final Time time,
                      final Duration pollTime,
                      final long flushInterval) {
            this.log = logContext.logger(getClass());
            this.globalConsumer = globalConsumer;
            this.stateMaintainer = stateMaintainer;
            this.time = time;
            this.pollTime = pollTime;
            this.flushInterval = flushInterval;
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
            lastFlush = time.milliseconds();
        }

        void pollAndUpdate() {
            try {
                final ConsumerRecords<byte[], byte[]> received = globalConsumer.poll(pollTime);
                for (final ConsumerRecord<byte[], byte[]> record : received) {
                    stateMaintainer.update(record);
                }
                final long now = time.milliseconds();
                if (now >= lastFlush + flushInterval) {
                    stateMaintainer.flushState();
                    lastFlush = now;
                }
            } catch (final InvalidOffsetException recoverableException) {
                log.error("Updating global state failed. You can restart KafkaStreams to recover from this error.", recoverableException);
                throw new StreamsException("Updating global state failed. " +
                    "You can restart KafkaStreams to recover from this error.", recoverableException);
            }
        }

        public void close() throws IOException {
            try {
                globalConsumer.close();
            } catch (final RuntimeException e) {
                // just log an error if the consumer throws an exception during close
                // so we can always attempt to close the state stores.
                log.error("Failed to close consumer due to the following error:", e);
            }

            stateMaintainer.close();
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

            log.warn("Error happened during initialization of the global state store; this thread has shutdown");
            streamsMetrics.removeAllThreadLevelSensors();

            return;
        }
        setState(State.RUNNING);

        try {
            while (stillRunning()) {
                stateConsumer.pollAndUpdate();
            }
        } finally {
            // set the state to pending shutdown first as it may be called due to error;
            // its state may already be PENDING_SHUTDOWN so it will return false but we
            // intentionally do not check the returned flag
            setState(State.PENDING_SHUTDOWN);

            log.info("Shutting down");

            try {
                stateConsumer.close();
            } catch (final IOException e) {
                log.error("Failed to close state maintainer due to the following error:", e);
            }

            streamsMetrics.removeAllThreadLevelSensors();

            setState(DEAD);

            log.info("Shutdown complete");
        }
    }

    private StateConsumer initialize() {
        try {
            final GlobalStateManager stateMgr = new GlobalStateManagerImpl(
                logContext,
                topology,
                globalConsumer,
                stateDirectory,
                stateRestoreListener,
                config);

            final GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(
                config,
                stateMgr,
                streamsMetrics,
                cache);
            stateMgr.setGlobalProcessorContext(globalProcessorContext);

            final StateConsumer stateConsumer = new StateConsumer(
                logContext,
                globalConsumer,
                new GlobalStateUpdateTask(
                    topology,
                    globalProcessorContext,
                    stateMgr,
                    config.defaultDeserializationExceptionHandler(),
                    logContext
                ),
                time,
                Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)),
                config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
            );
            stateConsumer.initialize();

            return stateConsumer;
        } catch (final LockException fatalException) {
            final String errorMsg = "Could not lock global state directory. This could happen if multiple KafkaStreams " +
                "instances are running on the same host using the same state directory.";
            log.error(errorMsg, fatalException);
            startupException = new StreamsException(errorMsg, fatalException);
        } catch (final StreamsException fatalException) {
            startupException = fatalException;
        } catch (final Exception fatalException) {
            startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", fatalException);
        }
        return null;
    }

    @Override
    public synchronized void start() {
        super.start();
        while (!stillRunning()) {
            Utils.sleep(1);
            if (startupException != null) {
                throw startupException;
            }
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
}
