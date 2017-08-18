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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.DEAD;
import static org.apache.kafka.streams.processor.internals.GlobalStreamThread.State.PENDING_SHUTDOWN;

/**
 * This is the thread responsible for keeping all Global State Stores updated.
 * It delegates most of the responsibility to the internal class StateConsumer
 */
public class GlobalStreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(GlobalStreamThread.class);

    private final StreamsConfig config;
    private final Consumer<byte[], byte[]> consumer;
    private final StateDirectory stateDirectory;
    private final Time time;
    private final ThreadCache cache;
    private final StreamsMetrics streamsMetrics;
    private final ProcessorTopology topology;
    private volatile StreamsException startupException;

    /**
     * The states that the global stream thread can be in
     *
     * <pre>
     *                +-------------+
     *          +<--- | Created     |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Running     |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *                | Shutdown    |
     *                +-----+-------+
     *                      |
     *                      v
     *                +-----+-------+
     *                | Dead        |
     *                +-------------+
     * </pre>
     *
     * Note the following:
     * - Any state can go to PENDING_SHUTDOWN and subsequently to DEAD
     *
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 2), RUNNING(2), PENDING_SHUTDOWN(3), DEAD;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return !equals(PENDING_SHUTDOWN) && !equals(CREATED) && !equals(DEAD);
        }

        public boolean isValidTransition(final ThreadStateTransitionValidator newState) {
            State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    private volatile State state = State.CREATED;
    private final Object stateLock = new Object();
    private StreamThread.StateListener stateListener = null;
    private final String logPrefix;


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
     * @param ignoreWhenShuttingDownOrDead,       if true, then we'll first check if the state is
     *                                            PENDING_SHUTDOWN or DEAD, and if it is,
     *                                            we immediately return. Effectively this enables
     *                                            a conditional set, under the stateLock lock.
     */
    void setState(final State newState, boolean ignoreWhenShuttingDownOrDead) {
        State oldState;
        synchronized (stateLock) {
            oldState = state;

            if (ignoreWhenShuttingDownOrDead) {
                if (state == PENDING_SHUTDOWN || state == DEAD) {
                    return;
                }
            }

            if (!state.isValidTransition(newState)) {
                log.warn("{} Unexpected state transition from {} to {}.", logPrefix, oldState, newState);
                throw new StreamsException(logPrefix + " Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("{} State transition from {} to {}.", logPrefix, oldState, newState);
            }

            state = newState;
        }
        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }
    }


    public GlobalStreamThread(final ProcessorTopology topology,
                              final StreamsConfig config,
                              final Consumer<byte[], byte[]> globalConsumer,
                              final StateDirectory stateDirectory,
                              final Metrics metrics,
                              final Time time,
                              final String threadClientId) {
        super(threadClientId);
        this.time = time;
        this.config = config;
        this.topology = topology;
        this.consumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        long cacheSizeBytes = Math.max(0, config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) /
                (config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) + 1));
        this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId, Collections.singletonMap("client-id", threadClientId));
        this.cache = new ThreadCache(threadClientId, cacheSizeBytes, streamsMetrics);
        this.logPrefix = String.format("global-stream-thread [%s]", threadClientId);
    }

    static class StateConsumer {
        private final Consumer<byte[], byte[]> consumer;
        private final GlobalStateMaintainer stateMaintainer;
        private final Time time;
        private final long pollMs;
        private final long flushInterval;

        private long lastFlush;

        StateConsumer(final Consumer<byte[], byte[]> consumer,
                      final GlobalStateMaintainer stateMaintainer,
                      final Time time,
                      final long pollMs,
                      final long flushInterval) {
            this.consumer = consumer;
            this.stateMaintainer = stateMaintainer;
            this.time = time;
            this.pollMs = pollMs;
            this.flushInterval = flushInterval;
        }

        void initialize() {
            final Map<TopicPartition, Long> partitionOffsets = stateMaintainer.initialize();
            consumer.assign(partitionOffsets.keySet());
            for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
            lastFlush = time.milliseconds();
        }

        void pollAndUpdate() {
            final ConsumerRecords<byte[], byte[]> received = consumer.poll(pollMs);
            for (ConsumerRecord<byte[], byte[]> record : received) {
                stateMaintainer.update(record);
            }
            final long now = time.milliseconds();
            if (flushInterval >= 0 && now >= lastFlush + flushInterval) {
                stateMaintainer.flushState();
                lastFlush = now;
            }
        }

        public void close() throws IOException {

            // just log an error if the consumer throws an exception during close
            // so we can always attempt to close the state stores.
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Failed to cleanly close GlobalStreamThread consumer", e);
            }

            stateMaintainer.close();

        }
    }


    @Override
    public void run() {
        final StateConsumer stateConsumer = initialize();

        if (stateConsumer == null) {
            return;
        }
        // one could kill the thread before it had a chance to actually start
        setState(State.RUNNING, true);

        try {
            while (stillRunning()) {
                stateConsumer.pollAndUpdate();
            }
            log.debug("Shutting down GlobalStreamThread at user request");
        } finally {
            try {
                setState(PENDING_SHUTDOWN, true);
                stateConsumer.close();
                setState(DEAD, false);
            } catch (IOException e) {
                log.error("Failed to cleanly shutdown GlobalStreamThread", e);
            }
        }
    }

    private StateConsumer initialize() {
        try {
            final GlobalStateManager stateMgr = new GlobalStateManagerImpl(topology, consumer, stateDirectory);
            final StateConsumer stateConsumer
                    = new StateConsumer(consumer,
                                        new GlobalStateUpdateTask(topology,
                                                                  new GlobalProcessorContextImpl(
                                                                          config,
                                                                          stateMgr,
                                                                          streamsMetrics,
                                                                          cache),
                                                                  stateMgr,
                                                                  config.defaultDeserializationExceptionHandler()),
                                        time,
                                        config.getLong(StreamsConfig.POLL_MS_CONFIG),
                                        config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
            stateConsumer.initialize();
            return stateConsumer;
        } catch (StreamsException e) {
            startupException = e;
        } catch (Exception e) {
            startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", e);
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


    public void close() {
        // one could call close() multiple times, so ignore subsequent calls
        // if already shutting down or dead
        setState(PENDING_SHUTDOWN, true);
    }

    public boolean stillRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
    }


}
