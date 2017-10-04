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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.processor.internals.ThreadStateTransitionValidator;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.internals.GlobalStateStoreProvider;
import org.apache.kafka.streams.state.internals.QueryableStoreProvider;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.StreamThreadStateStoreProvider;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;

/**
 * A Kafka client that allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero, one, or more output topics.
 * <p>
 * The computational logic can be specified either by using the {@link Topology} to define a DAG topology of
 * {@link Processor}s or by using the {@link StreamsBuilder} which provides the high-level DSL to define
 * transformations.
 * <p>
 * One {@code KafkaStreams} instance can contain one or more threads specified in the configs for the processing work.
 * <p>
 * A {@code KafkaStreams} instance can co-ordinate with any other instances with the same
 * {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} (whether in the same process, on other processes on this
 * machine, or on remote machines) as a single (possibly distributed) stream processing application.
 * These instances will divide up the work based on the assignment of the input topic partitions so that all partitions
 * are being consumed.
 * If instances are added or fail, all (remaining) instances will rebalance the partition assignment among themselves
 * to balance processing load and ensure that all input topic partitions are processed.
 * <p>
 * Internally a {@code KafkaStreams} instance contains a normal {@link KafkaProducer} and {@link KafkaConsumer} instance
 * that is used for reading input and writing output.
 * <p>
 * A simple example might look like this:
 * <pre>{@code
 * Map<String, Object> props = new HashMap<>();
 * props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
 * props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 * props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 * StreamsConfig config = new StreamsConfig(props);
 *
 * KStreamBuilder builder = new KStreamBuilder();
 * builder.stream("my-input-topic").mapValues(value -> value.length().toString()).to("my-output-topic");
 *
 * KafkaStreams streams = new KafkaStreams(builder, config);
 * streams.start();
 * }</pre>
 *
 * @see org.apache.kafka.streams.StreamsBuilder
 * @see org.apache.kafka.streams.Topology
 */
@InterfaceStability.Evolving
public class KafkaStreams {

    private static final String JMX_PREFIX = "kafka.streams";
    private static final int DEFAULT_CLOSE_TIMEOUT = 0;

    // processId is expected to be unique across JVMs and to be used
    // in userData of the subscription request to allow assignor be aware
    // of the co-location of stream thread's consumers. It is for internal
    // usage only and should not be exposed to users at all.
    private final Logger log;
    private final String logPrefix;
    private final UUID processId;
    private final Metrics metrics;
    private final StreamsConfig config;
    private final StreamThread[] threads;
    private final StateDirectory stateDirectory;
    private final StreamsMetadataState streamsMetadataState;
    private final ScheduledExecutorService stateDirCleaner;
    private final QueryableStoreProvider queryableStoreProvider;

    private GlobalStreamThread globalStreamThread;
    private KafkaStreams.StateListener stateListener;
    private StateRestoreListener globalStateRestoreListener;

    // container states
    /**
     * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
     * An instance must only be in one state at a time.
     * The expected state transition with the following defined states is:
     *
     * <pre>
     *                 +--------------+
     *         +<----- | Created (0)  |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +--------------+
     *         +<----- | Running (2)  | -------->+
     *         |       +----+--+------+          |
     *         |            |  ^                 |
     *         |            v  |                 |
     *         |       +----+--+------+          |
     *         |       | Re-          |          v
     *         |       | Balancing (1)| -------->+
     *         |       +-----+--------+          |
     *         |             |                   |
     *         |             v                   v
     *         |       +-----+--------+     +----+-------+
     *         +-----> | Pending      |<--- | Error (5)  |
     *                 | Shutdown (3) |     +------------+
     *                 +-----+--------+
     *                       |
     *                       v
     *                 +-----+--------+
     *                 | Not          |
     *                 | Running (4)  |
     *                 +--------------+
     *
     *
     * </pre>
     * Note the following:
     * - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED state
     * - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
     * - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
     * - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
     *   the instance will be in the ERROR state. The user will need to close it.
     */
    public enum State {
        CREATED(2, 3), REBALANCING(2, 3, 5), RUNNING(1, 3, 5), PENDING_SHUTDOWN(4), NOT_RUNNING, ERROR(3);

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return equals(RUNNING) || equals(REBALANCING);
        }

        public boolean isValidTransition(final State newState) {
            return validTransitions.contains(newState.ordinal());
        }
    }

    private final Object stateLock = new Object();
    private volatile State state = State.CREATED;

    private boolean waitOnState(final State targetState, final long waitMs) {
        long begin = System.currentTimeMillis();
        synchronized (stateLock) {
            long elapsedMs = 0L;
            while (state != State.NOT_RUNNING) {
                if (waitMs == 0) {
                    try {
                        stateLock.wait();
                    } catch (final InterruptedException e) {
                        // it is ok: just move on to the next iteration
                    }
                } else if (waitMs > elapsedMs) {
                    long remainingMs = waitMs - elapsedMs;
                    try {
                        stateLock.wait(remainingMs);
                    } catch (final InterruptedException e) {
                        // it is ok: just move on to the next iteration
                    }
                } else {
                    log.debug("Cannot transit to {} within {}ms", targetState, waitMs);
                    return false;
                }
                elapsedMs = System.currentTimeMillis() - begin;
            }
            return true;
        }
    }

    /**
     * Sets the state
     * @param newState New state
     */
    private boolean setState(final State newState) {
        final State oldState = state;

        synchronized (stateLock) {
            if (state == State.PENDING_SHUTDOWN && newState != State.NOT_RUNNING) {
                // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                // refused but we do not throw exception here, to allow appropriate error handling
                return false;
            } else if (state == State.NOT_RUNNING && (newState == State.PENDING_SHUTDOWN || newState == State.NOT_RUNNING)) {
                // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                // will be refused but we do not throw exception here, to allow idempotent close calls
                return false;
            } else if (!state.isValidTransition(newState)) {
                log.error("Unexpected state transition from {} to {}", oldState, newState);
                throw new IllegalStateException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("State transition from {} to {}", oldState, newState);
            }
            state = newState;
            stateLock.notifyAll();
        }

        // we need to call the user customized state listener outside the state lock to avoid potential deadlocks
        if (stateListener != null) {
            stateListener.onChange(state, oldState);
        }

        return true;
    }

    private boolean setRunningFromCreated() {
        synchronized (stateLock) {
            if (state != State.CREATED) {
                log.error("{} Unexpected state transition from {} to {}", logPrefix, state, State.RUNNING);
                throw new IllegalStateException(logPrefix + " Unexpected state transition from " + state + " to " + State.RUNNING);
            }
            state = State.RUNNING;
            stateLock.notifyAll();
        }

        // we need to call the user customized state listener outside the state lock to avoid potential deadlocks
        if (stateListener != null) {
            stateListener.onChange(State.RUNNING, State.CREATED);
        }

        return true;
    }

    /**
     * Return the current {@link State} of this {@code KafkaStreams} instance.
     *
     * @return the current state of this Kafka Streams instance
     */
    public State state() {
        return state;
    }

    private boolean isRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
    }

    private void validateIsRunning() {
        if (!isRunning()) {
            throw new IllegalStateException("KafkaStreams is not running. State is " + state + ".");
        }
    }
    /**
     * Listen to {@link State} change events.
     */
    public interface StateListener {

        /**
         * Called when state changes.
         *
         * @param newState new state
         * @param oldState previous state
         */
        void onChange(final State newState, final State oldState);
    }

    /**
     * An app can set a single {@link KafkaStreams.StateListener} so that the app is notified when state changes.
     *
     * @param listener a new state listener
     * @throws IllegalStateException if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
     */
    public void setStateListener(final KafkaStreams.StateListener listener) {
        if (state == State.CREATED) {
            stateListener = listener;
        } else {
            throw new IllegalStateException("Can only set StateListener in CREATED state.");
        }
    }

    /**
     * Set the handler invoked when a {@link StreamsConfig#NUM_STREAM_THREADS_CONFIG internal thread} abruptly
     * terminates due to an uncaught exception.
     *
     * @param eh the uncaught exception handler for all internal threads; {@code null} deletes the current handler
     * @throws IllegalStateException if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
     */
    public void setUncaughtExceptionHandler(final Thread.UncaughtExceptionHandler eh) {
        if (state == State.CREATED) {
            for (final StreamThread thread : threads) {
                thread.setUncaughtExceptionHandler(eh);
            }

            if (globalStreamThread != null) {
                globalStreamThread.setUncaughtExceptionHandler(eh);
            }
        } else {
            throw new IllegalStateException("Can only set UncaughtExceptionHandler in CREATED state.");
        }
    }

    /**
     * Set the listener which is triggered whenever a {@link StateStore} is being restored in order to resume
     * processing.
     *
     * @param globalStateRestoreListener The listener triggered when {@link StateStore} is being restored.
     * @throws IllegalStateException if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
     */
    public void setGlobalStateRestoreListener(final StateRestoreListener globalStateRestoreListener) {
        if (state == State.CREATED) {
            this.globalStateRestoreListener = globalStateRestoreListener;
        } else {
            throw new IllegalStateException("Can only set the GlobalRestoreListener in the CREATED state");
        }
    }

    /**
     * Get read-only handle on global metrics registry.
     *
     * @return Map of all metrics.
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(metrics.metrics());
    }

    /**
     * Class that handles stream thread transitions
     */
    final class StreamStateListener implements StreamThread.StateListener {
        private final Map<Long, StreamThread.State> threadState;
        private GlobalStreamThread.State globalThreadState;

        StreamStateListener(final Map<Long, StreamThread.State> threadState,
                            final GlobalStreamThread.State globalThreadState) {
            this.threadState = threadState;
            this.globalThreadState = globalThreadState;
        }

        /**
         * If all threads are dead set to ERROR
         */
        private void maybeSetError() {
            // check if we have enough threads running
            for (final StreamThread.State state : threadState.values()) {
                if (state != StreamThread.State.DEAD) {
                    return;
                }
            }

            if (setState(State.ERROR)) {
                log.warn("All stream threads have died. The instance will be in error state and should be closed.");
            }
        }

        /**
         * If all threads are up, including the global thread, set to RUNNING
         */
        private void maybeSetRunning() {
            // one thread is running, check others, including global thread
            for (final StreamThread.State state : threadState.values()) {
                if (state != StreamThread.State.RUNNING) {
                    return;
                }
            }
            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            if (globalThreadState != null && globalThreadState != GlobalStreamThread.State.RUNNING) {
                return;
            }

            setState(State.RUNNING);
        }


        @Override
        public synchronized void onChange(final Thread thread,
                                          final ThreadStateTransitionValidator abstractNewState,
                                          final ThreadStateTransitionValidator abstractOldState) {
            // StreamThreads first
            if (thread instanceof StreamThread) {
                StreamThread.State newState = (StreamThread.State) abstractNewState;
                threadState.put(thread.getId(), newState);

                if (newState == StreamThread.State.PARTITIONS_REVOKED && state != State.REBALANCING) {
                    setState(State.REBALANCING);
                } else if (newState == StreamThread.State.RUNNING && state != State.RUNNING) {
                    maybeSetRunning();
                } else if (newState == StreamThread.State.DEAD && state != State.ERROR) {
                    maybeSetError();
                }
            } else if (thread instanceof GlobalStreamThread) {
                // global stream thread has different invariants
                GlobalStreamThread.State newState = (GlobalStreamThread.State) abstractNewState;
                globalThreadState = newState;

                // special case when global thread is dead
                if (newState == GlobalStreamThread.State.DEAD && state != State.ERROR && setState(State.ERROR)) {
                    log.warn("Global thread has died. The instance will be in error state and should be closed.");
                }
            }
        }
    }

    /**
     * @deprecated use {@link #KafkaStreams(Topology, Properties)} instead
     */
    @Deprecated
    public KafkaStreams(final org.apache.kafka.streams.processor.TopologyBuilder builder,
                        final Properties props) {
        this(builder.internalTopologyBuilder, new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * @deprecated use {@link #KafkaStreams(Topology, StreamsConfig)} instead
     */
    @Deprecated
    public KafkaStreams(final org.apache.kafka.streams.processor.TopologyBuilder builder,
                        final StreamsConfig config) {
        this(builder.internalTopologyBuilder, config, new DefaultKafkaClientSupplier());
    }

    /**
     * @deprecated use {@link #KafkaStreams(Topology, StreamsConfig, KafkaClientSupplier)} instead
     */
    @Deprecated
    public KafkaStreams(final org.apache.kafka.streams.processor.TopologyBuilder builder,
                        final StreamsConfig config,
                        final KafkaClientSupplier clientSupplier) {
        this(builder.internalTopologyBuilder, config, clientSupplier);
    }

    /**
     * Create a {@code KafkaStreams} instance.
     *
     * @param topology the topology specifying the computational logic
     * @param props   properties for {@link StreamsConfig}
     * @throws StreamsException if any fatal error occurs
     */
    public KafkaStreams(final Topology topology,
                        final Properties props) {
        this(topology.internalTopologyBuilder, new StreamsConfig(props), new DefaultKafkaClientSupplier());
    }

    /**
     * Create a {@code KafkaStreams} instance.
     *
     * @param topology the topology specifying the computational logic
     * @param config  the Kafka Streams configuration
     * @throws StreamsException if any fatal error occurs
     */
    public KafkaStreams(final Topology topology,
                        final StreamsConfig config) {
        this(topology.internalTopologyBuilder, config, new DefaultKafkaClientSupplier());
    }

    /**
     * Create a {@code KafkaStreams} instance.
     *
     * @param topology       the topology specifying the computational logic
     * @param config         the Kafka Streams configuration
     * @param clientSupplier the Kafka clients supplier which provides underlying producer and consumer clients
     *                       for the new {@code KafkaStreams} instance
     * @throws StreamsException if any fatal error occurs
     */
    public KafkaStreams(final Topology topology,
                        final StreamsConfig config,
                        final KafkaClientSupplier clientSupplier) {
        this(topology.internalTopologyBuilder, config, clientSupplier);
    }

    private KafkaStreams(final InternalTopologyBuilder internalTopologyBuilder,
                         final StreamsConfig config,
                         final KafkaClientSupplier clientSupplier) throws StreamsException {
        this.config = config;

        // The application ID is a required config and hence should always have value
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        processId = UUID.randomUUID();

        String clientId = config.getString(StreamsConfig.CLIENT_ID_CONFIG);
        if (clientId.length() <= 0) {
            clientId = applicationId + "-" + processId;
        }

        this.logPrefix = String.format("stream-client [%s]", clientId);
        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());
        final String cleanupThreadName = clientId + "-CleanupThread";

        internalTopologyBuilder.setApplicationId(applicationId);
        // sanity check to fail-fast in case we cannot build a ProcessorTopology due to an exception
        internalTopologyBuilder.build(null);

        long cacheSize = config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG);
        if (cacheSize < 0) {
            cacheSize = 0;
            log.warn("Negative cache size passed in. Reverting to cache size of 0 bytes.");
        }

        final StateRestoreListener delegatingStateRestoreListener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition, final String storeName, final long startingOffset, final long endingOffset) {
                if (globalStateRestoreListener != null) {
                    try {
                        globalStateRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
                    } catch (final Exception fatalUserException) {
                        throw new StreamsException(
                            String.format("Fatal user code error in store restore listener for store %s, partition %s.",
                                storeName,
                                topicPartition),
                            fatalUserException);
                    }
                }
            }

            @Override
            public void onBatchRestored(final TopicPartition topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {
                if (globalStateRestoreListener != null) {
                    try {
                        globalStateRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
                    } catch (final Exception fatalUserException) {
                        throw new StreamsException(
                            String.format("Fatal user code error in store restore listener for store %s, partition %s.",
                                storeName,
                                topicPartition),
                            fatalUserException);
                    }
                }
            }

            @Override
            public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
                if (globalStateRestoreListener != null) {
                    try {
                        globalStateRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
                    } catch (final Exception fatalUserException) {
                        throw new StreamsException(
                            String.format("Fatal user code error in store restore listener for store %s, partition %s.",
                                storeName,
                                topicPartition),
                            fatalUserException);
                    }
                }
            }
        };

        threads = new StreamThread[config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG)];
        try {
            stateDirectory = new StateDirectory(
                applicationId,
                config.getString(StreamsConfig.STATE_DIR_CONFIG),
                Time.SYSTEM);
        } catch (final ProcessorStateException fatal) {
            throw new StreamsException(fatal);
        }
        streamsMetadataState = new StreamsMetadataState(
            internalTopologyBuilder,
            parseHostInfo(config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG)));

        final MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                TimeUnit.MILLISECONDS);
        final List<MetricsReporter> reporters = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));
        metrics = new Metrics(metricConfig, reporters, Time.SYSTEM);

        GlobalStreamThread.State globalThreadState = null;
        final ProcessorTopology globalTaskTopology = internalTopologyBuilder.buildGlobalStateTopology();
        if (globalTaskTopology != null) {
            final String globalThreadId = clientId + "-GlobalStreamThread";
            globalStreamThread = new GlobalStreamThread(globalTaskTopology,
                                                        config,
                                                        clientSupplier.getRestoreConsumer(config.getRestoreConsumerConfigs(clientId + "-global")),
                                                        stateDirectory,
                                                        metrics,
                                                        Time.SYSTEM,
                                                        globalThreadId,
                                                        delegatingStateRestoreListener);
            globalThreadState = globalStreamThread.state();
        }

        final Map<Long, StreamThread.State> threadState = new HashMap<>(threads.length);
        final ArrayList<StateStoreProvider> storeProviders = new ArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = StreamThread.create(internalTopologyBuilder,
                                             config,
                                             clientSupplier,
                                             processId,
                                             clientId,
                                             metrics,
                                             Time.SYSTEM,
                                             streamsMetadataState,
                                             cacheSize / (threads.length + (globalTaskTopology == null ? 0 : 1)),
                                             stateDirectory,
                                             delegatingStateRestoreListener);
            threadState.put(threads[i].getId(), threads[i].state());
            storeProviders.add(new StreamThreadStateStoreProvider(threads[i]));
        }

        final StreamStateListener streamStateListener = new StreamStateListener(threadState, globalThreadState);
        if (globalTaskTopology != null) {
            globalStreamThread.setStateListener(streamStateListener);
        }
        for (StreamThread thread : threads) {
            thread.setStateListener(streamStateListener);
        }

        final GlobalStateStoreProvider globalStateStoreProvider = new GlobalStateStoreProvider(internalTopologyBuilder.globalStateStores());
        queryableStoreProvider = new QueryableStoreProvider(storeProviders, globalStateStoreProvider);
        stateDirCleaner = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r, cleanupThreadName);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private static HostInfo parseHostInfo(final String endPoint) {
        if (endPoint == null || endPoint.trim().isEmpty()) {
            return StreamsMetadataState.UNKNOWN_HOST;
        }
        final String host = getHost(endPoint);
        final Integer port = getPort(endPoint);

        if (host == null || port == null) {
            throw new ConfigException(String.format("Error parsing host address %s. Expected format host:port.", endPoint));
        }

        return new HostInfo(host, port);
    }

    /**
     * Check if the used brokers have version 0.10.1.x or higher.
     * <p>
     * Note, for <em>pre</em> 0.10.x brokers the broker version cannot be checked and the client will hang and retry
     * until it {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.
     *
     * @throws StreamsException if brokers have version 0.10.0.x
     */
    private void checkBrokerVersionCompatibility() throws StreamsException {
        final StreamsKafkaClient client = StreamsKafkaClient.create(config);

        client.checkBrokerCompatibility(EXACTLY_ONCE.equals(config.getString(PROCESSING_GUARANTEE_CONFIG)));

        try {
            client.close();
        } catch (final IOException e) {
            log.warn("Could not close StreamKafkaClient.", e);
        }

    }

    /**
     * Start the {@code KafkaStreams} instance by starting all its threads.
     * This function is expected to be called only once during the life cycle of the client.
     * <p>
     * Note, for brokers with version {@code 0.9.x} or lower, the broker version cannot be checked.
     * There will be no error and the client will hang and retry to verify the broker version until it
     * {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.

     * @throws IllegalStateException if process was already started
     * @throws StreamsException if the Kafka brokers have version 0.10.0.x or
     *                          if {@link StreamsConfig#PROCESSING_GUARANTEE_CONFIG exactly-once} is enabled for pre 0.11.0.x brokers
     */
    public synchronized void start() throws IllegalStateException, StreamsException {
        log.debug("Starting Streams client");

        // first set state to RUNNING before kicking off the threads,
        // making sure the state will always transit to RUNNING before REBALANCING
        if (setRunningFromCreated()) {
            checkBrokerVersionCompatibility();

            if (globalStreamThread != null) {
                globalStreamThread.start();
            }

            for (final StreamThread thread : threads) {
                thread.start();
            }

            final Long cleanupDelay = config.getLong(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG);
            stateDirCleaner.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (state == State.RUNNING) {
                        stateDirectory.cleanRemovedTasks(cleanupDelay);
                    }
                }
            }, cleanupDelay, cleanupDelay, TimeUnit.MILLISECONDS);

            log.info("Started Streams client");
        } else {
            // if transition failed but no exception is thrown; currently it is not possible
            // since we do not allow calling start multiple times whether or not it is already shutdown.
            // TODO: In the future if we lift this restriction this code path could then be triggered and be updated
            log.error("Already stopped, cannot re-start");
        }
    }

    /**
     * Shutdown this {@code KafkaStreams} instance by signaling all the threads to stop, and then wait for them to join.
     * This will block until all threads have stopped.
     */
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * Shutdown this {@code KafkaStreams} by signaling all the threads to stop, and then wait up to the timeout for the
     * threads to join.
     * A {@code timeout} of 0 means to wait forever.
     *
     * @param timeout  how long to wait for the threads to shutdown
     * @param timeUnit unit of time used for timeout
     * @return {@code true} if all threads were successfully stopped&mdash;{@code false} if the timeout was reached
     * before all threads stopped
     * Note that this method must not be called in the {@code onChange} callback of {@link StateListener}.
     */
    public synchronized boolean close(final long timeout, final TimeUnit timeUnit) {
        log.debug("Stopping Streams client with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));

        if (!setState(State.PENDING_SHUTDOWN)) {
            // if transition failed, it means it was either in PENDING_SHUTDOWN
            // or NOT_RUNNING already; just check that all threads have been stopped
            log.info("Already in the pending shutdown state, wait to complete shutdown");
        } else {
            stateDirCleaner.shutdownNow();

            // notify all the threads to stop; avoid deadlocks by stopping any
            // further state reports from the thread since we're shutting down
            for (final StreamThread thread : threads) {
                thread.setStateListener(null);
                thread.shutdown();
            }
            if (globalStreamThread != null) {
                globalStreamThread.setStateListener(null);
                globalStreamThread.shutdown();
            }

            // wait for all threads to join in a separate thread;
            // save the current thread so that if it is a stream thread
            // we don't attempt to join it and cause a deadlock
            final Thread shutdownThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (final StreamThread thread : threads) {
                        try {
                            if (!thread.isRunning()) {
                                thread.join();
                            }
                        } catch (final InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (globalStreamThread != null && !globalStreamThread.stillRunning()) {
                        try {
                            globalStreamThread.join();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        globalStreamThread = null;
                    }

                    metrics.close();
                    setState(State.NOT_RUNNING);
                }
            }, "kafka-streams-close-thread");

            shutdownThread.setDaemon(true);
            shutdownThread.start();
        }

        if (waitOnState(State.NOT_RUNNING, timeUnit.toMillis(timeout))) {
            log.info("Streams client stopped completely");
            return true;
        } else {
            log.info("Streams client cannot stop completely within the timeout");
            return false;
        }
    }

    /**
     * Produce a string representation containing useful information about this {@code KafkaStream} instance such as
     * thread IDs, task IDs, and a representation of the topology DAG including {@link StateStore}s (cf.
     * {@link Topology} and {@link StreamsBuilder}).
     *
     * @return A string representation of the Kafka Streams instance.
     *
     * @deprecated Use {@link #localThreadsMetadata()} to retrieve runtime information.
     */
    @Override
    @Deprecated
    public String toString() {
        return toString("");
    }

    /**
     * Produce a string representation containing useful information about this {@code KafkaStream} instance such as
     * thread IDs, task IDs, and a representation of the topology DAG including {@link StateStore}s (cf.
     * {@link Topology} and {@link StreamsBuilder}).
     *
     * @param indent the top-level indent for each line
     * @return A string representation of the Kafka Streams instance.
     *
     * @deprecated Use {@link #localThreadsMetadata()} to retrieve runtime information.
     */
    @Deprecated
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder()
            .append(indent)
            .append("KafkaStreams processID: ")
            .append(processId)
            .append("\n");
        for (final StreamThread thread : threads) {
            sb.append(thread.toString(indent + "\t"));
        }
        sb.append("\n");

        return sb.toString();
    }

    /**
     * Do a clean up of the local {@link StateStore} directory ({@link StreamsConfig#STATE_DIR_CONFIG}) by deleting all
     * data with regard to the {@link StreamsConfig#APPLICATION_ID_CONFIG application ID}.
     * <p>
     * May only be called either before this {@code KafkaStreams} instance is {@link #start() started} or after the
     * instance is {@link #close() closed}.
     * <p>
     * Calling this method triggers a restore of local {@link StateStore}s on the next {@link #start() application start}.
     *
     * @throws IllegalStateException if this {@code KafkaStreams} instance is currently {@link State#RUNNING running}
     */
    public void cleanUp() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot clean up while running.");
        }
        stateDirectory.cleanRemovedTasks(0);
    }

    /**
     * Find all currently running {@code KafkaStreams} instances (potentially remotely) that use the same
     * {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all instances that belong to
     * the same Kafka Streams application) and return {@link StreamsMetadata} for each discovered instance.
     * <p>
     * Note: this is a point in time view and it may change due to partition reassignment.
     *
     * @return {@link StreamsMetadata} for each {@code KafkaStreams} instances of this application
     */
    public Collection<StreamsMetadata> allMetadata() {
        validateIsRunning();
        return streamsMetadataState.getAllMetadata();
    }

    /**
     * Find all currently running {@code KafkaStreams} instances (potentially remotely) that
     * <ul>
     *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
     *       instances that belong to the same Kafka Streams application)</li>
     *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
     * </ul>
     * and return {@link StreamsMetadata} for each discovered instance.
     * <p>
     * Note: this is a point in time view and it may change due to partition reassignment.
     *
     * @param storeName the {@code storeName} to find metadata for
     * @return {@link StreamsMetadata} for each {@code KafkaStreams} instances with the provide {@code storeName} of
     * this application
     */
    public Collection<StreamsMetadata> allMetadataForStore(final String storeName) {
        validateIsRunning();
        return streamsMetadataState.getAllMetadataForStore(storeName);
    }

    /**
     * Find the currently running {@code KafkaStreams} instance (potentially remotely) that
     * <ul>
     *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
     *       instances that belong to the same Kafka Streams application)</li>
     *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
     *   <li>and the {@link StateStore} contains the given {@code key}</li>
     * </ul>
     * and return {@link StreamsMetadata} for it.
     * <p>
     * This will use the default Kafka Streams partitioner to locate the partition.
     * If a {@link StreamPartitioner custom partitioner} has been
     * {@link ProducerConfig#PARTITIONER_CLASS_CONFIG configured} via {@link StreamsConfig} or
     * {@link KStream#through(String, Produced)}, or if the original {@link KTable}'s input
     * {@link StreamsBuilder#table(String) topic} is partitioned differently, please use
     * {@link #metadataForKey(String, Object, StreamPartitioner)}.
     * <p>
     * Note:
     * <ul>
     *   <li>this is a point in time view and it may change due to partition reassignment</li>
     *   <li>the key may not exist in the {@link StateStore}; this method provides a way of finding which host it
     *       <em>would</em> exist on</li>
     *   <li>if this is for a window store the serializer should be the serializer for the record key,
     *       not the window serializer</li>
     * </ul>
     *
     * @param storeName     the {@code storeName} to find metadata for
     * @param key           the key to find metadata for
     * @param keySerializer serializer for the key
     * @param <K>           key type
     * @return {@link StreamsMetadata} for the {@code KafkaStreams} instance with the provide {@code storeName} and
     * {@code key} of this application or {@link StreamsMetadata#NOT_AVAILABLE} if Kafka Streams is (re-)initializing
     */
    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final Serializer<K> keySerializer) {
        validateIsRunning();
        return streamsMetadataState.getMetadataWithKey(storeName, key, keySerializer);
    }

    /**
     * Find the currently running {@code KafkaStreams} instance (potentially remotely) that
     * <ul>
     *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
     *       instances that belong to the same Kafka Streams application)</li>
     *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
     *   <li>and the {@link StateStore} contains the given {@code key}</li>
     * </ul>
     * and return {@link StreamsMetadata} for it.
     * <p>
     * Note:
     * <ul>
     *   <li>this is a point in time view and it may change due to partition reassignment</li>
     *   <li>the key may not exist in the {@link StateStore}; this method provides a way of finding which host it
     *       <em>would</em> exist on</li>
     * </ul>
     *
     * @param storeName   the {@code storeName} to find metadata for
     * @param key         the key to find metadata for
     * @param partitioner the partitioner to be use to locate the host for the key
     * @param <K>         key type
     * @return {@link StreamsMetadata} for the {@code KafkaStreams} instance with the provide {@code storeName} and
     * {@code key} of this application or {@link StreamsMetadata#NOT_AVAILABLE} if Kafka Streams is (re-)initializing
     */
    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final StreamPartitioner<? super K, ?> partitioner) {
        validateIsRunning();
        return streamsMetadataState.getMetadataWithKey(storeName, key, partitioner);
    }

    /**
     * Get a facade wrapping the local {@link StateStore} instances with the provided {@code storeName} if the Store's
     * type is accepted by the provided {@link QueryableStoreType#accepts(StateStore) queryableStoreType}.
     * The returned object can be used to query the {@link StateStore} instances.
     *
     * @param storeName           name of the store to find
     * @param queryableStoreType  accept only stores that are accepted by {@link QueryableStoreType#accepts(StateStore)}
     * @param <T>                 return type
     * @return A facade wrapping the local {@link StateStore} instances
     * @throws InvalidStateStoreException if Kafka Streams is (re-)initializing or a store with {@code storeName} and
     * {@code queryableStoreType} doesnt' exist
     */
    public <T> T store(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        validateIsRunning();
        return queryableStoreProvider.getStore(storeName, queryableStoreType);
    }

    /**
     * Returns runtime information about the local threads of this {@link KafkaStreams} instance.
     *
     * @return the set of {@link ThreadMetadata}.
     */
    public Set<ThreadMetadata> localThreadsMetadata() {
        validateIsRunning();
        final Set<ThreadMetadata> threadMetadata = new HashSet<>();
        for (StreamThread thread : threads) {
            threadMetadata.add(thread.threadMetadata());
        }
        return threadMetadata;
    }
}
