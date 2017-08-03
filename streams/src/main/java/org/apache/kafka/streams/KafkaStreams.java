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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
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
import org.slf4j.LoggerFactory;

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
import static org.apache.kafka.streams.KafkaStreams.State.CREATED;
import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.NOT_RUNNING;
import static org.apache.kafka.streams.KafkaStreams.State.PENDING_SHUTDOWN;
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

    private static final Logger log = LoggerFactory.getLogger(KafkaStreams.class);
    private static final String JMX_PREFIX = "kafka.streams";
    private static final int DEFAULT_CLOSE_TIMEOUT = 0;
    private GlobalStreamThread globalStreamThread;

    private final ScheduledExecutorService stateDirCleaner;
    private final StreamThread[] threads;
    private final Metrics metrics;
    private final QueryableStoreProvider queryableStoreProvider;

    // processId is expected to be unique across JVMs and to be used
    // in userData of the subscription request to allow assignor be aware
    // of the co-location of stream thread's consumers. It is for internal
    // usage only and should not be exposed to users at all.
    private final UUID processId;
    private final String logPrefix;
    private final StreamsMetadataState streamsMetadataState;
    private final StreamsConfig config;
    private final StateDirectory stateDirectory;

    // container states
    /**
     * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
     * An instance must only be in one state at a time.
     * Note this instance will be in "Rebalancing" state if any of its threads is rebalancing
     * The expected state transition with the following defined states is:
     *
     * <pre>
     *                 +--------------+
     *         +<----- | Created      |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +-----+--------+ <-+
     *         +<----- | Rebalancing  | --+
     *         |       +--------------+ <----+
     *         |                             |
     *         |                             |
     *         |       +--------------+      |
     *         +-----> | Running      | ---->+
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +-----+--------+
     *         +-----> | Pending      |
     *         |       | Shutdown     |
     *         |       +-----+--------+
     *         |             |
     *         |             v
     *         |       +-----+--------+
     *         +-----> | Not Running  |
     *         |       +--------------+
     *         |
     *         |       +--------------+
     *         +-----> | Error        |
     *                 +--------------+
     *
     *
     * </pre>
     * Note the following:
     * - Any state can go to PENDING_SHUTDOWN (during clean shutdown) or NOT_RUNNING (e.g., during an exception).
     * - It is theoretically possible for a thread to always be in the PARTITION_REVOKED state
     * (see {@code StreamThread} state diagram) and hence it is possible that this instance is always
     * on a REBALANCING state.
     * - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
     * the instance will be in the ERROR state. The user will need to close it.
     */
    public enum State {
        CREATED(1, 2, 3, 5), REBALANCING(1, 2, 3, 4, 5), RUNNING(1, 3, 4, 5), PENDING_SHUTDOWN(4), NOT_RUNNING, ERROR;

        private final Set<Integer> validTransitions = new HashSet<>();

        State(final Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return equals(RUNNING) || equals(REBALANCING);
        }
        public boolean isCreatedOrRunning() {
            return isRunning() || equals(CREATED);
        }
        public boolean isValidTransition(final State newState) {
            return validTransitions.contains(newState.ordinal());
        }
    }

    private final Object stateLock = new Object();
    private volatile State state = State.CREATED;
    private KafkaStreams.StateListener stateListener = null;


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
     */
    public void setStateListener(final KafkaStreams.StateListener listener) {
        synchronized (stateLock) {
            if (state == CREATED) {
                stateListener = listener;
            } else {
                throw new IllegalStateException("Can only set StateListener in CREATED state.");
            }
        }
    }

    /**
     * Sets the state
     * @param newState New state
     */
    private void setState(final State newState) {

        synchronized (stateLock) {

            // there are cases when we shouldn't check if a transition is valid, e.g.,
            // when, for testing, Kafka Streams is closed multiple times. We could either
            // check here and immediately return for those cases, or add them to the transition
            // diagram (but then the diagram would be confusing and have transitions like
            // NOT_RUNNING->NOT_RUNNING).
            if (newState != NOT_RUNNING && (state == State.NOT_RUNNING || state == PENDING_SHUTDOWN)) {
                return;
            }

            final State oldState = state;
            if (!state.isValidTransition(newState)) {
                log.warn("{} Unexpected state transition from {} to {}.", logPrefix, oldState, newState);
                throw new StreamsException(logPrefix + " Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("{} State transition from {} to {}.", logPrefix, oldState, newState);
            }
            state = newState;
            if (stateListener != null) {
                stateListener.onChange(state, oldState);
            }
        }
    }

    /**
     * Return the current {@link State} of this {@code KafkaStreams} instance.
     *
     * @return the currnt state of this Kafka Streams instance
     */
    public State state() {
        synchronized (stateLock) {
            return state;
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
        private void checkAllThreadsDeadAndSetError() {

            synchronized (stateLock) {
                // if we are pending a shutdown, it's ok for all threads to die, in fact
                // it is expected. Otherwise, it is an error
                if (state != PENDING_SHUTDOWN) {
                    // one thread died, check if we have enough threads running
                    for (final StreamThread.State state : threadState.values()) {
                        if (state != StreamThread.State.DEAD) {
                            return;
                        }
                    }
                    log.warn("{} All stream threads have died. The Kafka Streams instance will be in an error state and should be closed.",
                            logPrefix);
                    setState(ERROR);
                }
            }
        }

        /**
         * If all global thread is DEAD
         */
        private void maybeSetErrorSinceGlobalStreamThreadIsDead() {

            synchronized (stateLock) {
                // if we are pending a shutdown, it's ok for all threads to die, in fact
                // it is expected. Otherwise, it is an error
                if (state != PENDING_SHUTDOWN) {
                    log.warn("{} Global Stream thread has died. The Kafka Streams instance will be in an error state and should be closed.",
                            logPrefix);
                    setState(ERROR);
                }
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

                if (newState == StreamThread.State.PARTITIONS_REVOKED ||
                        newState == StreamThread.State.ASSIGNING_PARTITIONS) {
                    setState(State.REBALANCING);
                } else if (newState == StreamThread.State.RUNNING && state() != State.RUNNING) {
                    maybeSetRunning();
                } else if (newState == StreamThread.State.DEAD) {
                    checkAllThreadsDeadAndSetError();
                }
            } else if (thread instanceof GlobalStreamThread) {
                // global stream thread has different invariants
                GlobalStreamThread.State newState = (GlobalStreamThread.State) abstractNewState;
                globalThreadState = newState;

                // special case when global thread is dead
                if (newState == GlobalStreamThread.State.DEAD) {
                    maybeSetErrorSinceGlobalStreamThreadIsDead();
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
     */
    public KafkaStreams(final Topology topology,
                        final StreamsConfig config,
                        final KafkaClientSupplier clientSupplier) {
        this(topology.internalTopologyBuilder, config, clientSupplier);
    }

    private KafkaStreams(final InternalTopologyBuilder internalTopologyBuilder,
                         final StreamsConfig config,
                         final KafkaClientSupplier clientSupplier) {
        // create the metrics
        final Time time = Time.SYSTEM;

        processId = UUID.randomUUID();

        this.config = config;

        // The application ID is a required config and hence should always have value
        final String applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);

        internalTopologyBuilder.setApplicationId(applicationId);

        String clientId = config.getString(StreamsConfig.CLIENT_ID_CONFIG);
        if (clientId.length() <= 0)
            clientId = applicationId + "-" + processId;

        this.logPrefix = String.format("stream-client [%s]", clientId);

        final List<MetricsReporter> reporters = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));

        final MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                TimeUnit.MILLISECONDS);

        metrics = new Metrics(metricConfig, reporters, time);

        threads = new StreamThread[config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG)];
        final Map<Long, StreamThread.State> threadState = new HashMap<>(threads.length);
        GlobalStreamThread.State globalThreadState = null;

        final ArrayList<StateStoreProvider> storeProviders = new ArrayList<>();
        streamsMetadataState = new StreamsMetadataState(internalTopologyBuilder, parseHostInfo(config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG)));

        final ProcessorTopology globalTaskTopology = internalTopologyBuilder.buildGlobalStateTopology();

        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) < 0) {
            log.warn("{} Negative cache size passed in. Reverting to cache size of 0 bytes.", logPrefix);
        }

        final long cacheSizeBytes = Math.max(0, config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) /
            (config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) + (globalTaskTopology == null ? 0 : 1)));

        stateDirectory = new StateDirectory(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG), time);
        if (globalTaskTopology != null) {
            final String globalThreadId = clientId + "-GlobalStreamThread";
            globalStreamThread = new GlobalStreamThread(globalTaskTopology,
                                                        config,
                                                        clientSupplier.getRestoreConsumer(config.getRestoreConsumerConfigs(clientId + "-global")),
                                                        stateDirectory,
                                                        metrics,
                                                        time,
                                                        globalThreadId);
            globalThreadState = globalStreamThread.state();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new StreamThread(internalTopologyBuilder,
                                          config,
                                          clientSupplier,
                                          applicationId,
                                          clientId,
                                          processId,
                                          metrics,
                                          time,
                                          streamsMetadataState,
                                          cacheSizeBytes,
                                          stateDirectory);
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
        final String cleanupThreadName = clientId + "-CleanupThread";
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
            log.warn("{} Could not close StreamKafkaClient.", logPrefix, e);
        }

    }

    private void validateStartOnce() {
        synchronized (stateLock) {
            if (state == State.CREATED) {
                state = State.RUNNING;
            } else {
                throw new IllegalStateException("Cannot start again.");
            }
        }
    }

    /**
     * Start the {@code KafkaStreams} instance by starting all its threads.
     * <p>
     * Note, for brokers with version {@code 0.9.x} or lower, the broker version cannot be checked.
     * There will be no error and the client will hang and retry to verify the broker version until it
     * {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.

     * @throws IllegalStateException if process was already started
     * @throws StreamsException if the Kafka brokers have version 0.10.0.x
     */
    public synchronized void start() throws IllegalStateException, StreamsException {
        log.debug("{} Starting Kafka Stream process.", logPrefix);
        validateStartOnce();
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
                synchronized (stateLock) {
                    if (state == State.RUNNING) {
                        stateDirectory.cleanRemovedTasks(cleanupDelay);
                    }
                }
            }
        }, cleanupDelay, cleanupDelay, TimeUnit.MILLISECONDS);

        log.info("{} Started Kafka Stream process", logPrefix);
    }

    /**
     * Shutdown this {@code KafkaStreams} instance by signaling all the threads to stop, and then wait for them to join.
     * This will block until all threads have stopped.
     */
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT, TimeUnit.SECONDS);
    }


    private boolean checkFirstTimeClosing() {
        synchronized (stateLock) {
            if (state.isCreatedOrRunning() || state == ERROR) {
                state = PENDING_SHUTDOWN;
                return true;
            }
            return false;
        }
    }

    private void closeGlobalStreamThread() {
        if (globalStreamThread != null) {
            globalStreamThread.close();
            if (!globalStreamThread.stillRunning()) {
                try {
                    globalStreamThread.join();
                } catch (final InterruptedException e) {
                    Thread.interrupted();
                }
            }
            globalStreamThread = null;
        }
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
     */
    public synchronized boolean close(final long timeout, final TimeUnit timeUnit) {
        log.debug("{} Stopping Kafka Stream process.", logPrefix);

        // only clean up once
        if (!checkFirstTimeClosing()) {
            return true;
        }

        stateDirCleaner.shutdownNow();
        // save the current thread so that if it is a stream thread
        // we don't attempt to join it and cause a deadlock
        final Thread shutdown = new Thread(new Runnable() {
            @Override
            public void run() {
                // signal the threads to stop and wait
                for (final StreamThread thread : threads) {
                    // avoid deadlocks by stopping any further state reports
                    // from the thread since we're shutting down
                    thread.setStateListener(null);
                    thread.close();
                }
                closeGlobalStreamThread();
                for (final StreamThread thread : threads) {
                    try {
                        if (!thread.stillRunning()) {
                            thread.join();
                        }
                    } catch (final InterruptedException ex) {
                        Thread.interrupted();
                    }
                }

                metrics.close();
                log.info("{} Stopped Kafka Streams process.", logPrefix);
            }
        }, "kafka-streams-close-thread");
        shutdown.setDaemon(true);
        shutdown.start();
        try {
            shutdown.join(TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
        } catch (final InterruptedException e) {
            Thread.interrupted();
        }
        setState(State.NOT_RUNNING);
        return !shutdown.isAlive();
    }

    /**
     * Produce a string representation containing useful information about this {@code KafkaStream} instance such as
     * thread IDs, task IDs, and a representation of the topology DAG including {@link StateStore}s (cf.
     * {@link Topology} and {@link StreamsBuilder}).
     *
     * @return A string representation of the Kafka Streams instance.
     */
    @Override
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
     */
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

    private boolean isRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
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
     * @throws IllegalStateException if the instance is currently running
     */
    public void cleanUp() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot clean up while running.");
        }
        stateDirectory.cleanRemovedTasks(0);
    }

    /**
     * Set the handler invoked when a {@link StreamsConfig#NUM_STREAM_THREADS_CONFIG internal thread} abruptly
     * terminates due to an uncaught exception.
     *
     * @param eh the uncaught exception handler for all internal threads; {@code null} deletes the current handler
     */
    public void setUncaughtExceptionHandler(final Thread.UncaughtExceptionHandler eh) {
        synchronized (stateLock) {
            if (state == CREATED) {
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
    }

    /**
     * Set the listener which is triggered whenever a {@link StateStore} is being restored in order to resume
     * processing.
     *
     * @param globalStateRestoreListener The listener triggered when {@link StateStore} is being restored.
     */
    public void setGlobalStateRestoreListener(final StateRestoreListener globalStateRestoreListener) {
        synchronized (stateLock) {
            if (state == State.CREATED) {
                for (StreamThread thread : threads) {
                    thread.setGlobalStateRestoreListener(globalStateRestoreListener);
                }
            } else {
                throw new IllegalStateException("Can only set the GlobalRestoreListener in the CREATED state");
            }
        }
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
     * {@link ProducerConfig#PARTITIONER_CLASS_CONFIG configured} via {@link StreamsConfig},
     * {@link KStream#through(StreamPartitioner, String)}, or {@link KTable#through(StreamPartitioner, String, String)},
     * or if the original {@link KTable}'s input {@link StreamsBuilder#table(String, String) topic} is partitioned
     * differently, please use {@link #metadataForKey(String, Object, StreamPartitioner)}.
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

    private void validateIsRunning() {
        if (!isRunning()) {
            throw new IllegalStateException("KafkaStreams is not running. State is " + state + ".");
        }
    }
}
