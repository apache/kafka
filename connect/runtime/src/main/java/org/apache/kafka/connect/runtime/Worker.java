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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.LogReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.SinkUtils;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * Worker runs a (dynamic) set of tasks in a set of threads, doing the work of actually moving
 * data to/from Kafka.
 * </p>
 * <p>
 * Since each task has a dedicated thread, this is mainly just a container for them.
 * </p>
 */
public class Worker {

    public static final long CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    protected Herder herder;
    private final ExecutorService executor;
    private final Time time;
    private final String workerId;
    //kafka cluster id
    private final String kafkaClusterId;
    private final Plugins plugins;
    private final ConnectMetrics metrics;
    private final WorkerMetricsGroup workerMetricsGroup;
    private ConnectorStatusMetricsGroup connectorStatusMetricsGroup;
    private final WorkerConfig config;
    private final Converter internalKeyConverter;
    private final Converter internalValueConverter;
    private final OffsetBackingStore offsetBackingStore;

    private final ConcurrentMap<String, WorkerConnector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;
    private final WorkerConfigTransformer workerConfigTransformer;
    private final ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy;

    public Worker(
        String workerId,
        Time time,
        Plugins plugins,
        WorkerConfig config,
        OffsetBackingStore offsetBackingStore,
        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        this(workerId, time, plugins, config, offsetBackingStore, Executors.newCachedThreadPool(), connectorClientConfigOverridePolicy);
    }

    @SuppressWarnings("deprecation")
    Worker(
            String workerId,
            Time time,
            Plugins plugins,
            WorkerConfig config,
            OffsetBackingStore offsetBackingStore,
            ExecutorService executorService,
            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy
    ) {
        this.kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
        this.metrics = new ConnectMetrics(workerId, config, time, kafkaClusterId);
        this.executor = executorService;
        this.workerId = workerId;
        this.time = time;
        this.plugins = plugins;
        this.config = config;
        this.connectorClientConfigOverridePolicy = connectorClientConfigOverridePolicy;
        this.workerMetricsGroup = new WorkerMetricsGroup(metrics);

        // Internal converters are required properties, thus getClass won't return null.
        this.internalKeyConverter = plugins.newConverter(
                config,
                WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG,
                ClassLoaderUsage.PLUGINS
        );
        this.internalValueConverter = plugins.newConverter(
                config,
                WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG,
                ClassLoaderUsage.PLUGINS
        );

        this.offsetBackingStore = offsetBackingStore;
        this.offsetBackingStore.configure(config);

        this.workerConfigTransformer = initConfigTransformer();

    }

    private WorkerConfigTransformer initConfigTransformer() {
        final List<String> providerNames = config.getList(WorkerConfig.CONFIG_PROVIDERS_CONFIG);
        Map<String, ConfigProvider> providerMap = new HashMap<>();
        for (String providerName : providerNames) {
            ConfigProvider configProvider = plugins.newConfigProvider(
                    config,
                    WorkerConfig.CONFIG_PROVIDERS_CONFIG + "." + providerName,
                    ClassLoaderUsage.PLUGINS
            );
            providerMap.put(providerName, configProvider);
        }
        return new WorkerConfigTransformer(this, providerMap);
    }

    public WorkerConfigTransformer configTransformer() {
        return workerConfigTransformer;
    }

    protected Herder herder() {
        return herder;
    }

    /**
     * Start worker.
     */
    public void start() {
        log.info("Worker starting");

        offsetBackingStore.start();
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(config);

        connectorStatusMetricsGroup = new ConnectorStatusMetricsGroup(metrics, tasks, herder);

        log.info("Worker started");
    }

    /**
     * Stop worker.
     */
    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        if (!connectors.isEmpty()) {
            log.warn("Shutting down connectors {} uncleanly; herder should have shut down connectors before the Worker is stopped", connectors.keySet());
            stopAndAwaitConnectors();
        }

        if (!tasks.isEmpty()) {
            log.warn("Shutting down tasks {} uncleanly; herder should have shut down tasks before the Worker is stopped", tasks.keySet());
            stopAndAwaitTasks();
        }

        long timeoutMs = limit - time.milliseconds();
        sourceTaskOffsetCommitter.close(timeoutMs);

        offsetBackingStore.stop();
        metrics.stop();

        log.info("Worker stopped");

        workerMetricsGroup.close();
        connectorStatusMetricsGroup.close();

        workerConfigTransformer.close();
    }

    /**
     * Start a connector managed by this worker.
     *
     * @param connName the connector name.
     * @param connProps the properties of the connector.
     * @param ctx the connector runtime context.
     * @param statusListener a listener for the runtime status transitions of the connector.
     * @param initialState the initial state of the connector.
     * @param onConnectorStateChange invoked when the initial state change of the connector is completed
     */
    public void startConnector(
            String connName,
            Map<String, String> connProps,
            CloseableConnectorContext ctx,
            ConnectorStatus.Listener statusListener,
            TargetState initialState,
            Callback<TargetState> onConnectorStateChange
    ) {
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            if (connectors.containsKey(connName)) {
                onConnectorStateChange.onCompletion(
                        new ConnectException("Connector with name " + connName + " already exists"),
                        null);
                return;
            }

            final WorkerConnector workerConnector;
            ClassLoader savedLoader = plugins.currentThreadLoader();
            try {
                // By the time we arrive here, CONNECTOR_CLASS_CONFIG has been validated already
                // Getting this value from the unparsed map will allow us to instantiate the
                // right config (source or sink)
                final String connClass = connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
                ClassLoader connectorLoader = plugins.delegatingLoader().connectorLoader(connClass);
                savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);

                log.info("Creating connector {} of type {}", connName, connClass);
                final Connector connector = plugins.newConnector(connClass);
                final ConnectorConfig connConfig = ConnectUtils.isSinkConnector(connector)
                        ? new SinkConnectorConfig(plugins, connProps)
                        : new SourceConnectorConfig(plugins, connProps, config.topicCreationEnable());

                final OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(
                        offsetBackingStore, connName, internalKeyConverter, internalValueConverter);
                workerConnector = new WorkerConnector(
                        connName, connector, connConfig, ctx, metrics, statusListener, offsetReader, connectorLoader);
                log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connector.getClass());
                workerConnector.transitionTo(initialState, onConnectorStateChange);
                Plugins.compareAndSwapLoaders(savedLoader);
            } catch (Throwable t) {
                log.error("Failed to start connector {}", connName, t);
                // Can't be put in a finally block because it needs to be swapped before the call on
                // statusListener
                Plugins.compareAndSwapLoaders(savedLoader);
                workerMetricsGroup.recordConnectorStartupFailure();
                statusListener.onFailure(connName, t);
                onConnectorStateChange.onCompletion(t, null);
                return;
            }

            WorkerConnector existing = connectors.putIfAbsent(connName, workerConnector);
            if (existing != null) {
                onConnectorStateChange.onCompletion(
                        new ConnectException("Connector with name " + connName + " already exists"),
                        null);
                // Don't need to do any cleanup of the WorkerConnector instance (such as calling
                // shutdown() on it) here because it hasn't actually started running yet
                return;
            }

            executor.submit(workerConnector);

            log.info("Finished creating connector {}", connName);
            workerMetricsGroup.recordConnectorStartupSuccess();
        }
    }

    /**
     * Return true if the connector associated with this worker is a sink connector.
     *
     * @param connName the connector name.
     * @return true if the connector belongs to the worker and is a sink connector.
     * @throws ConnectException if the worker does not manage a connector with the given name.
     */
    public boolean isSinkConnector(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = Plugins.compareAndSwapLoaders(workerConnector.loader());
            return workerConnector.isSinkConnector();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    /**
     * Get a list of updated task properties for the tasks of this connector.
     *
     * @param connName the connector name.
     * @return a list of updated tasks properties.
     */
    public List<Map<String, String>> connectorTaskConfigs(String connName, ConnectorConfig connConfig) {
        List<Map<String, String>> result = new ArrayList<>();
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            log.trace("Reconfiguring connector tasks for {}", connName);

            WorkerConnector workerConnector = connectors.get(connName);
            if (workerConnector == null)
                throw new ConnectException("Connector " + connName + " not found in this worker.");

            int maxTasks = connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG);
            Map<String, String> connOriginals = connConfig.originalsStrings();

            Connector connector = workerConnector.connector();
            ClassLoader savedLoader = plugins.currentThreadLoader();
            try {
                savedLoader = Plugins.compareAndSwapLoaders(workerConnector.loader());
                String taskClassName = connector.taskClass().getName();
                for (Map<String, String> taskProps : connector.taskConfigs(maxTasks)) {
                    // Ensure we don't modify the connector's copy of the config
                    Map<String, String> taskConfig = new HashMap<>(taskProps);
                    taskConfig.put(TaskConfig.TASK_CLASS_CONFIG, taskClassName);
                    if (connOriginals.containsKey(SinkTask.TOPICS_CONFIG)) {
                        taskConfig.put(SinkTask.TOPICS_CONFIG, connOriginals.get(SinkTask.TOPICS_CONFIG));
                    }
                    if (connOriginals.containsKey(SinkTask.TOPICS_REGEX_CONFIG)) {
                        taskConfig.put(SinkTask.TOPICS_REGEX_CONFIG, connOriginals.get(SinkTask.TOPICS_REGEX_CONFIG));
                    }
                    result.add(taskConfig);
                }
            } finally {
                Plugins.compareAndSwapLoaders(savedLoader);
            }
        }

        return result;
    }

    /**
     * Stop a connector managed by this worker.
     *
     * @param connName the connector name.
     */
    private void stopConnector(String connName) {
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            WorkerConnector workerConnector = connectors.get(connName);
            log.info("Stopping connector {}", connName);

            if (workerConnector == null) {
                log.warn("Ignoring stop request for unowned connector {}", connName);
                return;
            }

            ClassLoader savedLoader = plugins.currentThreadLoader();
            try {
                savedLoader = Plugins.compareAndSwapLoaders(workerConnector.loader());
                workerConnector.shutdown();
            } finally {
                Plugins.compareAndSwapLoaders(savedLoader);
            }
        }
    }

    private void stopConnectors(Collection<String> ids) {
        // Herder is responsible for stopping connectors. This is an internal method to sequentially
        // stop connectors that have not explicitly been stopped.
        for (String connector: ids)
            stopConnector(connector);
    }

    private void awaitStopConnector(String connName, long timeout) {
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            WorkerConnector connector = connectors.remove(connName);
            if (connector == null) {
                log.warn("Ignoring await stop request for non-present connector {}", connName);
                return;
            }

            if (!connector.awaitShutdown(timeout)) {
                log.error("Connector ‘{}’ failed to properly shut down, has become unresponsive, and "
                        + "may be consuming external resources. Correct the configuration for "
                        + "this connector or remove the connector. After fixing the connector, it "
                        + "may be necessary to restart this worker to release any consumed "
                        + "resources.", connName);
                connector.cancel();
            } else {
                log.debug("Graceful stop of connector {} succeeded.", connName);
            }
        }
    }

    private void awaitStopConnectors(Collection<String> ids) {
        long now = time.milliseconds();
        long deadline = now + CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS;
        for (String id : ids) {
            long remaining = Math.max(0, deadline - time.milliseconds());
            awaitStopConnector(id, remaining);
        }
    }

    /**
     * Stop asynchronously all the worker's connectors and await their termination.
     */
    public void stopAndAwaitConnectors() {
        stopAndAwaitConnectors(new ArrayList<>(connectors.keySet()));
    }

    /**
     * Stop asynchronously a collection of connectors that belong to this worker and await their
     * termination.
     *
     * @param ids the collection of connectors to be stopped.
     */
    public void stopAndAwaitConnectors(Collection<String> ids) {
        stopConnectors(ids);
        awaitStopConnectors(ids);
    }

    /**
     * Stop a connector that belongs to this worker and await its termination.
     *
     * @param connName the name of the connector to be stopped.
     */
    public void stopAndAwaitConnector(String connName) {
        stopConnector(connName);
        awaitStopConnectors(Collections.singletonList(connName));
    }

    /**
     * Get the IDs of the connectors currently running in this worker.
     *
     * @return the set of connector IDs.
     */
    public Set<String> connectorNames() {
        return connectors.keySet();
    }

    /**
     * Return true if a connector with the given name is managed by this worker and is currently running.
     *
     * @param connName the connector name.
     * @return true if the connector is running, false if the connector is not running or is not manages by this worker.
     */
    public boolean isRunning(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        return workerConnector != null && workerConnector.isRunning();
    }

    /**
     * Start a task managed by this worker.
     *
     * @param id the task ID.
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param initialState the initial state of the connector.
     * @return true if the task started successfully.
     */
    public boolean startTask(
            ConnectorTaskId id,
            ClusterConfigState configState,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState
    ) {
        final WorkerTask workerTask;
        try (LoggingContext loggingContext = LoggingContext.forTask(id)) {
            log.info("Creating task {}", id);

            if (tasks.containsKey(id))
                throw new ConnectException("Task already exists in this worker: " + id);

            connectorStatusMetricsGroup.recordTaskAdded(id);
            ClassLoader savedLoader = plugins.currentThreadLoader();
            try {
                String connType = connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
                ClassLoader connectorLoader = plugins.delegatingLoader().connectorLoader(connType);
                savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);
                final ConnectorConfig connConfig = new ConnectorConfig(plugins, connProps);
                final TaskConfig taskConfig = new TaskConfig(taskProps);
                final Class<? extends Task> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(Task.class);
                final Task task = plugins.newTask(taskClass);
                log.info("Instantiated task {} with version {} of type {}", id, task.version(), taskClass.getName());

                // By maintaining connector's specific class loader for this thread here, we first
                // search for converters within the connector dependencies.
                // If any of these aren't found, that means the connector didn't configure specific converters,
                // so we should instantiate based upon the worker configuration
                Converter keyConverter = plugins.newConverter(connConfig, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage
                                                                                                                           .CURRENT_CLASSLOADER);
                Converter valueConverter = plugins.newConverter(connConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.CURRENT_CLASSLOADER);
                HeaderConverter headerConverter = plugins.newHeaderConverter(connConfig, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
                                                                             ClassLoaderUsage.CURRENT_CLASSLOADER);
                if (keyConverter == null) {
                    keyConverter = plugins.newConverter(config, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS);
                    log.info("Set up the key converter {} for task {} using the worker config", keyConverter.getClass(), id);
                } else {
                    log.info("Set up the key converter {} for task {} using the connector config", keyConverter.getClass(), id);
                }
                if (valueConverter == null) {
                    valueConverter = plugins.newConverter(config, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS);
                    log.info("Set up the value converter {} for task {} using the worker config", valueConverter.getClass(), id);
                } else {
                    log.info("Set up the value converter {} for task {} using the connector config", valueConverter.getClass(), id);
                }
                if (headerConverter == null) {
                    headerConverter = plugins.newHeaderConverter(config, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, ClassLoaderUsage
                                                                                                                             .PLUGINS);
                    log.info("Set up the header converter {} for task {} using the worker config", headerConverter.getClass(), id);
                } else {
                    log.info("Set up the header converter {} for task {} using the connector config", headerConverter.getClass(), id);
                }

                workerTask = buildWorkerTask(configState, connConfig, id, task, statusListener, initialState, keyConverter, valueConverter,
                                             headerConverter, connectorLoader);
                workerTask.initialize(taskConfig);
                Plugins.compareAndSwapLoaders(savedLoader);
            } catch (Throwable t) {
                log.error("Failed to start task {}", id, t);
                // Can't be put in a finally block because it needs to be swapped before the call on
                // statusListener
                Plugins.compareAndSwapLoaders(savedLoader);
                connectorStatusMetricsGroup.recordTaskRemoved(id);
                workerMetricsGroup.recordTaskFailure();
                statusListener.onFailure(id, t);
                return false;
            }

            WorkerTask existing = tasks.putIfAbsent(id, workerTask);
            if (existing != null)
                throw new ConnectException("Task already exists in this worker: " + id);

            executor.submit(workerTask);
            if (workerTask instanceof WorkerSourceTask) {
                sourceTaskOffsetCommitter.schedule(id, (WorkerSourceTask) workerTask);
            }
            workerMetricsGroup.recordTaskSuccess();
            return true;
        }
    }

    private WorkerTask buildWorkerTask(ClusterConfigState configState,
                                       ConnectorConfig connConfig,
                                       ConnectorTaskId id,
                                       Task task,
                                       TaskStatus.Listener statusListener,
                                       TargetState initialState,
                                       Converter keyConverter,
                                       Converter valueConverter,
                                       HeaderConverter headerConverter,
                                       ClassLoader loader) {
        ErrorHandlingMetrics errorHandlingMetrics = errorHandlingMetrics(id);
        final Class<? extends Connector> connectorClass = plugins.connectorClass(
            connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(connConfig.errorRetryTimeout(),
                connConfig.errorMaxDelayInMillis(), connConfig.errorToleranceType(), Time.SYSTEM);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);

        // Decide which type of worker task we need based on the type of task.
        if (task instanceof SourceTask) {
            SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins,
                    connConfig.originalsStrings(), config.topicCreationEnable());
            retryWithToleranceOperator.reporters(sourceTaskReporters(id, sourceConfig, errorHandlingMetrics));
            TransformationChain<SourceRecord> transformationChain = new TransformationChain<>(sourceConfig.<SourceRecord>transformations(), retryWithToleranceOperator);
            log.info("Initializing: {}", transformationChain);
            CloseableOffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            Map<String, Object> producerProps = producerConfigs(id, "connector-producer-" + id, config, sourceConfig, connectorClass,
                                                                connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
            TopicAdmin admin;
            Map<String, TopicCreationGroup> topicCreationGroups;
            if (config.topicCreationEnable() && sourceConfig.usesTopicCreation()) {
                Map<String, Object> adminProps = adminConfigs(id, "connector-adminclient-" + id, config,
                        sourceConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId);
                admin = new TopicAdmin(adminProps);
                topicCreationGroups = TopicCreationGroup.configuredGroups(sourceConfig);
            } else {
                admin = null;
                topicCreationGroups = null;
            }

            // Note we pass the configState as it performs dynamic transformations under the covers
            return new WorkerSourceTask(id, (SourceTask) task, statusListener, initialState, keyConverter, valueConverter,
                    headerConverter, transformationChain, producer, admin, topicCreationGroups,
                    offsetReader, offsetWriter, config, configState, metrics, loader, time, retryWithToleranceOperator, herder.statusBackingStore());
        } else if (task instanceof SinkTask) {
            TransformationChain<SinkRecord> transformationChain = new TransformationChain<>(connConfig.<SinkRecord>transformations(), retryWithToleranceOperator);
            log.info("Initializing: {}", transformationChain);
            SinkConnectorConfig sinkConfig = new SinkConnectorConfig(plugins, connConfig.originalsStrings());
            retryWithToleranceOperator.reporters(sinkTaskReporters(id, sinkConfig, errorHandlingMetrics, connectorClass));
            WorkerErrantRecordReporter workerErrantRecordReporter = createWorkerErrantRecordReporter(sinkConfig, retryWithToleranceOperator,
                    keyConverter, valueConverter, headerConverter);

            Map<String, Object> consumerProps = consumerConfigs(id, config, connConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

            return new WorkerSinkTask(id, (SinkTask) task, statusListener, initialState, config, configState, metrics, keyConverter,
                                      valueConverter, headerConverter, transformationChain, consumer, loader, time,
                                      retryWithToleranceOperator, workerErrantRecordReporter, herder.statusBackingStore());
        } else {
            log.error("Tasks must be a subclass of either SourceTask or SinkTask and current is {}", task);
            throw new ConnectException("Tasks must be a subclass of either SourceTask or SinkTask");
        }
    }

    static Map<String, Object> producerConfigs(ConnectorTaskId id,
                                               String defaultClientId,
                                               WorkerConfig config,
                                               ConnectorConfig connConfig,
                                               Class<? extends Connector>  connectorClass,
                                               ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                               String clusterId) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings will execute infinite retries on retriable exceptions. They *may* be overridden via configs passed to the worker,
        // but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, defaultClientId);
        // User-specified overrides
        producerProps.putAll(config.originalsWithPrefix("producer."));
        //add client metrics.context properties
        ConnectUtils.addMetricsContextProperties(producerProps, config, clusterId);

        // Connector-specified overrides
        Map<String, Object> producerOverrides =
            connectorClientConfigOverrides(id, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX,
                                           ConnectorType.SOURCE, ConnectorClientConfigRequest.ClientType.PRODUCER,
                                           connectorClientConfigOverridePolicy);
        producerProps.putAll(producerOverrides);

        return producerProps;
    }

    static Map<String, Object> consumerConfigs(ConnectorTaskId id,
                                               WorkerConfig config,
                                               ConnectorConfig connConfig,
                                               Class<? extends Connector> connectorClass,
                                               ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                               String clusterId) {
        // Include any unknown worker configs so consumer configs can be set globally on the worker
        // and through to the task
        Map<String, Object> consumerProps = new HashMap<>();

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, SinkUtils.consumerGroupId(id.connector()));
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "connector-consumer-" + id);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumerProps.putAll(config.originalsWithPrefix("consumer."));
        //add client metrics.context properties
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);
        // Connector-specified overrides
        Map<String, Object> consumerOverrides =
            connectorClientConfigOverrides(id, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX,
                                           ConnectorType.SINK, ConnectorClientConfigRequest.ClientType.CONSUMER,
                                           connectorClientConfigOverridePolicy);
        consumerProps.putAll(consumerOverrides);

        return consumerProps;
    }

    static Map<String, Object> adminConfigs(ConnectorTaskId id,
                                            String defaultClientId,
                                            WorkerConfig config,
                                            ConnectorConfig connConfig,
                                            Class<? extends Connector> connectorClass,
                                            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                            String clusterId) {
        Map<String, Object> adminProps = new HashMap<>();
        // Use the top-level worker configs to retain backwards compatibility with older releases which
        // did not require a prefix for connector admin client configs in the worker configuration file
        // Ignore configs that begin with "admin." since those will be added next (with the prefix stripped)
        // and those that begin with "producer." and "consumer.", since we know they aren't intended for
        // the admin client
        Map<String, Object> nonPrefixedWorkerConfigs = config.originals().entrySet().stream()
            .filter(e -> !e.getKey().startsWith("admin.")
                && !e.getKey().startsWith("producer.")
                && !e.getKey().startsWith("consumer."))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, defaultClientId);
        adminProps.putAll(nonPrefixedWorkerConfigs);

        // Admin client-specific overrides in the worker config
        adminProps.putAll(config.originalsWithPrefix("admin."));

        // Connector-specified overrides
        Map<String, Object> adminOverrides =
            connectorClientConfigOverrides(id, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX,
                                           ConnectorType.SINK, ConnectorClientConfigRequest.ClientType.ADMIN,
                                           connectorClientConfigOverridePolicy);
        adminProps.putAll(adminOverrides);

        //add client metrics.context properties
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);

        return adminProps;
    }

    private static Map<String, Object> connectorClientConfigOverrides(ConnectorTaskId id,
                                                                      ConnectorConfig connConfig,
                                                                      Class<? extends Connector> connectorClass,
                                                                      String clientConfigPrefix,
                                                                      ConnectorType connectorType,
                                                                      ConnectorClientConfigRequest.ClientType clientType,
                                                                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        Map<String, Object> clientOverrides = connConfig.originalsWithPrefix(clientConfigPrefix);
        ConnectorClientConfigRequest connectorClientConfigRequest = new ConnectorClientConfigRequest(
            id.connector(),
            connectorType,
            connectorClass,
            clientOverrides,
            clientType
        );
        List<ConfigValue> configValues = connectorClientConfigOverridePolicy.validate(connectorClientConfigRequest);
        List<ConfigValue> errorConfigs = configValues.stream().
            filter(configValue -> configValue.errorMessages().size() > 0).collect(Collectors.toList());
        // These should be caught when the herder validates the connector configuration, but just in case
        if (errorConfigs.size() > 0) {
            throw new ConnectException("Client Config Overrides not allowed " + errorConfigs);
        }
        return clientOverrides;
    }

    ErrorHandlingMetrics errorHandlingMetrics(ConnectorTaskId id) {
        return new ErrorHandlingMetrics(id, metrics);
    }

    private List<ErrorReporter> sinkTaskReporters(ConnectorTaskId id, SinkConnectorConfig connConfig,
                                                  ErrorHandlingMetrics errorHandlingMetrics,
                                                  Class<? extends Connector> connectorClass) {
        ArrayList<ErrorReporter> reporters = new ArrayList<>();
        LogReporter logReporter = new LogReporter(id, connConfig, errorHandlingMetrics);
        reporters.add(logReporter);

        // check if topic for dead letter queue exists
        String topic = connConfig.dlqTopicName();
        if (topic != null && !topic.isEmpty()) {
            Map<String, Object> producerProps = producerConfigs(id, "connector-dlq-producer-" + id, config, connConfig, connectorClass,
                                                                connectorClientConfigOverridePolicy, kafkaClusterId);
            Map<String, Object> adminProps = adminConfigs(id, "connector-dlq-adminclient-", config, connConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId);
            DeadLetterQueueReporter reporter = DeadLetterQueueReporter.createAndSetup(adminProps, id, connConfig, producerProps, errorHandlingMetrics);

            reporters.add(reporter);
        }

        return reporters;
    }

    private List<ErrorReporter> sourceTaskReporters(ConnectorTaskId id, ConnectorConfig connConfig,
                                                      ErrorHandlingMetrics errorHandlingMetrics) {
        List<ErrorReporter> reporters = new ArrayList<>();
        LogReporter logReporter = new LogReporter(id, connConfig, errorHandlingMetrics);
        reporters.add(logReporter);

        return reporters;
    }

    private WorkerErrantRecordReporter createWorkerErrantRecordReporter(
        SinkConnectorConfig connConfig,
        RetryWithToleranceOperator retryWithToleranceOperator,
        Converter keyConverter,
        Converter valueConverter,
        HeaderConverter headerConverter
    ) {
        // check if errant record reporter topic is configured
        if (connConfig.enableErrantRecordReporter()) {
            return new WorkerErrantRecordReporter(retryWithToleranceOperator, keyConverter, valueConverter, headerConverter);
        }
        return null;
    }

    private void stopTask(ConnectorTaskId taskId) {
        try (LoggingContext loggingContext = LoggingContext.forTask(taskId)) {
            WorkerTask task = tasks.get(taskId);
            if (task == null) {
                log.warn("Ignoring stop request for unowned task {}", taskId);
                return;
            }

            log.info("Stopping task {}", task.id());
            if (task instanceof WorkerSourceTask)
                sourceTaskOffsetCommitter.remove(task.id());

            ClassLoader savedLoader = plugins.currentThreadLoader();
            try {
                savedLoader = Plugins.compareAndSwapLoaders(task.loader());
                task.stop();
            } finally {
                Plugins.compareAndSwapLoaders(savedLoader);
            }
        }
    }

    private void stopTasks(Collection<ConnectorTaskId> ids) {
        // Herder is responsible for stopping tasks. This is an internal method to sequentially
        // stop the tasks that have not explicitly been stopped.
        for (ConnectorTaskId taskId : ids) {
            stopTask(taskId);
        }
    }

    private void awaitStopTask(ConnectorTaskId taskId, long timeout) {
        try (LoggingContext loggingContext = LoggingContext.forTask(taskId)) {
            WorkerTask task = tasks.remove(taskId);
            if (task == null) {
                log.warn("Ignoring await stop request for non-present task {}", taskId);
                return;
            }

            if (!task.awaitStop(timeout)) {
                log.error("Graceful stop of task {} failed.", task.id());
                task.cancel();
            } else {
                log.debug("Graceful stop of task {} succeeded.", task.id());
            }

            try {
                task.removeMetrics();
            } finally {
                connectorStatusMetricsGroup.recordTaskRemoved(taskId);
            }
        }
    }

    private void awaitStopTasks(Collection<ConnectorTaskId> ids) {
        long now = time.milliseconds();
        long deadline = now + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);
        for (ConnectorTaskId id : ids) {
            long remaining = Math.max(0, deadline - time.milliseconds());
            awaitStopTask(id, remaining);
        }
    }

    /**
     * Stop asynchronously all the worker's tasks and await their termination.
     */
    public void stopAndAwaitTasks() {
        stopAndAwaitTasks(new ArrayList<>(tasks.keySet()));
    }

    /**
     * Stop asynchronously a collection of tasks that belong to this worker and await their termination.
     *
     * @param ids the collection of tasks to be stopped.
     */
    public void stopAndAwaitTasks(Collection<ConnectorTaskId> ids) {
        stopTasks(ids);
        awaitStopTasks(ids);
    }

    /**
     * Stop a task that belongs to this worker and await its termination.
     *
     * @param taskId the ID of the task to be stopped.
     */
    public void stopAndAwaitTask(ConnectorTaskId taskId) {
        stopTask(taskId);
        awaitStopTasks(Collections.singletonList(taskId));
    }

    /**
     * Get the IDs of the tasks currently running in this worker.
     */
    public Set<ConnectorTaskId> taskIds() {
        return tasks.keySet();
    }

    public Converter getInternalKeyConverter() {
        return internalKeyConverter;
    }

    public Converter getInternalValueConverter() {
        return internalValueConverter;
    }

    public Plugins getPlugins() {
        return plugins;
    }

    public String workerId() {
        return workerId;
    }

    /**
     * Returns whether this worker is configured to allow source connectors to create the topics
     * that they use with custom configurations, if these topics don't already exist.
     *
     * @return true if topic creation by source connectors is allowed; false otherwise
     */
    public boolean isTopicCreationEnabled() {
        return config.topicCreationEnable();
    }

    /**
     * Get the {@link ConnectMetrics} that uses Kafka Metrics and manages the JMX reporter.
     * @return the Connect-specific metrics; never null
     */
    public ConnectMetrics metrics() {
        return metrics;
    }

    public void setTargetState(String connName, TargetState state, Callback<TargetState> stateChangeCallback) {
        log.info("Setting connector {} state to {}", connName, state);

        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector != null) {
            ClassLoader connectorLoader =
                    plugins.delegatingLoader().connectorLoader(workerConnector.connector());
            executeStateTransition(
                () -> workerConnector.transitionTo(state, stateChangeCallback),
                connectorLoader);
        }

        for (Map.Entry<ConnectorTaskId, WorkerTask> taskEntry : tasks.entrySet()) {
            if (taskEntry.getKey().connector().equals(connName)) {
                WorkerTask workerTask = taskEntry.getValue();
                executeStateTransition(() -> workerTask.transitionTo(state), workerTask.loader);
            }
        }
    }

    private void executeStateTransition(Runnable stateTransition, ClassLoader loader) {
        ClassLoader savedLoader = plugins.currentThreadLoader();
        try {
            savedLoader = Plugins.compareAndSwapLoaders(loader);
            stateTransition.run();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    ConnectorStatusMetricsGroup connectorStatusMetricsGroup() {
        return connectorStatusMetricsGroup;
    }

    WorkerMetricsGroup workerMetricsGroup() {
        return workerMetricsGroup;
    }

    static class ConnectorStatusMetricsGroup {
        private final ConnectMetrics connectMetrics;
        private final ConnectMetricsRegistry registry;
        private final ConcurrentMap<String, MetricGroup> connectorStatusMetrics = new ConcurrentHashMap<>();
        private final Herder herder;
        private final ConcurrentMap<ConnectorTaskId, WorkerTask> tasks;


        protected ConnectorStatusMetricsGroup(
            ConnectMetrics connectMetrics, ConcurrentMap<ConnectorTaskId, WorkerTask> tasks, Herder herder) {
            this.connectMetrics = connectMetrics;
            this.registry = connectMetrics.registry();
            this.tasks = tasks;
            this.herder = herder;
        }

        protected ConnectMetrics.LiteralSupplier<Long> taskCounter(String connName) {
            return now -> tasks.keySet()
                .stream()
                .filter(taskId -> taskId.connector().equals(connName))
                .count();
        }

        protected ConnectMetrics.LiteralSupplier<Long> taskStatusCounter(String connName, TaskStatus.State state) {
            return now -> tasks.values()
                .stream()
                .filter(task ->
                    task.id().connector().equals(connName) &&
                    herder.taskStatus(task.id()).state().equalsIgnoreCase(state.toString()))
                .count();
        }

        protected synchronized void recordTaskAdded(ConnectorTaskId connectorTaskId) {
            if (connectorStatusMetrics.containsKey(connectorTaskId.connector())) {
                return;
            }

            String connName = connectorTaskId.connector();

            MetricGroup metricGroup = connectMetrics.group(registry.workerGroupName(),
                registry.connectorTagName(), connName);

            metricGroup.addValueMetric(registry.connectorTotalTaskCount, taskCounter(connName));
            for (Map.Entry<MetricNameTemplate, TaskStatus.State> statusMetric : registry.connectorStatusMetrics
                .entrySet()) {
                metricGroup.addValueMetric(statusMetric.getKey(), taskStatusCounter(connName,
                    statusMetric.getValue()));
            }
            connectorStatusMetrics.put(connectorTaskId.connector(), metricGroup);
        }

        protected synchronized void recordTaskRemoved(ConnectorTaskId connectorTaskId) {
            // Unregister connector task count metric if we remove the last task of the connector
            if (tasks.keySet().stream().noneMatch(id -> id.connector().equals(connectorTaskId.connector()))) {
                connectorStatusMetrics.get(connectorTaskId.connector()).close();
                connectorStatusMetrics.remove(connectorTaskId.connector());
            }
        }

        protected synchronized void close() {
            for (MetricGroup metricGroup: connectorStatusMetrics.values()) {
                metricGroup.close();
            }
        }

        protected MetricGroup metricGroup(String connectorId) {
            return connectorStatusMetrics.get(connectorId);
        }
    }

    class WorkerMetricsGroup {
        private final MetricGroup metricGroup;
        private final Sensor connectorStartupAttempts;
        private final Sensor connectorStartupSuccesses;
        private final Sensor connectorStartupFailures;
        private final Sensor connectorStartupResults;
        private final Sensor taskStartupAttempts;
        private final Sensor taskStartupSuccesses;
        private final Sensor taskStartupFailures;
        private final Sensor taskStartupResults;

        public WorkerMetricsGroup(ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.workerGroupName());

            metricGroup.addValueMetric(registry.connectorCount, now -> (double) connectors.size());
            metricGroup.addValueMetric(registry.taskCount, now -> (double) tasks.size());

            MetricName connectorFailurePct = metricGroup.metricName(registry.connectorStartupFailurePercentage);
            MetricName connectorSuccessPct = metricGroup.metricName(registry.connectorStartupSuccessPercentage);
            Frequencies connectorStartupResultFrequencies = Frequencies.forBooleanValues(connectorFailurePct, connectorSuccessPct);
            connectorStartupResults = metricGroup.sensor("connector-startup-results");
            connectorStartupResults.add(connectorStartupResultFrequencies);

            connectorStartupAttempts = metricGroup.sensor("connector-startup-attempts");
            connectorStartupAttempts.add(metricGroup.metricName(registry.connectorStartupAttemptsTotal), new CumulativeSum());

            connectorStartupSuccesses = metricGroup.sensor("connector-startup-successes");
            connectorStartupSuccesses.add(metricGroup.metricName(registry.connectorStartupSuccessTotal), new CumulativeSum());

            connectorStartupFailures = metricGroup.sensor("connector-startup-failures");
            connectorStartupFailures.add(metricGroup.metricName(registry.connectorStartupFailureTotal), new CumulativeSum());

            MetricName taskFailurePct = metricGroup.metricName(registry.taskStartupFailurePercentage);
            MetricName taskSuccessPct = metricGroup.metricName(registry.taskStartupSuccessPercentage);
            Frequencies taskStartupResultFrequencies = Frequencies.forBooleanValues(taskFailurePct, taskSuccessPct);
            taskStartupResults = metricGroup.sensor("task-startup-results");
            taskStartupResults.add(taskStartupResultFrequencies);

            taskStartupAttempts = metricGroup.sensor("task-startup-attempts");
            taskStartupAttempts.add(metricGroup.metricName(registry.taskStartupAttemptsTotal), new CumulativeSum());

            taskStartupSuccesses = metricGroup.sensor("task-startup-successes");
            taskStartupSuccesses.add(metricGroup.metricName(registry.taskStartupSuccessTotal), new CumulativeSum());

            taskStartupFailures = metricGroup.sensor("task-startup-failures");
            taskStartupFailures.add(metricGroup.metricName(registry.taskStartupFailureTotal), new CumulativeSum());
        }

        void close() {
            metricGroup.close();
        }

        void recordConnectorStartupFailure() {
            connectorStartupAttempts.record(1.0);
            connectorStartupFailures.record(1.0);
            connectorStartupResults.record(0.0);
        }

        void recordConnectorStartupSuccess() {
            connectorStartupAttempts.record(1.0);
            connectorStartupSuccesses.record(1.0);
            connectorStartupResults.record(1.0);
        }

        void recordTaskFailure() {
            taskStartupAttempts.record(1.0);
            taskStartupFailures.record(1.0);
            taskStartupResults.record(0.0);
        }

        void recordTaskSuccess() {
            taskStartupAttempts.record(1.0);
            taskStartupSuccesses.record(1.0);
            taskStartupResults.record(1.0);
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}
