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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
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
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

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
    public static final long EXECUTOR_SHUTDOWN_TERMINATION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);

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
    private final OffsetBackingStore globalOffsetBackingStore;

    private final ConcurrentMap<String, WorkerConnector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
    private Optional<SourceTaskOffsetCommitter> sourceTaskOffsetCommitter;
    private final WorkerConfigTransformer workerConfigTransformer;
    private final ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy;

    public Worker(
        String workerId,
        Time time,
        Plugins plugins,
        WorkerConfig config,
        OffsetBackingStore globalOffsetBackingStore,
        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        this(workerId, time, plugins, config, globalOffsetBackingStore, Executors.newCachedThreadPool(), connectorClientConfigOverridePolicy);
    }

    Worker(
            String workerId,
            Time time,
            Plugins plugins,
            WorkerConfig config,
            OffsetBackingStore globalOffsetBackingStore,
            ExecutorService executorService,
            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy
    ) {
        this.kafkaClusterId = config.kafkaClusterId();
        this.metrics = new ConnectMetrics(workerId, config, time, kafkaClusterId);
        this.executor = executorService;
        this.workerId = workerId;
        this.time = time;
        this.plugins = plugins;
        this.config = config;
        this.connectorClientConfigOverridePolicy = connectorClientConfigOverridePolicy;
        this.workerMetricsGroup = new WorkerMetricsGroup(this.connectors, this.tasks, metrics);

        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        this.internalKeyConverter = plugins.newInternalConverter(true, JsonConverter.class.getName(), internalConverterConfig);
        this.internalValueConverter = plugins.newInternalConverter(false, JsonConverter.class.getName(), internalConverterConfig);

        this.globalOffsetBackingStore = globalOffsetBackingStore;

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

        globalOffsetBackingStore.start();

        sourceTaskOffsetCommitter = config.exactlyOnceSourceEnabled()
                ? Optional.empty()
                : Optional.of(new SourceTaskOffsetCommitter(config));

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
        sourceTaskOffsetCommitter.ifPresent(committer -> committer.close(timeoutMs));

        globalOffsetBackingStore.stop();
        metrics.stop();

        log.info("Worker stopped");

        workerMetricsGroup.close();
        connectorStatusMetricsGroup.close();

        workerConfigTransformer.close();
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow(); //cancel current executing threads
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TERMINATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                    log.error("Executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow(); // (Re-)Cancel if current thread also interrupted
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
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
        final ConnectorStatus.Listener connectorStatusListener = workerMetricsGroup.wrapStatusListener(statusListener);
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            if (connectors.containsKey(connName)) {
                onConnectorStateChange.onCompletion(
                        new ConnectException("Connector with name " + connName + " already exists"),
                        null);
                return;
            }

            final WorkerConnector workerConnector;
            final String connClass = connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            ClassLoader connectorLoader = plugins.connectorLoader(connClass);
            try (LoaderSwap loaderSwap = plugins.withClassLoader(connectorLoader)) {
                log.info("Creating connector {} of type {}", connName, connClass);
                final Connector connector = plugins.newConnector(connClass);
                final ConnectorConfig connConfig;
                final CloseableOffsetStorageReader offsetReader;
                final ConnectorOffsetBackingStore offsetStore;
                if (ConnectUtils.isSinkConnector(connector)) {
                    connConfig = new SinkConnectorConfig(plugins, connProps);
                    offsetReader = null;
                    offsetStore = null;
                } else {
                    SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connProps, config.topicCreationEnable());
                    connConfig = sourceConfig;

                    // Set up the offset backing store for this connector instance
                    offsetStore = config.exactlyOnceSourceEnabled()
                            ? offsetStoreForExactlyOnceSourceConnector(sourceConfig, connName, connector)
                            : offsetStoreForRegularSourceConnector(sourceConfig, connName, connector);
                    offsetStore.configure(config);
                    offsetReader = new OffsetStorageReaderImpl(offsetStore, connName, internalKeyConverter, internalValueConverter);
                }
                workerConnector = new WorkerConnector(
                        connName, connector, connConfig, ctx, metrics, connectorStatusListener, offsetReader, offsetStore, connectorLoader);
                log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connector.getClass());
                workerConnector.transitionTo(initialState, onConnectorStateChange);
            } catch (Throwable t) {
                log.error("Failed to start connector {}", connName, t);
                connectorStatusListener.onFailure(connName, t);
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

            executor.submit(plugins.withClassLoader(connectorLoader, workerConnector));

            log.info("Finished creating connector {}", connName);
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

        try (LoaderSwap loaderSwap = plugins.withClassLoader(workerConnector.loader())) {
            return workerConnector.isSinkConnector();
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
            try (LoaderSwap loaderSwap = plugins.withClassLoader(workerConnector.loader())) {
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

            try (LoaderSwap loaderSwap = plugins.withClassLoader(workerConnector.loader())) {
                workerConnector.shutdown();
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
                log.error("Connector '{}' failed to properly shut down, has become unresponsive, and "
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
     * Start a sink task managed by this worker.
     *
     * @param id the task ID.
     * @param configState the most recent {@link ClusterConfigState} known to the worker
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param initialState the initial state of the connector.
     * @return true if the task started successfully.
     */
    public boolean startSinkTask(
            ConnectorTaskId id,
            ClusterConfigState configState,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState
    ) {
        return startTask(id, connProps, taskProps, statusListener,
                new SinkTaskBuilder(id, configState, statusListener, initialState));
    }

    /**
     * Start a source task managed by this worker using older behavior that does not provide exactly-once support.
     *
     * @param id the task ID.
     * @param configState the most recent {@link ClusterConfigState} known to the worker
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param initialState the initial state of the connector.
     * @return true if the task started successfully.
     */
    public boolean startSourceTask(
            ConnectorTaskId id,
            ClusterConfigState configState,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState
    ) {
        return startTask(id, connProps, taskProps, statusListener,
                new SourceTaskBuilder(id, configState, statusListener, initialState));
    }

    /**
     * Start a source task with exactly-once support managed by this worker.
     *
     * @param id the task ID.
     * @param configState the most recent {@link ClusterConfigState} known to the worker
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param initialState the initial state of the connector.
     * @param preProducerCheck a preflight check that should be performed before the task initializes its transactional producer.
     * @param postProducerCheck a preflight check that should be performed after the task initializes its transactional producer,
     *                          but before producing any source records or offsets.
     * @return true if the task started successfully.
     */
    public boolean startExactlyOnceSourceTask(
            ConnectorTaskId id,
            ClusterConfigState configState,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState,
            Runnable preProducerCheck,
            Runnable postProducerCheck
    ) {
        return startTask(id, connProps, taskProps, statusListener,
                new ExactlyOnceSourceTaskBuilder(id, configState, statusListener, initialState, preProducerCheck, postProducerCheck));
    }

    /**
     * Start a task managed by this worker.
     *
     * @param id the task ID.
     * @param connProps the connector properties.
     * @param taskProps the tasks properties.
     * @param statusListener a listener for the runtime status transitions of the task.
     * @param taskBuilder the {@link TaskBuilder} used to create the {@link WorkerTask} that manages the lifecycle of the task.
     * @return true if the task started successfully.
     */
    private boolean startTask(
            ConnectorTaskId id,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TaskBuilder taskBuilder
    ) {
        final WorkerTask workerTask;
        final TaskStatus.Listener taskStatusListener = workerMetricsGroup.wrapStatusListener(statusListener);
        try (LoggingContext loggingContext = LoggingContext.forTask(id)) {
            log.info("Creating task {}", id);

            if (tasks.containsKey(id))
                throw new ConnectException("Task already exists in this worker: " + id);

            connectorStatusMetricsGroup.recordTaskAdded(id);
            String connType = connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            ClassLoader connectorLoader = plugins.connectorLoader(connType);

            try (LoaderSwap loaderSwap = plugins.withClassLoader(connectorLoader)) {
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

                workerTask = taskBuilder
                        .withTask(task)
                        .withConnectorConfig(connConfig)
                        .withKeyConverter(keyConverter)
                        .withValueConverter(valueConverter)
                        .withHeaderConverter(headerConverter)
                        .withClassloader(connectorLoader)
                        .build();

                workerTask.initialize(taskConfig);
            } catch (Throwable t) {
                log.error("Failed to start task {}", id, t);
                connectorStatusMetricsGroup.recordTaskRemoved(id);
                taskStatusListener.onFailure(id, t);
                return false;
            }

            WorkerTask existing = tasks.putIfAbsent(id, workerTask);
            if (existing != null)
                throw new ConnectException("Task already exists in this worker: " + id);

            executor.submit(plugins.withClassLoader(connectorLoader, workerTask));
            if (workerTask instanceof WorkerSourceTask) {
                sourceTaskOffsetCommitter.ifPresent(committer -> committer.schedule(id, (WorkerSourceTask) workerTask));
            }
            return true;
        }
    }

    /**
     * Using the admin principal for this connector, perform a round of zombie fencing that disables transactional producers
     * for the specified number of source tasks from sending any more records.
     * @param connName the name of the connector
     * @param numTasks the number of tasks to fence out
     * @param connProps the configuration of the connector; may not be null
     * @return a {@link KafkaFuture} that will complete when the producers have all been fenced out, or the attempt has failed
     */
    public KafkaFuture<Void> fenceZombies(String connName, int numTasks, Map<String, String> connProps) {
        return fenceZombies(connName, numTasks, connProps, Admin::create);
    }

    // Allows us to mock out the Admin client for testing
    KafkaFuture<Void> fenceZombies(String connName, int numTasks, Map<String, String> connProps, Function<Map<String, Object>, Admin> adminFactory) {
        log.debug("Fencing out {} task producers for source connector {}", numTasks, connName);
        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            String connType = connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            ClassLoader connectorLoader = plugins.connectorLoader(connType);
            try (LoaderSwap loaderSwap = plugins.withClassLoader(connectorLoader)) {
                final SourceConnectorConfig connConfig = new SourceConnectorConfig(plugins, connProps, config.topicCreationEnable());
                final Class<? extends Connector> connClass = plugins.connectorClass(
                        connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG));

                Map<String, Object> adminConfig = adminConfigs(
                        connName,
                        "connector-worker-adminclient-" + connName,
                        config,
                        connConfig,
                        connClass,
                        connectorClientConfigOverridePolicy,
                        kafkaClusterId,
                        ConnectorType.SOURCE);
                final Admin admin = adminFactory.apply(adminConfig);

                try {
                    Collection<String> transactionalIds = IntStream.range(0, numTasks)
                            .mapToObj(i -> new ConnectorTaskId(connName, i))
                            .map(this::taskTransactionalId)
                            .collect(Collectors.toList());
                    FenceProducersOptions fencingOptions = new FenceProducersOptions()
                            .timeoutMs((int) ConnectResource.DEFAULT_REST_REQUEST_TIMEOUT_MS);
                    return admin.fenceProducers(transactionalIds, fencingOptions).all().whenComplete((ignored, error) -> {
                        if (error != null)
                            log.debug("Finished fencing out {} task producers for source connector {}", numTasks, connName);
                        Utils.closeQuietly(admin, "Zombie fencing admin for connector " + connName);
                    });
                } catch (Exception e) {
                    Utils.closeQuietly(admin, "Zombie fencing admin for connector " + connName);
                    throw e;
                }
            }
        }
    }

    static Map<String, Object> exactlyOnceSourceTaskProducerConfigs(ConnectorTaskId id,
                                                              WorkerConfig config,
                                                              ConnectorConfig connConfig,
                                                              Class<? extends Connector>  connectorClass,
                                                              ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                                              String clusterId) {
        Map<String, Object> result = baseProducerConfigs(id.connector(), "connector-producer-" + id, config, connConfig, connectorClass, connectorClientConfigOverridePolicy, clusterId);
        // The base producer properties forcibly disable idempotence; remove it from those properties
        // if not explicitly requested by the user
        boolean connectorProducerIdempotenceConfigured = connConfig.originals().containsKey(
                ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
        );
        if (!connectorProducerIdempotenceConfigured) {
            boolean workerProducerIdempotenceConfigured = config.originals().containsKey(
                    "producer." + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
            );
            if (!workerProducerIdempotenceConfigured) {
                result.remove(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
            }
        }
        ConnectUtils.ensureProperty(
                result, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
                "for connectors when exactly-once source support is enabled",
                false
        );
        String transactionalId = taskTransactionalId(config.groupId(), id.connector(), id.task());
        ConnectUtils.ensureProperty(
                result, ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                "for connectors when exactly-once source support is enabled",
                true
        );
        return result;
    }

    static Map<String, Object> baseProducerConfigs(String connName,
                                               String defaultClientId,
                                               WorkerConfig config,
                                               ConnectorConfig connConfig,
                                               Class<? extends Connector>  connectorClass,
                                               ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                               String clusterId) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings will execute infinite retries on retriable exceptions. They *may* be overridden via configs passed to the worker,
        // but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        // By default, Connect disables idempotent behavior for all producers, even though idempotence became
        // default for Kafka producers. This is to ensure Connect continues to work with many Kafka broker versions, including older brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
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
            connectorClientConfigOverrides(connName, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX,
                                           ConnectorType.SOURCE, ConnectorClientConfigRequest.ClientType.PRODUCER,
                                           connectorClientConfigOverridePolicy);
        producerProps.putAll(producerOverrides);

        return producerProps;
    }

    static Map<String, Object> exactlyOnceSourceOffsetsConsumerConfigs(String connName,
                                                                       String defaultClientId,
                                                                       WorkerConfig config,
                                                                       ConnectorConfig connConfig,
                                                                       Class<? extends Connector> connectorClass,
                                                                       ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                                                       String clusterId) {
        Map<String, Object> result = baseConsumerConfigs(
                connName, defaultClientId, config, connConfig, connectorClass,
                connectorClientConfigOverridePolicy, clusterId, ConnectorType.SOURCE);
        ConnectUtils.ensureProperty(
                result, ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT),
                "for source connectors' offset consumers when exactly-once source support is enabled",
                false
        );
        return result;
    }

    static Map<String, Object> regularSourceOffsetsConsumerConfigs(String connName,
                                                                   String defaultClientId,
                                                                   WorkerConfig config,
                                                                   ConnectorConfig connConfig,
                                                                   Class<? extends Connector> connectorClass,
                                                                   ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                                                   String clusterId) {
        Map<String, Object> result = baseConsumerConfigs(
                connName, defaultClientId, config, connConfig, connectorClass,
                connectorClientConfigOverridePolicy, clusterId, ConnectorType.SOURCE);
        // Users can disable this if they want to; it won't affect delivery guarantees since the task isn't exactly-once anyways
        result.putIfAbsent(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return result;
    }

    static Map<String, Object> baseConsumerConfigs(String connName,
                                               String defaultClientId,
                                               WorkerConfig config,
                                               ConnectorConfig connConfig,
                                               Class<? extends Connector> connectorClass,
                                               ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                               String clusterId,
                                               ConnectorType connectorType) {
        // Include any unknown worker configs so consumer configs can be set globally on the worker
        // and through to the task
        Map<String, Object> consumerProps = new HashMap<>();

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, SinkUtils.consumerGroupId(connName));
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, defaultClientId);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumerProps.putAll(config.originalsWithPrefix("consumer."));
        //add client metrics.context properties
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);
        // Connector-specified overrides
        Map<String, Object> consumerOverrides =
            connectorClientConfigOverrides(connName, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX,
                                           connectorType, ConnectorClientConfigRequest.ClientType.CONSUMER,
                                           connectorClientConfigOverridePolicy);
        consumerProps.putAll(consumerOverrides);

        return consumerProps;
    }

    static Map<String, Object> adminConfigs(String connName,
                                            String defaultClientId,
                                            WorkerConfig config,
                                            ConnectorConfig connConfig,
                                            Class<? extends Connector> connectorClass,
                                            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                            String clusterId,
                                            ConnectorType connectorType) {
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
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, defaultClientId);
        adminProps.putAll(nonPrefixedWorkerConfigs);

        // Admin client-specific overrides in the worker config
        adminProps.putAll(config.originalsWithPrefix("admin."));

        // Connector-specified overrides
        Map<String, Object> adminOverrides =
                connectorClientConfigOverrides(connName, connConfig, connectorClass, ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX,
                        connectorType, ConnectorClientConfigRequest.ClientType.ADMIN,
                        connectorClientConfigOverridePolicy);
        adminProps.putAll(adminOverrides);

        //add client metrics.context properties
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);

        return adminProps;
    }

    private static Map<String, Object> connectorClientConfigOverrides(String connName,
                                                                      ConnectorConfig connConfig,
                                                                      Class<? extends Connector> connectorClass,
                                                                      String clientConfigPrefix,
                                                                      ConnectorType connectorType,
                                                                      ConnectorClientConfigRequest.ClientType clientType,
                                                                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        Map<String, Object> clientOverrides = connConfig.originalsWithPrefix(clientConfigPrefix);
        ConnectorClientConfigRequest connectorClientConfigRequest = new ConnectorClientConfigRequest(
            connName,
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

    private String taskTransactionalId(ConnectorTaskId id) {
        return taskTransactionalId(config.groupId(), id.connector(), id.task());
    }

    /**
     * @return the {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG transactional ID} to use for a task that writes
     * records and/or offsets in a transaction. Not to be confused with {@link DistributedConfig#transactionalProducerId()},
     * which is not used by tasks at all, but instead, by the worker itself.
     */
    public static String taskTransactionalId(String groupId, String connector, int taskId) {
        return String.format("%s-%s-%d", groupId, connector, taskId);
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
            Map<String, Object> producerProps = baseProducerConfigs(id.connector(), "connector-dlq-producer-" + id, config, connConfig, connectorClass,
                                                                connectorClientConfigOverridePolicy, kafkaClusterId);
            Map<String, Object> adminProps = adminConfigs(id.connector(), "connector-dlq-adminclient-", config, connConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SINK);
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
                sourceTaskOffsetCommitter.ifPresent(committer -> committer.remove(task.id()));

            try (LoaderSwap loaderSwap = plugins.withClassLoader(task.loader())) {
                task.stop();
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
            try (LoaderSwap loaderSwap = plugins.withClassLoader(workerConnector.loader())) {
                workerConnector.transitionTo(state, stateChangeCallback);
            }
        }

        for (Map.Entry<ConnectorTaskId, WorkerTask> taskEntry : tasks.entrySet()) {
            if (taskEntry.getKey().connector().equals(connName)) {
                WorkerTask workerTask = taskEntry.getValue();
                try (LoaderSwap loaderSwap = plugins.withClassLoader(workerTask.loader())) {
                    workerTask.transitionTo(state);
                }
            }
        }
    }

    ConnectorStatusMetricsGroup connectorStatusMetricsGroup() {
        return connectorStatusMetricsGroup;
    }

    WorkerMetricsGroup workerMetricsGroup() {
        return workerMetricsGroup;
    }

    abstract class TaskBuilder {

        private final ConnectorTaskId id;
        private final ClusterConfigState configState;
        private final TaskStatus.Listener statusListener;
        private final TargetState initialState;

        private Task task = null;
        private ConnectorConfig connectorConfig = null;
        private Converter keyConverter = null;
        private Converter valueConverter = null;
        private HeaderConverter headerConverter = null;
        private ClassLoader classLoader = null;

        public TaskBuilder(ConnectorTaskId id,
                           ClusterConfigState configState,
                           TaskStatus.Listener statusListener,
                           TargetState initialState) {
            this.id = id;
            this.configState = configState;
            this.statusListener = statusListener;
            this.initialState = initialState;
        }

        public TaskBuilder withTask(Task task) {
            this.task = task;
            return this;
        }

        public TaskBuilder withConnectorConfig(ConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public TaskBuilder withKeyConverter(Converter keyConverter) {
            this.keyConverter = keyConverter;
            return this;
        }

        public TaskBuilder withValueConverter(Converter valueConverter) {
            this.valueConverter = valueConverter;
            return this;
        }

        public TaskBuilder withHeaderConverter(HeaderConverter headerConverter) {
            this.headerConverter = headerConverter;
            return this;
        }

        public TaskBuilder withClassloader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public WorkerTask build() {
            Objects.requireNonNull(task, "Task cannot be null");
            Objects.requireNonNull(connectorConfig, "Connector config used by task cannot be null");
            Objects.requireNonNull(keyConverter, "Key converter used by task cannot be null");
            Objects.requireNonNull(valueConverter, "Value converter used by task cannot be null");
            Objects.requireNonNull(headerConverter, "Header converter used by task cannot be null");
            Objects.requireNonNull(classLoader, "Classloader used by task cannot be null");

            ErrorHandlingMetrics errorHandlingMetrics = errorHandlingMetrics(id);
            final Class<? extends Connector> connectorClass = plugins.connectorClass(
                    connectorConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
            RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(connectorConfig.errorRetryTimeout(),
                    connectorConfig.errorMaxDelayInMillis(), connectorConfig.errorToleranceType(), Time.SYSTEM, errorHandlingMetrics);

            return doBuild(task, id, configState, statusListener, initialState,
                    connectorConfig, keyConverter, valueConverter, headerConverter, classLoader,
                    errorHandlingMetrics, connectorClass, retryWithToleranceOperator);
        }

        abstract WorkerTask doBuild(Task task,
                                    ConnectorTaskId id,
                                    ClusterConfigState configState,
                                    TaskStatus.Listener statusListener,
                                    TargetState initialState,
                                    ConnectorConfig connectorConfig,
                                    Converter keyConverter,
                                    Converter valueConverter,
                                    HeaderConverter headerConverter,
                                    ClassLoader classLoader,
                                    ErrorHandlingMetrics errorHandlingMetrics,
                                    Class<? extends Connector> connectorClass,
                                    RetryWithToleranceOperator retryWithToleranceOperator);

    }

    class SinkTaskBuilder extends TaskBuilder {
        public SinkTaskBuilder(ConnectorTaskId id,
                               ClusterConfigState configState,
                               TaskStatus.Listener statusListener,
                               TargetState initialState) {
            super(id, configState, statusListener, initialState);
        }

        @Override
        public WorkerTask doBuild(Task task,
                           ConnectorTaskId id,
                           ClusterConfigState configState,
                           TaskStatus.Listener statusListener,
                           TargetState initialState,
                           ConnectorConfig connectorConfig,
                           Converter keyConverter,
                           Converter valueConverter,
                           HeaderConverter headerConverter,
                           ClassLoader classLoader,
                           ErrorHandlingMetrics errorHandlingMetrics,
                           Class<? extends Connector> connectorClass,
                           RetryWithToleranceOperator retryWithToleranceOperator) {

            TransformationChain<SinkRecord> transformationChain = new TransformationChain<>(connectorConfig.<SinkRecord>transformations(), retryWithToleranceOperator);
            log.info("Initializing: {}", transformationChain);
            SinkConnectorConfig sinkConfig = new SinkConnectorConfig(plugins, connectorConfig.originalsStrings());
            retryWithToleranceOperator.reporters(sinkTaskReporters(id, sinkConfig, errorHandlingMetrics, connectorClass));
            WorkerErrantRecordReporter workerErrantRecordReporter = createWorkerErrantRecordReporter(sinkConfig, retryWithToleranceOperator,
                    keyConverter, valueConverter, headerConverter);

            Map<String, Object> consumerProps = baseConsumerConfigs(
                    id.connector(),  "connector-consumer-" + id, config, connectorConfig, connectorClass,
                    connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SINK);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

            return new WorkerSinkTask(id, (SinkTask) task, statusListener, initialState, config, configState, metrics, keyConverter,
                    valueConverter, errorHandlingMetrics, headerConverter, transformationChain, consumer, classLoader, time,
                    retryWithToleranceOperator, workerErrantRecordReporter, herder.statusBackingStore());
        }
    }

    class SourceTaskBuilder extends TaskBuilder {
        public SourceTaskBuilder(ConnectorTaskId id,
                               ClusterConfigState configState,
                               TaskStatus.Listener statusListener,
                               TargetState initialState) {
            super(id, configState, statusListener, initialState);
        }

        @Override
        public WorkerTask doBuild(Task task,
                           ConnectorTaskId id,
                           ClusterConfigState configState,
                           TaskStatus.Listener statusListener,
                           TargetState initialState,
                           ConnectorConfig connectorConfig,
                           Converter keyConverter,
                           Converter valueConverter,
                           HeaderConverter headerConverter,
                           ClassLoader classLoader,
                           ErrorHandlingMetrics errorHandlingMetrics,
                           Class<? extends Connector> connectorClass,
                           RetryWithToleranceOperator retryWithToleranceOperator) {

            SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins,
                    connectorConfig.originalsStrings(), config.topicCreationEnable());
            retryWithToleranceOperator.reporters(sourceTaskReporters(id, sourceConfig, errorHandlingMetrics));
            TransformationChain<SourceRecord> transformationChain = new TransformationChain<>(sourceConfig.<SourceRecord>transformations(), retryWithToleranceOperator);
            log.info("Initializing: {}", transformationChain);

            Map<String, Object> producerProps = baseProducerConfigs(id.connector(), "connector-producer-" + id, config, sourceConfig, connectorClass,
                    connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);

            TopicAdmin topicAdmin = null;
            final boolean topicCreationEnabled = sourceConnectorTopicCreationEnabled(sourceConfig);
            if (topicCreationEnabled || regularSourceTaskUsesConnectorSpecificOffsetsStore(sourceConfig)) {
                Map<String, Object> adminOverrides = adminConfigs(id.connector(), "connector-adminclient-" + id, config,
                        sourceConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SOURCE);
                topicAdmin = new TopicAdmin(adminOverrides);
            }

            Map<String, TopicCreationGroup> topicCreationGroups = topicCreationEnabled
                    ? TopicCreationGroup.configuredGroups(sourceConfig)
                    : null;

            // Set up the offset backing store for this task instance
            ConnectorOffsetBackingStore offsetStore = offsetStoreForRegularSourceTask(
                    id, sourceConfig, connectorClass, producer, producerProps, topicAdmin);
            offsetStore.configure(config);

            CloseableOffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, id.connector(), internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, id.connector(), internalKeyConverter, internalValueConverter);

            // Note we pass the configState as it performs dynamic transformations under the covers
            return new WorkerSourceTask(id, (SourceTask) task, statusListener, initialState, keyConverter, valueConverter, errorHandlingMetrics,
                    headerConverter, transformationChain, producer, topicAdmin, topicCreationGroups,
                    offsetReader, offsetWriter, offsetStore, config, configState, metrics, classLoader, time,
                    retryWithToleranceOperator, herder.statusBackingStore(), executor);
        }
    }

    class ExactlyOnceSourceTaskBuilder extends TaskBuilder {
        private final Runnable preProducerCheck;
        private final Runnable postProducerCheck;

        public ExactlyOnceSourceTaskBuilder(ConnectorTaskId id,
                                            ClusterConfigState configState,
                                            TaskStatus.Listener statusListener,
                                            TargetState initialState,
                                            Runnable preProducerCheck,
                                            Runnable postProducerCheck) {
            super(id, configState, statusListener, initialState);
            this.preProducerCheck = preProducerCheck;
            this.postProducerCheck = postProducerCheck;
        }

        @Override
        public WorkerTask doBuild(Task task,
                                  ConnectorTaskId id,
                                  ClusterConfigState configState,
                                  TaskStatus.Listener statusListener,
                                  TargetState initialState,
                                  ConnectorConfig connectorConfig,
                                  Converter keyConverter,
                                  Converter valueConverter,
                                  HeaderConverter headerConverter,
                                  ClassLoader classLoader,
                                  ErrorHandlingMetrics errorHandlingMetrics,
                                  Class<? extends Connector> connectorClass,
                                  RetryWithToleranceOperator retryWithToleranceOperator) {

            SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins,
                    connectorConfig.originalsStrings(), config.topicCreationEnable());
            retryWithToleranceOperator.reporters(sourceTaskReporters(id, sourceConfig, errorHandlingMetrics));
            TransformationChain<SourceRecord> transformationChain = new TransformationChain<>(sourceConfig.<SourceRecord>transformations(), retryWithToleranceOperator);
            log.info("Initializing: {}", transformationChain);

            Map<String, Object> producerProps = exactlyOnceSourceTaskProducerConfigs(
                    id, config, sourceConfig, connectorClass,
                    connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);

            // Create a topic admin that the task will use for its offsets topic and, potentially, automatic topic creation
            Map<String, Object> adminOverrides = adminConfigs(id.connector(), "connector-adminclient-" + id, config,
                    sourceConfig, connectorClass, connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SOURCE);
            TopicAdmin topicAdmin = new TopicAdmin(adminOverrides);

            Map<String, TopicCreationGroup> topicCreationGroups = sourceConnectorTopicCreationEnabled(sourceConfig)
                    ? TopicCreationGroup.configuredGroups(sourceConfig)
                    : null;

            // Set up the offset backing store for this task instance
            ConnectorOffsetBackingStore offsetStore = offsetStoreForExactlyOnceSourceTask(
                    id, sourceConfig, connectorClass, producer, producerProps, topicAdmin);
            offsetStore.configure(config);

            CloseableOffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, id.connector(), internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, id.connector(), internalKeyConverter, internalValueConverter);

            // Note we pass the configState as it performs dynamic transformations under the covers
            return new ExactlyOnceWorkerSourceTask(id, (SourceTask) task, statusListener, initialState, keyConverter, valueConverter,
                    headerConverter, transformationChain, producer, topicAdmin, topicCreationGroups,
                    offsetReader, offsetWriter, offsetStore, config, configState, metrics, errorHandlingMetrics, classLoader, time, retryWithToleranceOperator,
                    herder.statusBackingStore(), sourceConfig, executor, preProducerCheck, postProducerCheck);
        }
    }

    // Visible for testing
    ConnectorOffsetBackingStore offsetStoreForRegularSourceConnector(
            SourceConnectorConfig sourceConfig,
            String connName,
            Connector connector
    ) {
        String connectorSpecificOffsetsTopic = sourceConfig.offsetsTopic();

        Map<String, Object> producerProps = baseProducerConfigs(connName, "connector-producer-" + connName, config, sourceConfig, connector.getClass(),
                connectorClientConfigOverridePolicy, kafkaClusterId);

        // We use a connector-specific store (i.e., a dedicated KafkaOffsetBackingStore for this connector)
        // if the worker supports per-connector offsets topics (which may be the case in distributed but not standalone mode, for example)
        // and if the connector is explicitly configured with an offsets topic
        final boolean usesConnectorSpecificStore = connectorSpecificOffsetsTopic != null
                && config.connectorOffsetsTopicsPermitted();

        if (usesConnectorSpecificStore) {
            Map<String, Object> consumerProps = regularSourceOffsetsConsumerConfigs(
                        connName, "connector-consumer-" + connName, config, sourceConfig, connector.getClass(),
                        connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

            Map<String, Object> adminOverrides = adminConfigs(connName, "connector-adminclient-" + connName, config,
                    sourceConfig, connector.getClass(), connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SOURCE);

            TopicAdmin admin = new TopicAdmin(adminOverrides);
            KafkaOffsetBackingStore connectorStore =
                    KafkaOffsetBackingStore.forConnector(connectorSpecificOffsetsTopic, consumer, admin);

            // If the connector's offsets topic is the same as the worker-global offsets topic, there's no need to construct
            // an offset store that has a primary and a secondary store which both read from that same topic.
            // So, if the user has explicitly configured the connector with a connector-specific offsets topic
            // but we know that that topic is the same as the worker-global offsets topic, we ignore the worker-global
            // offset store and build a store backed exclusively by a connector-specific offsets store.
            // It may seem reasonable to instead build a store backed exclusively by the worker-global offset store, but that
            // would prevent users from being able to customize the config properties used for the Kafka clients that
            // access the offsets topic, and we would not be able to establish reasonable defaults like setting
            // isolation.level=read_committed for the offsets topic consumer for this connector
            if (sameOffsetTopicAsWorker(connectorSpecificOffsetsTopic, producerProps)) {
                return ConnectorOffsetBackingStore.withOnlyConnectorStore(
                        () -> LoggingContext.forConnector(connName),
                        connectorStore,
                        connectorSpecificOffsetsTopic,
                        admin
                );
            } else {
                return ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                        () -> LoggingContext.forConnector(connName),
                        globalOffsetBackingStore,
                        connectorStore,
                        connectorSpecificOffsetsTopic,
                        admin
                );
            }
        } else {
            return ConnectorOffsetBackingStore.withOnlyWorkerStore(
                    () -> LoggingContext.forConnector(connName),
                    globalOffsetBackingStore,
                    config.offsetsTopic()
            );
        }
    }

    // Visible for testing
    ConnectorOffsetBackingStore offsetStoreForExactlyOnceSourceConnector(
            SourceConnectorConfig sourceConfig,
            String connName,
            Connector connector
    ) {
        String connectorSpecificOffsetsTopic = Optional.ofNullable(sourceConfig.offsetsTopic()).orElse(config.offsetsTopic());

        Map<String, Object> producerProps = baseProducerConfigs(connName, "connector-producer-" + connName, config, sourceConfig, connector.getClass(),
                connectorClientConfigOverridePolicy, kafkaClusterId);

        Map<String, Object> consumerProps = exactlyOnceSourceOffsetsConsumerConfigs(
                    connName, "connector-consumer-" + connName, config, sourceConfig, connector.getClass(),
                    connectorClientConfigOverridePolicy, kafkaClusterId);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

        Map<String, Object> adminOverrides = adminConfigs(connName, "connector-adminclient-" + connName, config,
                sourceConfig, connector.getClass(), connectorClientConfigOverridePolicy, kafkaClusterId, ConnectorType.SOURCE);

        TopicAdmin admin = new TopicAdmin(adminOverrides);
        KafkaOffsetBackingStore connectorStore =
                KafkaOffsetBackingStore.forConnector(connectorSpecificOffsetsTopic, consumer, admin);

        // If the connector's offsets topic is the same as the worker-global offsets topic, there's no need to construct
        // an offset store that has a primary and a secondary store which both read from that same topic.
        // So, even if the user has explicitly configured the connector with a connector-specific offsets topic,
        // if we know that that topic is the same as the worker-global offsets topic, we ignore the worker-global
        // offset store and build a store backed exclusively by a connector-specific offsets store.
        // It may seem reasonable to instead build a store backed exclusively by the worker-global offset store, but that
        // would prevent users from being able to customize the config properties used for the Kafka clients that
        // access the offsets topic, and may lead to confusion for them when tasks are created for the connector
        // since they will all have their own dedicated offsets stores anyways
        if (sameOffsetTopicAsWorker(connectorSpecificOffsetsTopic, producerProps)) {
            return ConnectorOffsetBackingStore.withOnlyConnectorStore(
                    () -> LoggingContext.forConnector(connName),
                    connectorStore,
                    connectorSpecificOffsetsTopic,
                    admin
            );
        } else {
            return ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                    () -> LoggingContext.forConnector(connName),
                    globalOffsetBackingStore,
                    connectorStore,
                    connectorSpecificOffsetsTopic,
                    admin
            );
        }
    }

    // Visible for testing
    ConnectorOffsetBackingStore offsetStoreForRegularSourceTask(
            ConnectorTaskId id,
            SourceConnectorConfig sourceConfig,
            Class<? extends Connector> connectorClass,
            Producer<byte[], byte[]> producer,
            Map<String, Object> producerProps,
            TopicAdmin topicAdmin
    ) {
        String connectorSpecificOffsetsTopic = sourceConfig.offsetsTopic();

        if (regularSourceTaskUsesConnectorSpecificOffsetsStore(sourceConfig)) {
            Objects.requireNonNull(topicAdmin, "Source tasks require a non-null topic admin when configured to use their own offsets topic");

            Map<String, Object> consumerProps = regularSourceOffsetsConsumerConfigs(
                    id.connector(), "connector-consumer-" + id, config, sourceConfig, connectorClass,
                    connectorClientConfigOverridePolicy, kafkaClusterId);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

            KafkaOffsetBackingStore connectorStore =
                    KafkaOffsetBackingStore.forTask(sourceConfig.offsetsTopic(), producer, consumer, topicAdmin);

            // If the connector's offsets topic is the same as the worker-global offsets topic, there's no need to construct
            // an offset store that has a primary and a secondary store which both read from that same topic.
            // So, if the user has (implicitly or explicitly) configured the connector with a connector-specific offsets topic
            // but we know that that topic is the same as the worker-global offsets topic, we ignore the worker-global
            // offset store and build a store backed exclusively by a connector-specific offsets store.
            // It may seem reasonable to instead build a store backed exclusively by the worker-global offset store, but that
            // would prevent users from being able to customize the config properties used for the Kafka clients that
            // access the offsets topic, and we would not be able to establish reasonable defaults like setting
            // isolation.level=read_committed for the offsets topic consumer for this task
            if (sameOffsetTopicAsWorker(sourceConfig.offsetsTopic(), producerProps)) {
                return ConnectorOffsetBackingStore.withOnlyConnectorStore(
                        () -> LoggingContext.forTask(id),
                        connectorStore,
                        connectorSpecificOffsetsTopic,
                        topicAdmin
                );
            } else {
                return ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                        () -> LoggingContext.forTask(id),
                        globalOffsetBackingStore,
                        connectorStore,
                        connectorSpecificOffsetsTopic,
                        topicAdmin
                );
            }
        } else {
            return ConnectorOffsetBackingStore.withOnlyWorkerStore(
                    () -> LoggingContext.forTask(id),
                    globalOffsetBackingStore,
                    config.offsetsTopic()
            );
        }
    }

    // Visible for testing
    ConnectorOffsetBackingStore offsetStoreForExactlyOnceSourceTask(
            ConnectorTaskId id,
            SourceConnectorConfig sourceConfig,
            Class<? extends Connector> connectorClass,
            Producer<byte[], byte[]> producer,
            Map<String, Object> producerProps,
            TopicAdmin topicAdmin
    ) {
        Objects.requireNonNull(topicAdmin, "Source tasks require a non-null topic admin when exactly-once support is enabled");

        Map<String, Object> consumerProps = exactlyOnceSourceOffsetsConsumerConfigs(
                id.connector(), "connector-consumer-" + id, config, sourceConfig, connectorClass,
                connectorClientConfigOverridePolicy, kafkaClusterId);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

        String connectorOffsetsTopic = Optional.ofNullable(sourceConfig.offsetsTopic()).orElse(config.offsetsTopic());

        KafkaOffsetBackingStore connectorStore =
                KafkaOffsetBackingStore.forTask(connectorOffsetsTopic, producer, consumer, topicAdmin);

        // If the connector's offsets topic is the same as the worker-global offsets topic, there's no need to construct
        // an offset store that has a primary and a secondary store which both read from that same topic.
        // So, if the user has (implicitly or explicitly) configured the connector with a connector-specific offsets topic
        // but we know that that topic is the same as the worker-global offsets topic, we ignore the worker-global
        // offset store and build a store backed exclusively by a connector-specific offsets store.
        // We cannot under any circumstances build an offset store backed exclusively by the worker-global offset store
        // as that would prevent us from being able to write source records and source offset information for the task
        // with the same producer, and therefore, in the same transaction.
        if (sameOffsetTopicAsWorker(connectorOffsetsTopic, producerProps)) {
            return ConnectorOffsetBackingStore.withOnlyConnectorStore(
                    () -> LoggingContext.forTask(id),
                    connectorStore,
                    connectorOffsetsTopic,
                    topicAdmin
            );
        } else {
            return ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                    () -> LoggingContext.forTask(id),
                    globalOffsetBackingStore,
                    connectorStore,
                    connectorOffsetsTopic,
                    topicAdmin
            );
        }
    }

    /**
     * Gives a best-effort guess for whether the given offsets topic is the same topic as the worker-global offsets topic.
     * Even if the name of the topic is the same as the name of the worker's offsets topic, the two may still be different topics
     * if the connector is configured to produce to a different Kafka cluster than the one that hosts the worker's offsets topic.
     * @param offsetsTopic the name of the offsets topic for the connector
     * @param producerProps the producer configuration for the connector
     * @return whether it appears that the connector's offsets topic is the same topic as the worker-global offsets topic.
     * If {@code true}, it is guaranteed that the two are the same;
     * if {@code false}, it is likely but not guaranteed that the two are not the same
     */
    private boolean sameOffsetTopicAsWorker(String offsetsTopic, Map<String, Object> producerProps) {
        // We can check the offset topic name and the Kafka cluster's bootstrap servers,
        // although this isn't exact and can lead to some false negatives if the user
        // provides an overridden bootstrap servers value for their producer that is different than
        // the worker's but still resolves to the same Kafka cluster used by the worker.
        // At the moment this is probably adequate, especially since we don't want to put
        // a network ping to a remote Kafka cluster inside the herder's tick thread (which is where this
        // logic takes place right now) in case that takes a while.
        Set<String> workerBootstrapServers = new HashSet<>(config.getList(BOOTSTRAP_SERVERS_CONFIG));
        Set<String> producerBootstrapServers = new HashSet<>();
        try {
            String rawBootstrapServers = producerProps.getOrDefault(BOOTSTRAP_SERVERS_CONFIG, "").toString();
            @SuppressWarnings("unchecked")
            List<String> parsedBootstrapServers = (List<String>) ConfigDef.parseType(BOOTSTRAP_SERVERS_CONFIG, rawBootstrapServers, ConfigDef.Type.LIST);
            producerBootstrapServers.addAll(parsedBootstrapServers);
        } catch (Exception e) {
            // Should never happen by this point, but if it does, make sure to present a readable error message to the user
            throw new ConnectException("Failed to parse bootstrap servers property in producer config", e);
        }
        return offsetsTopic.equals(config.offsetsTopic())
                && workerBootstrapServers.equals(producerBootstrapServers);
    }

    private boolean regularSourceTaskUsesConnectorSpecificOffsetsStore(SourceConnectorConfig sourceConfig) {
        // We use a connector-specific store (i.e., a dedicated KafkaOffsetBackingStore for this task)
        // if the worker supports per-connector offsets topics (which may be the case in distributed mode but not standalone, for example)
        // and the user has explicitly specified an offsets topic for the connector
        return sourceConfig.offsetsTopic() != null && config.connectorOffsetsTopicsPermitted();
    }

    private boolean sourceConnectorTopicCreationEnabled(SourceConnectorConfig sourceConfig) {
        return config.topicCreationEnable() && sourceConfig.usesTopicCreation();
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

}
