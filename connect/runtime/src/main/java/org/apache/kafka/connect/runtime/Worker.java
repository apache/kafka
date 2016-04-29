/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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
    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final ExecutorService executor;
    private final Time time;
    private final String workerId;
    private final WorkerConfig config;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final Converter internalKeyConverter;
    private final Converter internalValueConverter;
    private final OffsetBackingStore offsetBackingStore;

    private HashMap<String, WorkerConnector> connectors = new HashMap<>();
    private HashMap<ConnectorTaskId, WorkerTask> tasks = new HashMap<>();
    private KafkaProducer<byte[], byte[]> producer;
    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;

    public Worker(String workerId,
                  Time time,
                  WorkerConfig config,
                  OffsetBackingStore offsetBackingStore) {
        this.executor = Executors.newCachedThreadPool();
        this.workerId = workerId;
        this.time = time;
        this.config = config;
        this.keyConverter = config.getConfiguredInstance(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Converter.class);
        this.keyConverter.configure(config.originalsWithPrefix("key.converter."), true);
        this.valueConverter = config.getConfiguredInstance(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
        this.valueConverter.configure(config.originalsWithPrefix("value.converter."), false);
        this.internalKeyConverter = config.getConfiguredInstance(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Converter.class);
        this.internalKeyConverter.configure(config.originalsWithPrefix("internal.key.converter."), true);
        this.internalValueConverter = config.getConfiguredInstance(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
        this.internalValueConverter.configure(config.originalsWithPrefix("internal.value.converter."), false);

        this.offsetBackingStore = offsetBackingStore;
        this.offsetBackingStore.configure(config);
    }

    public void start() {
        log.info("Worker starting");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        // These settings are designed to ensure there is no data loss. They *may* be overridden via configs passed to the
        // worker, but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ((Integer) Integer.MAX_VALUE).toString());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, ((Integer) Integer.MAX_VALUE).toString());
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, ((Long) Long.MAX_VALUE).toString());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        producerProps.putAll(config.originalsWithPrefix("producer."));

        producer = new KafkaProducer<>(producerProps);

        offsetBackingStore.start();
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(config);

        log.info("Worker started");
    }

    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        for (Map.Entry<String, WorkerConnector> entry : connectors.entrySet()) {
            WorkerConnector workerConnector = entry.getValue();
            log.warn("Shutting down connector {} uncleanly; herder should have shut down connectors before the" +
                    "Worker is stopped.", entry.getKey());
            workerConnector.shutdown();
        }

        Collection<ConnectorTaskId> taskIds = tasks.keySet();
        log.warn("Shutting down tasks {} uncleanly; herder should have shut down "
                + "tasks before the Worker is stopped.", taskIds);
        stopTasks(taskIds);
        awaitStopTasks(taskIds);

        long timeoutMs = limit - time.milliseconds();
        sourceTaskOffsetCommitter.close(timeoutMs);

        offsetBackingStore.stop();

        log.info("Worker stopped");
    }

    /**
     * Add a new connector.
     * @param connConfig connector configuration
     * @param ctx context for the connector
     * @param statusListener listener for notifications of connector status changes
     * @param initialState the initial target state that the connector should be initialized to
     */
    public void startConnector(ConnectorConfig connConfig,
                               ConnectorContext ctx,
                               ConnectorStatus.Listener statusListener,
                               TargetState initialState) {
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        Class<? extends Connector> connClass = getConnectorClass(connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG));

        log.info("Creating connector {} of type {}", connName, connClass.getName());

        if (connectors.containsKey(connName))
            throw new ConnectException("Connector with name " + connName + " already exists");

        final Connector connector = instantiateConnector(connClass);
        WorkerConnector workerConnector = new WorkerConnector(connName, connector, ctx, statusListener);

        log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connClass.getName());
        workerConnector.initialize(connConfig);
        workerConnector.transitionTo(initialState);

        connectors.put(connName, workerConnector);
        log.info("Finished creating connector {}", connName);
    }

    /* Now that the configuration doesn't contain the actual class name, we need to be able to tell the herder whether a connector is a Sink */
    public boolean isSinkConnector(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        return workerConnector.isSinkConnector();
    }

    public Connector getConnector(String connType) {
        Class<? extends Connector> connectorClass = getConnectorClass(connType);
        return instantiateConnector(connectorClass);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Connector> getConnectorClass(String connectorAlias) {
        // Avoid the classpath scan if the full class name was provided
        try {
            Class<?> clazz = Class.forName(connectorAlias);
            if (!Connector.class.isAssignableFrom(clazz))
                throw new ConnectException("Class " + connectorAlias + " does not implement Connector");
            return (Class<? extends Connector>) clazz;
        } catch (ClassNotFoundException e) {
            // Fall through to scan for the alias
        }

        // Iterate over our entire classpath to find all the connectors and hopefully one of them matches the alias from the connector configration
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forJavaClassPath()));

        Set<Class<? extends Connector>> connectors = reflections.getSubTypesOf(Connector.class);

        List<Class<? extends Connector>> results = new ArrayList<>();

        for (Class<? extends Connector> connector: connectors) {
            // Configuration included the class name but not package
            if (connector.getSimpleName().equals(connectorAlias))
                results.add(connector);

            // Configuration included a short version of the name (i.e. FileStreamSink instead of FileStreamSinkConnector)
            if (connector.getSimpleName().equals(connectorAlias + "Connector"))
                results.add(connector);
        }

        if (results.isEmpty())
            throw new ConnectException("Failed to find any class that implements Connector and which name matches " + connectorAlias + " available connectors are: " + connectorNames(connectors));
        if (results.size() > 1) {
            throw new ConnectException("More than one connector matches alias " +  connectorAlias + ". Please use full package + class name instead. Classes found: " + connectorNames(results));
        }

        // We just validated that we have exactly one result, so this is safe
        return results.get(0);
    }

    private String connectorNames(Collection<Class<? extends Connector>> connectors) {
        StringBuilder names = new StringBuilder();
        for (Class<?> c : connectors)
            names.append(c.getName()).append(", ");

        return names.substring(0, names.toString().length() - 2);
    }

    public boolean ownsTask(ConnectorTaskId taskId) {
        return tasks.containsKey(taskId);
    }

    private static Connector instantiateConnector(Class<? extends Connector> connClass) {
        try {
            return Utils.newInstance(connClass);
        } catch (Throwable t) {
            // Catches normal exceptions due to instantiation errors as well as any runtime errors that
            // may be caused by user code
            throw new ConnectException("Failed to create connector instance", t);
        }
    }

    public List<Map<String, String>> connectorTaskConfigs(String connName, int maxTasks, List<String> sinkTopics) {
        log.trace("Reconfiguring connector tasks for {}", connName);

        WorkerConnector workerConnector = connectors.get(connName);
        if (workerConnector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        Connector connector = workerConnector.connector();
        List<Map<String, String>> result = new ArrayList<>();
        String taskClassName = connector.taskClass().getName();
        for (Map<String, String> taskProps : connector.taskConfigs(maxTasks)) {
            Map<String, String> taskConfig = new HashMap<>(taskProps); // Ensure we don't modify the connector's copy of the config
            taskConfig.put(TaskConfig.TASK_CLASS_CONFIG, taskClassName);
            if (sinkTopics != null)
                taskConfig.put(SinkTask.TOPICS_CONFIG, Utils.join(sinkTopics, ","));
            result.add(taskConfig);
        }
        return result;
    }

    public void stopConnector(String connName) {
        log.info("Stopping connector {}", connName);

        WorkerConnector connector = connectors.get(connName);
        if (connector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        connector.shutdown();
        connectors.remove(connName);

        log.info("Stopped connector {}", connName);
    }

    /**
     * Get the IDs of the connectors currently running in this worker.
     */
    public Set<String> connectorNames() {
        return connectors.keySet();
    }

    public boolean isRunning(String connName) {
        WorkerConnector connector = connectors.get(connName);
        if (connector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");
        return connector.isRunning();
    }

    /**
     * Add a new task.
     * @param id Globally unique ID for this task.
     * @param taskConfig the parsed task configuration
     * @param statusListener listener for notifications of task status changes
     * @param initialState the initial target state that the task should be initialized to
     */
    public void startTask(ConnectorTaskId id,
                          TaskConfig taskConfig,
                          TaskStatus.Listener statusListener,
                          TargetState initialState) {
        log.info("Creating task {}", id);

        if (tasks.containsKey(id)) {
            String msg = "Task already exists in this worker; the herder should not have requested "
                    + "that this : " + id;
            log.error(msg);
            throw new ConnectException(msg);
        }

        Class<? extends Task> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(Task.class);
        final Task task = instantiateTask(taskClass);
        log.info("Instantiated task {} with version {} of type {}", id, task.version(), taskClass.getName());

        final WorkerTask workerTask = buildWorkerTask(id, task, statusListener, initialState);

        // Start the task before adding modifying any state, any exceptions are caught higher up the
        // call chain and there's no cleanup to do here
        workerTask.initialize(taskConfig);
        executor.submit(workerTask);

        if (task instanceof SourceTask) {
            WorkerSourceTask workerSourceTask = (WorkerSourceTask) workerTask;
            sourceTaskOffsetCommitter.schedule(id, workerSourceTask);
        }
        tasks.put(id, workerTask);
    }

    private WorkerTask buildWorkerTask(ConnectorTaskId id,
                                       Task task,
                                       TaskStatus.Listener statusListener,
                                       TargetState initialState) {
        // Decide which type of worker task we need based on the type of task.
        if (task instanceof SourceTask) {
            OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            return new WorkerSourceTask(id, (SourceTask) task, statusListener, initialState, keyConverter,
                     valueConverter, producer, offsetReader, offsetWriter, config, time);
        } else if (task instanceof SinkTask) {
            return new WorkerSinkTask(id, (SinkTask) task, statusListener, initialState, config, keyConverter,
                    valueConverter, time);
        } else {
            log.error("Tasks must be a subclass of either SourceTask or SinkTask", task);
            throw new ConnectException("Tasks must be a subclass of either SourceTask or SinkTask");
        }
    }

    private static Task instantiateTask(Class<? extends Task> taskClass) {
        try {
            return Utils.newInstance(taskClass);
        } catch (KafkaException e) {
            throw new ConnectException("Task class not found", e);
        }
    }

    public void stopTasks(Collection<ConnectorTaskId> ids) {
        for (ConnectorTaskId id : ids)
            stopTask(getTask(id));
    }

    public void awaitStopTasks(Collection<ConnectorTaskId> ids) {
        long now = time.milliseconds();
        long deadline = now + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);
        for (ConnectorTaskId id : ids) {
            long remaining = Math.max(0, deadline - time.milliseconds());
            awaitStopTask(getTask(id), remaining);
        }
    }

    private void awaitStopTask(WorkerTask task, long timeout) {
        if (!task.awaitStop(timeout)) {
            log.error("Graceful stop of task {} failed.", task.id());
            task.cancel();
        }
        tasks.remove(task.id());
    }

    private void stopTask(WorkerTask task) {
        log.info("Stopping task {}", task.id());
        if (task instanceof WorkerSourceTask)
            sourceTaskOffsetCommitter.remove(task.id());
        task.stop();
    }

    public void stopAndAwaitTask(ConnectorTaskId id) {
        WorkerTask task = getTask(id);
        stopTask(task);
        awaitStopTask(task, config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG));
    }

    /**
     * Get the IDs of the tasks currently running in this worker.
     */
    public Set<ConnectorTaskId> taskIds() {
        return tasks.keySet();
    }

    private WorkerTask getTask(ConnectorTaskId id) {
        WorkerTask task = tasks.get(id);
        if (task == null) {
            log.error("Task not found: " + id);
            throw new ConnectException("Task not found: " + id);
        }
        return task;
    }

    public Converter getInternalKeyConverter() {
        return internalKeyConverter;
    }

    public Converter getInternalValueConverter() {
        return internalValueConverter;
    }

    public String workerId() {
        return workerId;
    }

    public boolean ownsConnector(String connName) {
        return this.connectors.containsKey(connName);
    }

    public void setTargetState(String connName, TargetState state) {
        log.info("Setting connector {} state to {}", connName, state);

        WorkerConnector connector = connectors.get(connName);
        if (connector != null)
            connector.transitionTo(state);

        for (Map.Entry<ConnectorTaskId, WorkerTask> taskEntry : tasks.entrySet()) {
            if (taskEntry.getKey().connector().equals(connName))
                taskEntry.getValue().transitionTo(state);
        }
    }

}
