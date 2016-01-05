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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
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

    private Time time;
    private WorkerConfig config;
    private Converter keyConverter;
    private Converter valueConverter;
    private Converter internalKeyConverter;
    private Converter internalValueConverter;
    private OffsetBackingStore offsetBackingStore;
    private HashMap<String, Connector> connectors = new HashMap<>();
    private HashMap<ConnectorTaskId, WorkerTask> tasks = new HashMap<>();
    private KafkaProducer<byte[], byte[]> producer;
    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;

    public Worker(WorkerConfig config, OffsetBackingStore offsetBackingStore) {
        this(new SystemTime(), config, offsetBackingStore);
    }

    @SuppressWarnings("unchecked")
    public Worker(Time time, WorkerConfig config, OffsetBackingStore offsetBackingStore) {
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
        this.offsetBackingStore.configure(config.originals());
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
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(time, config);

        log.info("Worker started");
    }

    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        for (Map.Entry<String, Connector> entry : connectors.entrySet()) {
            Connector conn = entry.getValue();
            log.warn("Shutting down connector {} uncleanly; herder should have shut down connectors before the" +
                    "Worker is stopped.", conn);
            try {
                conn.stop();
            } catch (ConnectException e) {
                log.error("Error while shutting down connector " + conn, e);
            }
        }

        for (Map.Entry<ConnectorTaskId, WorkerTask> entry : tasks.entrySet()) {
            WorkerTask task = entry.getValue();
            log.warn("Shutting down task {} uncleanly; herder should have shut down "
                    + "tasks before the Worker is stopped.", task);
            try {
                task.stop();
            } catch (ConnectException e) {
                log.error("Error while shutting down task " + task, e);
            }
        }

        for (Map.Entry<ConnectorTaskId, WorkerTask> entry : tasks.entrySet()) {
            WorkerTask task = entry.getValue();
            log.debug("Waiting for task {} to finish shutting down", task);
            if (!task.awaitStop(Math.max(limit - time.milliseconds(), 0)))
                log.error("Graceful shutdown of task {} failed.", task);
            task.close();
        }

        long timeoutMs = limit - time.milliseconds();
        sourceTaskOffsetCommitter.close(timeoutMs);

        offsetBackingStore.stop();

        log.info("Worker stopped");
    }

    public WorkerConfig config() {
        return config;
    }

    /**
     * Add a new connector.
     * @param connConfig connector configuration
     * @param ctx context for the connector
     */
    public void addConnector(ConnectorConfig connConfig, ConnectorContext ctx) {
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        Class<? extends Connector> connClass = getConnectorClass(connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG));

        log.info("Creating connector {} of type {}", connName, connClass.getName());

        if (connectors.containsKey(connName))
            throw new ConnectException("Connector with name " + connName + " already exists");

        final Connector connector = instantiateConnector(connClass);
        log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connClass.getName());
        connector.initialize(ctx);
        try {
            connector.start(connConfig.originalsStrings());
        } catch (ConnectException e) {
            throw new ConnectException("Connector threw an exception while starting", e);
        }

        connectors.put(connName, connector);

        log.info("Finished creating connector {}", connName);
    }

    /* Now that the configuration doesn't contain the actual class name, we need to be able to tell the herder whether a connector is a Sink */
    public boolean isSinkConnector(String connName) {
        return SinkConnector.class.isAssignableFrom(connectors.get(connName).getClass());
    }


    // Iterate over our entire classpath to find all the connectors and hopefully one of them matches the alias from the connector configration
    private Class<? extends Connector> getConnectorClass(String connectorAlias) {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forJavaClassPath()));

        Set<Class<? extends Connector>> connectors = reflections.getSubTypesOf(Connector.class);

        List<Class<? extends Connector>> results = new ArrayList<>();

        for (Class<? extends Connector> connector: connectors) {
            // Configuration included the fully qualified class name
            if (connector.getName().equals(connectorAlias))
                results.add(connector);

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

        Connector connector = connectors.get(connName);
        if (connector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

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

        Connector connector = connectors.get(connName);
        if (connector == null)
            throw new ConnectException("Connector " + connName + " not found in this worker.");

        try {
            connector.stop();
        } catch (ConnectException e) {
            log.error("Error shutting down connector {}: ", connector, e);
        }

        connectors.remove(connName);

        log.info("Stopped connector {}", connName);
    }

    /**
     * Get the IDs of the connectors currently running in this worker.
     */
    public Set<String> connectorNames() {
        return connectors.keySet();
    }

    /**
     * Add a new task.
     * @param id Globally unique ID for this task.
     * @param taskConfig the parsed task configuration
     */
    public void addTask(ConnectorTaskId id, TaskConfig taskConfig) {
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

        // Decide which type of worker task we need based on the type of task.
        final WorkerTask workerTask;
        if (task instanceof SourceTask) {
            SourceTask sourceTask = (SourceTask) task;
            OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            workerTask = new WorkerSourceTask(id, sourceTask, keyConverter, valueConverter, producer,
                    offsetReader, offsetWriter, config, time);
        } else if (task instanceof SinkTask) {
            workerTask = new WorkerSinkTask(id, (SinkTask) task, config, keyConverter, valueConverter, time);
        } else {
            log.error("Tasks must be a subclass of either SourceTask or SinkTask", task);
            throw new ConnectException("Tasks must be a subclass of either SourceTask or SinkTask");
        }

        // Start the task before adding modifying any state, any exceptions are caught higher up the
        // call chain and there's no cleanup to do here
        workerTask.start(taskConfig.originalsStrings());
        if (task instanceof SourceTask) {
            WorkerSourceTask workerSourceTask = (WorkerSourceTask) workerTask;
            sourceTaskOffsetCommitter.schedule(id, workerSourceTask);
        }
        tasks.put(id, workerTask);
    }

    private static Task instantiateTask(Class<? extends Task> taskClass) {
        try {
            return Utils.newInstance(taskClass);
        } catch (KafkaException e) {
            throw new ConnectException("Task class not found", e);
        }
    }

    public void stopTask(ConnectorTaskId id) {
        log.info("Stopping task {}", id);

        WorkerTask task = getTask(id);
        if (task instanceof WorkerSourceTask)
            sourceTaskOffsetCommitter.remove(id);
        task.stop();
        if (!task.awaitStop(config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG)))
            log.error("Graceful stop of task {} failed.", task);
        task.close();
        tasks.remove(id);
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
}
