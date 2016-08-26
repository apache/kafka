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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    private final ConnectorFactory connectorFactory;
    private final WorkerConfig config;
    private final Converter defaultKeyConverter;
    private final Converter defaultValueConverter;
    private final Converter internalKeyConverter;
    private final Converter internalValueConverter;
    private final OffsetBackingStore offsetBackingStore;
    private final Map<String, Object> producerProps;

    private HashMap<String, WorkerConnector> connectors = new HashMap<>();
    private HashMap<ConnectorTaskId, WorkerTask> tasks = new HashMap<>();
    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;

    public Worker(String workerId, Time time, ConnectorFactory connectorFactory, WorkerConfig config, OffsetBackingStore offsetBackingStore) {
        this.executor = Executors.newCachedThreadPool();
        this.workerId = workerId;
        this.time = time;
        this.connectorFactory = connectorFactory;
        this.config = config;
        this.defaultKeyConverter = config.getConfiguredInstance(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Converter.class);
        this.defaultKeyConverter.configure(config.originalsWithPrefix("key.converter."), true);
        this.defaultValueConverter = config.getConfiguredInstance(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
        this.defaultValueConverter.configure(config.originalsWithPrefix("value.converter."), false);
        this.internalKeyConverter = config.getConfiguredInstance(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Converter.class);
        this.internalKeyConverter.configure(config.originalsWithPrefix("internal.key.converter."), true);
        this.internalValueConverter = config.getConfiguredInstance(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
        this.internalValueConverter.configure(config.originalsWithPrefix("internal.value.converter."), false);

        this.offsetBackingStore = offsetBackingStore;
        this.offsetBackingStore.configure(config);

        producerProps = new HashMap<>();
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
        // User-specified overrides
        producerProps.putAll(config.originalsWithPrefix("producer."));
    }

    public void start() {
        log.info("Worker starting");

        offsetBackingStore.start();
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(config);

        log.info("Worker started");
    }

    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        if (!connectors.isEmpty()) {
            log.warn("Shutting down connectors {} uncleanly; herder should have shut down connectors before the Worker is stopped", connectors.keySet());
            stopConnectors();
        }

        if (!tasks.isEmpty()) {
            log.warn("Shutting down tasks {} uncleanly; herder should have shut down tasks before the Worker is stopped", tasks.keySet());
            stopAndAwaitTasks();
        }

        long timeoutMs = limit - time.milliseconds();
        sourceTaskOffsetCommitter.close(timeoutMs);

        offsetBackingStore.stop();

        log.info("Worker stopped");
    }

    public boolean startConnector(
            String connName,
            Map<String, String> connProps,
            ConnectorContext ctx,
            ConnectorStatus.Listener statusListener,
            TargetState initialState
    ) {
        if (connectors.containsKey(connName))
            throw new ConnectException("Connector with name " + connName + " already exists");

        final WorkerConnector workerConnector;
        try {
            final ConnectorConfig connConfig = new ConnectorConfig(connProps);
            final String connClass = connConfig.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            log.info("Creating connector {} of type {}", connName, connClass);
            final Connector connector = connectorFactory.newConnector(connClass);
            workerConnector = new WorkerConnector(connName, connector, ctx, statusListener);
            log.info("Instantiated connector {} with version {} of type {}", connName, connector.version(), connector.getClass());
            workerConnector.initialize(connConfig);
            workerConnector.transitionTo(initialState);
        } catch (Throwable t) {
            log.error("Failed to start connector {}", connName, t);
            statusListener.onFailure(connName, t);
            return false;
        }

        connectors.put(connName, workerConnector);

        log.info("Finished creating connector {}", connName);
        return true;
    }

    /* Now that the configuration doesn't contain the actual class name, we need to be able to tell the herder whether a connector is a Sink */
    public boolean isSinkConnector(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        return workerConnector.isSinkConnector();
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

    public void stopConnectors() {
        stopConnectors(new HashSet<>(connectors.keySet()));
    }

    public Collection<String> stopConnectors(Collection<String> connectors) {
        final List<String> stopped = new ArrayList<>(connectors.size());
        for (String connector: connectors) {
            if (stopConnector(connector)) {
                stopped.add(connector);
            }
        }
        return stopped;
    }

    public boolean stopConnector(String connName) {
        log.info("Stopping connector {}", connName);

        WorkerConnector connector = connectors.get(connName);
        if (connector == null) {
            log.warn("Ignoring stop request for unowned connector {}", connName);
            return false;
        }

        connector.shutdown();
        connectors.remove(connName);

        log.info("Stopped connector {}", connName);
        return true;
    }

    /**
     * Get the IDs of the connectors currently running in this worker.
     */
    public Set<String> connectorNames() {
        return connectors.keySet();
    }

    public boolean isRunning(String connName) {
        WorkerConnector connector = connectors.get(connName);
        return connector != null && connector.isRunning();
    }

    public boolean startTask(
            ConnectorTaskId id,
            Map<String, String> connProps,
            Map<String, String> taskProps,
            TaskStatus.Listener statusListener,
            TargetState initialState
    ) {
        log.info("Creating task {}", id);

        if (tasks.containsKey(id))
            throw new ConnectException("Task already exists in this worker: " + id);

        final WorkerTask workerTask;
        try {
            final ConnectorConfig connConfig = new ConnectorConfig(connProps);
            final TaskConfig taskConfig = new TaskConfig(taskProps);

            final Class<? extends Task> taskClass = taskConfig.getClass(TaskConfig.TASK_CLASS_CONFIG).asSubclass(Task.class);
            final Task task = connectorFactory.newTask(taskClass);
            log.info("Instantiated task {} with version {} of type {}", id, task.version(), taskClass.getName());

            Converter keyConverter = connConfig.getConfiguredInstance(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Converter.class);
            if (keyConverter != null)
                keyConverter.configure(connConfig.originalsWithPrefix("key.converter."), true);
            else
                keyConverter = defaultKeyConverter;
            Converter valueConverter = connConfig.getConfiguredInstance(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Converter.class);
            if (valueConverter != null)
                valueConverter.configure(connConfig.originalsWithPrefix("value.converter."), false);
            else
                valueConverter = defaultValueConverter;

            workerTask = buildWorkerTask(id, task, statusListener, initialState, keyConverter, valueConverter);
            workerTask.initialize(taskConfig);
        } catch (Throwable t) {
            log.error("Failed to start task {}", id, t);
            statusListener.onFailure(id, t);
            return false;
        }

        executor.submit(workerTask);
        if (workerTask instanceof WorkerSourceTask) {
            sourceTaskOffsetCommitter.schedule(id, (WorkerSourceTask) workerTask);
        }
        tasks.put(id, workerTask);
        return true;
    }

    private WorkerTask buildWorkerTask(ConnectorTaskId id,
                                       Task task,
                                       TaskStatus.Listener statusListener,
                                       TargetState initialState,
                                       Converter keyConverter,
                                       Converter valueConverter) {
        // Decide which type of worker task we need based on the type of task.
        if (task instanceof SourceTask) {
            OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetBackingStore, id.connector(),
                    internalKeyConverter, internalValueConverter);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
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

    public boolean stopAndAwaitTask(ConnectorTaskId id) {
        return !stopAndAwaitTasks(Collections.singleton(id)).isEmpty();
    }

    public void stopAndAwaitTasks() {
        stopAndAwaitTasks(new HashSet<>(tasks.keySet()));
    }

    public Collection<ConnectorTaskId> stopAndAwaitTasks(Collection<ConnectorTaskId> ids) {
        final List<ConnectorTaskId> stoppable = new ArrayList<>(ids.size());
        for (ConnectorTaskId taskId : ids) {
            final WorkerTask task = tasks.get(taskId);
            if (task == null) {
                log.warn("Ignoring stop request for unowned task {}", taskId);
                continue;
            }
            stopTask(task);
            stoppable.add(taskId);
        }
        awaitStopTasks(stoppable);
        return stoppable;
    }

    private void stopTask(WorkerTask task) {
        log.info("Stopping task {}", task.id());
        if (task instanceof WorkerSourceTask)
            sourceTaskOffsetCommitter.remove(task.id());
        task.stop();
    }

    private void awaitStopTasks(Collection<ConnectorTaskId> ids) {
        long now = time.milliseconds();
        long deadline = now + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);
        for (ConnectorTaskId id : ids) {
            long remaining = Math.max(0, deadline - time.milliseconds());
            awaitStopTask(tasks.get(id), remaining);
        }
    }

    private void awaitStopTask(WorkerTask task, long timeout) {
        if (!task.awaitStop(timeout)) {
            log.error("Graceful stop of task {} failed.", task.id());
            task.cancel();
        }
        tasks.remove(task.id());
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

    public ConnectorFactory getConnectorFactory() {
        return connectorFactory;
    }

    public String workerId() {
        return workerId;
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
