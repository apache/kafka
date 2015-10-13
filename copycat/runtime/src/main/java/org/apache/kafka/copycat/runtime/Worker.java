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

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.cli.WorkerConfig;
import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.*;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

        Properties unusedConfigs = config.unusedProperties();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(config.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (String propName : unusedConfigs.stringPropertyNames()) {
            producerProps.put(propName, unusedConfigs.getProperty(propName));
        }
        producer = new KafkaProducer<>(producerProps);

        offsetBackingStore.start();
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(time, config);

        log.info("Worker started");
    }

    public void stop() {
        log.info("Worker stopping");

        long started = time.milliseconds();
        long limit = started + config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);

        for (Map.Entry<ConnectorTaskId, WorkerTask> entry : tasks.entrySet()) {
            WorkerTask task = entry.getValue();
            log.warn("Shutting down task {} uncleanly; herder should have shut down "
                    + "tasks before the Worker is stopped.", task);
            try {
                task.stop();
            } catch (CopycatException e) {
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

    /**
     * Add a new task.
     * @param id Globally unique ID for this task.
     * @param taskClassName name of the {@link org.apache.kafka.copycat.connector.Task}
     *                      class to instantiate. Must be a subclass of either
     *                      {@link org.apache.kafka.copycat.source.SourceTask} or
     *                      {@link org.apache.kafka.copycat.sink.SinkTask}.
     * @param props configuration options for the task
     */
    public void addTask(ConnectorTaskId id, String taskClassName, Properties props) {
        if (tasks.containsKey(id)) {
            String msg = "Task already exists in this worker; the herder should not have requested "
                    + "that this : " + id;
            log.error(msg);
            throw new CopycatException(msg);
        }

        final Task task = instantiateTask(taskClassName);

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
            throw new CopycatException("Tasks must be a subclass of either SourceTask or SinkTask");
        }

        // Start the task before adding modifying any state, any exceptions are caught higher up the
        // call chain and there's no cleanup to do here
        workerTask.start(props);
        tasks.put(id, workerTask);
    }

    private static Task instantiateTask(String taskClassName) {
        try {
            return Utils.newInstance(Class.forName(taskClassName).asSubclass(Task.class));
        } catch (ClassNotFoundException e) {
            throw new CopycatException("Task class not found", e);
        }
    }

    public void stopTask(ConnectorTaskId id) {
        WorkerTask task = getTask(id);
        task.stop();
        if (!task.awaitStop(config.getLong(WorkerConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG)))
            log.error("Graceful stop of task {} failed.", task);
        task.close();
        tasks.remove(id);
    }

    private WorkerTask getTask(ConnectorTaskId id) {
        WorkerTask task = tasks.get(id);
        if (task == null) {
            log.error("Task not found: " + id);
            throw new CopycatException("Task not found: " + id);
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
