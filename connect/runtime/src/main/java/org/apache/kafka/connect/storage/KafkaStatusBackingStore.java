/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * StatusBackingStore implementation which uses a compacted topic for storage
 * of connector and task status information. When a state change is observed,
 * the new state is written to the compacted topic. The new state will not be
 * visible until it has been read back from the topic.
 *
 * In spite of their names, the putSafe() methods cannot guarantee the safety
 * of the write (since Kafka itself cannot provide such guarantees currently),
 * but it can avoid specific unsafe conditions. In particular, we putSafe()
 * allows writes in the following conditions:
 *
 * 3) It is (probably) safe to overwrite the state if there is no previous
 *    value.
 * 1) It is (probably) safe to overwrite the state if the previous value was
 *    set by a worker with the same workerId.
 * 2) It is (probably) safe to overwrite the previous state if the current
 *    generation is higher than the previous .
 *
 * Basically all these conditions do is reduce the window for conflicts. They
 * obviously cannot take into account in-flight requests.
 *
 */
public class KafkaStatusBackingStore implements StatusBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaStatusBackingStore.class);

    public static final String STATUS_TOPIC_CONFIG = "status.storage.topic";

    private static final String TASK_STATUS_PREFIX = "status-task-";
    private static final String CONNECTOR_STATUS_PREFIX = "status-connector-";

    public static final String STATE_KEY_NAME = "state";
    public static final String MSG_KEY_NAME = "msg";
    public static final String WORKER_URL_KEY_NAME = "worker_id";
    public static final String GENERATION_KEY_NAME = "generation";

    private static final Schema TASK_STATUS_SCHEMA_V0 = SchemaBuilder.struct()
            .field(STATE_KEY_NAME, Schema.STRING_SCHEMA)
            .field(MSG_KEY_NAME, SchemaBuilder.string().optional().build())
            .field(WORKER_URL_KEY_NAME, Schema.STRING_SCHEMA)
            .field(GENERATION_KEY_NAME, Schema.INT32_SCHEMA)
            .build();
    private static final Schema CONNECTOR_STATUS_SCHEMA_V0 = TASK_STATUS_SCHEMA_V0;

    private final Time time;
    private final Converter converter;
    private final Table<String, Integer, TaskEntry> tasks;
    private final Map<String, ConnectorEntry> connectors;

    private String topic;
    private KafkaBasedLog<String, byte[]> kafkaLog;
    private int generation;

    public KafkaStatusBackingStore(Time time, Converter converter) {
        this.time = time;
        this.converter = converter;
        this.tasks = new Table<>();
        this.connectors = new HashMap<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.get(STATUS_TOPIC_CONFIG) == null)
            throw new ConnectException("Must specify topic for connector status.");
        this.topic = (String) configs.get(STATUS_TOPIC_CONFIG);

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(configs);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(configs);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        this.kafkaLog = new KafkaBasedLog<>(topic, producerProps, consumerProps, new ReadCallback(), time);
    }

    @Override
    public void start() {
        kafkaLog.start();

        // read to the end on startup to ensure that api requests see the most recent states
        kafkaLog.readToEnd();
    }

    @Override
    public void stop() {
        kafkaLog.stop();
    }

    @Override
    public void put(final String connector, final ConnectorStatus status) {
        sendConnectorStatus(connector, status, false);
    }

    @Override
    public void putSafe(final String connector, final ConnectorStatus status) {
        sendConnectorStatus(connector, status, true);
    }

    @Override
    public void put(final ConnectorTaskId task, final TaskStatus status) {
        sendTaskStatus(task, status, false);
    }

    @Override
    public void putSafe(final ConnectorTaskId task, final TaskStatus status) {
        sendTaskStatus(task, status, true);
    }

    @Override
    public void flush() {
        kafkaLog.flush();
    }

    private void sendConnectorStatus(final String connector,
                                     final ConnectorStatus status,
                                     boolean safeWrite) {
        CacheEntry<String, ConnectorStatus> entry = getOrAdd(connector);
        send(connector, status, entry, safeWrite);
    }

    private void sendTaskStatus(final ConnectorTaskId task,
                                final TaskStatus status,
                                boolean safeWrite) {
        CacheEntry<ConnectorTaskId, TaskStatus> entry = getOrAdd(task);
        send(task, status, entry, safeWrite);
    }

    private <K, V extends AbstractStatus> void send(K id,
                                                    final V status,
                                                    final CacheEntry<K, V> entry,
                                                    final boolean safeWrite) {
        final int sequence;
        synchronized (this) {
            this.generation = status.generation();
            if (safeWrite && !entry.canWrite(status))
                return;
            sequence = entry.increment();
        }

        final String key = entry.serializeKey(id);
        final byte[] value = status.state() == ConnectorStatus.State.DESTROYED ? null : entry.serializeValue(status);

        kafkaLog.send(key, value, new AbstractWriteCallback() {
            @Override
            protected void retry() {
                synchronized (this) {
                    if (entry.isDeleted()
                            || status.generation() != generation
                            || (safeWrite && !entry.canWrite(status, sequence)))
                        return;
                }
                kafkaLog.send(key, value, this);
            }
        });
    }

    private synchronized ConnectorEntry getOrAdd(String connector) {
        ConnectorEntry entry = connectors.get(connector);
        if (entry == null) {
            entry = new ConnectorEntry();
            connectors.put(connector, entry);
        }
        return entry;
    }

    private synchronized void remove(String connector) {
        ConnectorEntry removed = connectors.remove(connector);
        if (removed != null)
            removed.delete();

        Map<Integer, TaskEntry> tasks = this.tasks.remove(connector);
        if (tasks != null) {
            for (TaskEntry taskEntry : tasks.values())
                taskEntry.delete();
        }
    }

    private synchronized TaskEntry getOrAdd(ConnectorTaskId task) {
        TaskEntry entry = tasks.get(task.connector(), task.task());
        if (entry == null) {
            entry = new TaskEntry();
            tasks.put(task.connector(), task.task(), entry);
        }
        return entry;
    }

    private synchronized void remove(ConnectorTaskId id) {
        TaskEntry removed = tasks.remove(id.connector(), id.task());
        if (removed != null)
            removed.delete();
    }

    @Override
    public synchronized TaskStatus get(ConnectorTaskId id) {
        TaskEntry entry = tasks.get(id.connector(), id.task());
        return entry == null ? null : entry.get();
    }

    @Override
    public synchronized ConnectorStatus get(String connector) {
        ConnectorEntry entry = connectors.get(connector);
        return entry == null ? null : entry.get();
    }

    @Override
    public synchronized Map<Integer, TaskStatus> getAll(String connector) {
        Map<Integer, TaskStatus> res = new HashMap<>();
        for (Map.Entry<Integer, TaskEntry> statusEntry : tasks.row(connector).entrySet()) {
            int task = statusEntry.getKey();
            TaskStatus status = statusEntry.getValue().get();
            if (status != null)
                res.put(task, status);
        }
        return res;
    }

    @Override
    public synchronized Set<String> connectors() {
        return new HashSet<>(connectors.keySet());
    }

    private ConnectorStatus parseConnectorStatus(byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, data);
            if (!(schemaAndValue.value() instanceof Map)) {
                log.error("Invalid connector status type {}", schemaAndValue.value().getClass());
                return null;
            }

            Map<String, Object> statusMap = (Map<String, Object>) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) statusMap.get(STATE_KEY_NAME));
            String msg = (String) statusMap.get(MSG_KEY_NAME);
            String workerUrl = (String) statusMap.get(WORKER_URL_KEY_NAME);
            int generation = ((Long) statusMap.get(GENERATION_KEY_NAME)).intValue();
            return new ConnectorStatus(state, msg, workerUrl, generation);
        } catch (Exception e) {
            log.error("Failed to deserialize connector status", e);
            return null;
        }
    }

    private TaskStatus parseTaskStatus(byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, data);
            if (!(schemaAndValue.value() instanceof Map)) {
                log.error("Invalid connector status type {}", schemaAndValue.value().getClass());
                return null;
            }
            Map<String, Object> statusMap = (Map<String, Object>) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) statusMap.get(STATE_KEY_NAME));
            String msg = (String) statusMap.get(MSG_KEY_NAME);
            String workerUrl = (String) statusMap.get(WORKER_URL_KEY_NAME);
            int generation = ((Long) statusMap.get(GENERATION_KEY_NAME)).intValue();
            return new TaskStatus(state, msg, workerUrl, generation);
        } catch (Exception e) {
            log.error("Failed to deserialize connector status", e);
            return null;
        }
    }

    private byte[] fromConnect(TaskStatus status) {
        Struct struct = new Struct(TASK_STATUS_SCHEMA_V0);
        struct.put(STATE_KEY_NAME, status.state().name());
        if (status.msg() != null)
            struct.put(MSG_KEY_NAME, status.msg());
        struct.put(WORKER_URL_KEY_NAME, status.workerId());
        struct.put(GENERATION_KEY_NAME, status.generation());
        return converter.fromConnectData(topic, TASK_STATUS_SCHEMA_V0, struct);
    }

    private byte[] fromConnect(ConnectorStatus status) {
        Struct struct = new Struct(CONNECTOR_STATUS_SCHEMA_V0);
        struct.put(STATE_KEY_NAME, status.state().name());
        if (status.msg() != null)
            struct.put(MSG_KEY_NAME, status.msg());
        struct.put(WORKER_URL_KEY_NAME, status.workerId());
        struct.put(GENERATION_KEY_NAME, status.generation());
        return converter.fromConnectData(topic, CONNECTOR_STATUS_SCHEMA_V0, struct);
    }

    private String parseConnectorStatusKey(String key) {
        return key.substring(CONNECTOR_STATUS_PREFIX.length());
    }

    private ConnectorTaskId parseConnectorTaskId(String key) {
        String[] parts = key.split("-");
        if (parts.length < 4) return null;

        try {
            int taskNum = Integer.parseInt(parts[parts.length - 1]);
            String connectorName = Utils.join(Arrays.copyOfRange(parts, 2, parts.length - 1), "-");
            return new ConnectorTaskId(connectorName, taskNum);
        } catch (NumberFormatException e) {
            log.warn("Invalid task status key {}", key);
            return null;
        }
    }

    private abstract class AbstractWriteCallback implements org.apache.kafka.clients.producer.Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (exception instanceof RetriableException)
                    retry();
                else
                    log.error("Failed to write status update", exception);
            }
        }
        protected abstract void retry();
    }

    private class ReadCallback implements Callback<ConsumerRecord<String, byte[]>> {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> result) {
            String key = result.key();
            if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
                String connector = parseConnectorStatusKey(key);
                if (connector == null || connector.isEmpty()) {
                    log.warn("Discarding record with invalid connector status key {}", key);
                    return;
                }

                if (result.value() == null) {
                    log.trace("Removing status for connector {}", connector);
                    remove(connector);
                    return;
                }

                ConnectorStatus status = parseConnectorStatus(result.value());
                if (status == null)
                    return;

                synchronized (KafkaStatusBackingStore.this) {
                    log.trace("Received connector {} status update {}", connector, status);
                    ConnectorEntry entry = getOrAdd(connector);
                    entry.put(status);
                }
            } else if (key.startsWith(TASK_STATUS_PREFIX)) {
                ConnectorTaskId id = parseConnectorTaskId(key);
                if (id == null) {
                    log.warn("Discarding record with invalid task status key {}", key);
                    return;
                }

                if (result.value() == null) {
                    log.trace("Removing task status for {}", id);
                    remove(id);
                    return;
                }

                TaskStatus status = parseTaskStatus(result.value());
                if (status == null) {
                    log.warn("Failed to parse task status with key {}", key);
                    return;
                }

                synchronized (KafkaStatusBackingStore.this) {
                    log.trace("Received task {} status update {}", id, status);
                    TaskEntry entry = getOrAdd(id);
                    entry.put(status);
                }
            } else {
                log.warn("Discarding record with invalid key {}", key);
            }
        }
    }

    private class TaskEntry extends CacheEntry<ConnectorTaskId, TaskStatus> {

        @Override
        protected String serializeKey(ConnectorTaskId taskId) {
            return TASK_STATUS_PREFIX + taskId.connector() + "-" + taskId.task();
        }

        @Override
        protected byte[] serializeValue(TaskStatus status) {
            return fromConnect(status);
        }
    }

    private class ConnectorEntry extends CacheEntry<String, ConnectorStatus> {

        @Override
        protected String serializeKey(String connector) {
            return CONNECTOR_STATUS_PREFIX + connector;
        }

        @Override
        protected byte[] serializeValue(ConnectorStatus status) {
            return fromConnect(status);
        }
    }

    private abstract static class CacheEntry<K, V extends AbstractStatus> {
        private V value = null;
        private int sequence = 0;
        private boolean deleted = false;

        public int increment() {
            return ++sequence;
        }

        public void put(V value) {
            this.value = value;
        }

        public V get() {
            return value;
        }

        public void delete() {
            this.deleted = true;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public boolean canWrite(V status) {
            return value != null &&
                    (value.workerId().equals(status.workerId())
                    || value.generation() <= status.generation());
        }

        public boolean canWrite(V status, int sequence) {
            return canWrite(status) && this.sequence == sequence;
        }

        protected abstract String serializeKey(K key);

        protected abstract byte[] serializeValue(V value);
    }

}
