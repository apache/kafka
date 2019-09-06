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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
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
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.Table;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * 1) It is (probably) safe to overwrite the state if there is no previous
 *    value.
 * 2) It is (probably) safe to overwrite the state if the previous value was
 *    set by a worker with the same workerId.
 * 3) It is (probably) safe to overwrite the previous state if the current
 *    generation is higher than the previous .
 *
 * Basically all these conditions do is reduce the window for conflicts. They
 * obviously cannot take into account in-flight requests.
 *
 */
public class KafkaStatusBackingStore implements StatusBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaStatusBackingStore.class);

    private static final String TASK_STATUS_PREFIX = "status-task-";
    private static final String CONNECTOR_STATUS_PREFIX = "status-connector-";

    public static final String STATE_KEY_NAME = "state";
    public static final String TRACE_KEY_NAME = "trace";
    public static final String WORKER_ID_KEY_NAME = "worker_id";
    public static final String GENERATION_KEY_NAME = "generation";

    private static final Schema STATUS_SCHEMA_V0 = SchemaBuilder.struct()
            .field(STATE_KEY_NAME, Schema.STRING_SCHEMA)
            .field(TRACE_KEY_NAME, SchemaBuilder.string().optional().build())
            .field(WORKER_ID_KEY_NAME, Schema.STRING_SCHEMA)
            .field(GENERATION_KEY_NAME, Schema.INT32_SCHEMA)
            .build();

    private final Time time;
    private final Converter converter;
    private final Table<String, Integer, CacheEntry<TaskStatus>> tasks;
    private final Map<String, CacheEntry<ConnectorStatus>> connectors;

    private String topic;
    private KafkaBasedLog<String, byte[]> kafkaLog;
    private int generation;

    public KafkaStatusBackingStore(Time time, Converter converter) {
        this.time = time;
        this.converter = converter;
        this.tasks = new Table<>();
        this.connectors = new HashMap<>();
    }

    // visible for testing
    KafkaStatusBackingStore(Time time, Converter converter, String topic, KafkaBasedLog<String, byte[]> kafkaLog) {
        this(time, converter);
        this.kafkaLog = kafkaLog;
        this.topic = topic;
    }

    @Override
    public void configure(final WorkerConfig config) {
        this.topic = config.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
        if (this.topic == null || this.topic.trim().length() == 0)
            throw new ConfigException("Must specify topic for connector status.");

        Map<String, Object> originals = config.originals();
        Map<String, Object> producerProps = new HashMap<>(originals);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // we handle retries in this class

        Map<String, Object> consumerProps = new HashMap<>(originals);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        Map<String, Object> adminProps = new HashMap<>(originals);
        NewTopic topicDescription = TopicAdmin.defineTopic(topic).
                compacted().
                partitions(config.getInt(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG)).
                replicationFactor(config.getShort(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG)).
                build();

        Callback<ConsumerRecord<String, byte[]>> readCallback = new Callback<ConsumerRecord<String, byte[]>>() {
            @Override
            public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> record) {
                read(record);
            }
        };
        this.kafkaLog = createKafkaBasedLog(topic, producerProps, consumerProps, readCallback, topicDescription, adminProps);
    }

    private KafkaBasedLog<String, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps,
                                                              Callback<ConsumerRecord<String, byte[]>> consumedCallback,
                                                              final NewTopic topicDescription, final Map<String, Object> adminProps) {
        Runnable createTopics = new Runnable() {
            @Override
            public void run() {
                log.debug("Creating admin client to manage Connect internal status topic");
                try (TopicAdmin admin = new TopicAdmin(adminProps)) {
                    admin.createTopics(topicDescription);
                }
            }
        };
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, consumedCallback, time, createTopics);
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
    public void put(final ConnectorStatus status) {
        sendConnectorStatus(status, false);
    }

    @Override
    public void putSafe(final ConnectorStatus status) {
        sendConnectorStatus(status, true);
    }

    @Override
    public void put(final TaskStatus status) {
        sendTaskStatus(status, false);
    }

    @Override
    public void putSafe(final TaskStatus status) {
        sendTaskStatus(status, true);
    }

    @Override
    public void flush() {
        kafkaLog.flush();
    }

    private void sendConnectorStatus(final ConnectorStatus status, boolean safeWrite) {
        String connector = status.id();
        CacheEntry<ConnectorStatus> entry = getOrAdd(connector);
        String key = CONNECTOR_STATUS_PREFIX + connector;
        send(key, status, entry, safeWrite);
    }

    private void sendTaskStatus(final TaskStatus status, boolean safeWrite) {
        ConnectorTaskId taskId = status.id();
        CacheEntry<TaskStatus> entry = getOrAdd(taskId);
        String key = TASK_STATUS_PREFIX + taskId.connector() + "-" + taskId.task();
        send(key, status, entry, safeWrite);
    }

    private <V extends AbstractStatus> void send(final String key,
                                                 final V status,
                                                 final CacheEntry<V> entry,
                                                 final boolean safeWrite) {
        final int sequence;
        synchronized (this) {
            this.generation = status.generation();
            if (safeWrite && !entry.canWriteSafely(status))
                return;
            sequence = entry.increment();
        }

        final byte[] value = status.state() == ConnectorStatus.State.DESTROYED ? null : serialize(status);

        kafkaLog.send(key, value, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) return;
                if (exception instanceof RetriableException) {
                    synchronized (KafkaStatusBackingStore.this) {
                        if (entry.isDeleted()
                            || status.generation() != generation
                            || (safeWrite && !entry.canWriteSafely(status, sequence)))
                            return;
                    }
                    kafkaLog.send(key, value, this);
                } else {
                    log.error("Failed to write status update", exception);
                }
            }
        });
    }

    private synchronized CacheEntry<ConnectorStatus> getOrAdd(String connector) {
        CacheEntry<ConnectorStatus> entry = connectors.get(connector);
        if (entry == null) {
            entry = new CacheEntry<>();
            connectors.put(connector, entry);
        }
        return entry;
    }

    private synchronized void remove(String connector) {
        CacheEntry<ConnectorStatus> removed = connectors.remove(connector);
        if (removed != null)
            removed.delete();

        Map<Integer, CacheEntry<TaskStatus>> tasks = this.tasks.remove(connector);
        if (tasks != null) {
            for (CacheEntry<TaskStatus> taskEntry : tasks.values())
                taskEntry.delete();
        }
    }

    private synchronized CacheEntry<TaskStatus> getOrAdd(ConnectorTaskId task) {
        CacheEntry<TaskStatus> entry = tasks.get(task.connector(), task.task());
        if (entry == null) {
            entry = new CacheEntry<>();
            tasks.put(task.connector(), task.task(), entry);
        }
        return entry;
    }

    private synchronized void remove(ConnectorTaskId id) {
        CacheEntry<TaskStatus> removed = tasks.remove(id.connector(), id.task());
        if (removed != null)
            removed.delete();
    }

    @Override
    public synchronized TaskStatus get(ConnectorTaskId id) {
        CacheEntry<TaskStatus> entry = tasks.get(id.connector(), id.task());
        return entry == null ? null : entry.get();
    }

    @Override
    public synchronized ConnectorStatus get(String connector) {
        CacheEntry<ConnectorStatus> entry = connectors.get(connector);
        return entry == null ? null : entry.get();
    }

    @Override
    public synchronized Collection<TaskStatus> getAll(String connector) {
        List<TaskStatus> res = new ArrayList<>();
        for (CacheEntry<TaskStatus> statusEntry : tasks.row(connector).values()) {
            TaskStatus status = statusEntry.get();
            if (status != null)
                res.add(status);
        }
        return res;
    }

    @Override
    public synchronized Set<String> connectors() {
        return new HashSet<>(connectors.keySet());
    }

    private ConnectorStatus parseConnectorStatus(String connector, byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, data);
            if (!(schemaAndValue.value() instanceof Map)) {
                log.error("Invalid connector status type {}", schemaAndValue.value().getClass());
                return null;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> statusMap = (Map<String, Object>) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) statusMap.get(STATE_KEY_NAME));
            String trace = (String) statusMap.get(TRACE_KEY_NAME);
            String workerUrl = (String) statusMap.get(WORKER_ID_KEY_NAME);
            int generation = ((Long) statusMap.get(GENERATION_KEY_NAME)).intValue();
            return new ConnectorStatus(connector, state, trace, workerUrl, generation);
        } catch (Exception e) {
            log.error("Failed to deserialize connector status", e);
            return null;
        }
    }

    private TaskStatus parseTaskStatus(ConnectorTaskId taskId, byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, data);
            if (!(schemaAndValue.value() instanceof Map)) {
                log.error("Invalid task status type {}", schemaAndValue.value().getClass());
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> statusMap = (Map<String, Object>) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) statusMap.get(STATE_KEY_NAME));
            String trace = (String) statusMap.get(TRACE_KEY_NAME);
            String workerUrl = (String) statusMap.get(WORKER_ID_KEY_NAME);
            int generation = ((Long) statusMap.get(GENERATION_KEY_NAME)).intValue();
            return new TaskStatus(taskId, state, workerUrl, generation, trace);
        } catch (Exception e) {
            log.error("Failed to deserialize task status", e);
            return null;
        }
    }

    private byte[] serialize(AbstractStatus status) {
        Struct struct = new Struct(STATUS_SCHEMA_V0);
        struct.put(STATE_KEY_NAME, status.state().name());
        if (status.trace() != null)
            struct.put(TRACE_KEY_NAME, status.trace());
        struct.put(WORKER_ID_KEY_NAME, status.workerId());
        struct.put(GENERATION_KEY_NAME, status.generation());
        return converter.fromConnectData(topic, STATUS_SCHEMA_V0, struct);
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

    private void readConnectorStatus(String key, byte[] value) {
        String connector = parseConnectorStatusKey(key);
        if (connector == null || connector.isEmpty()) {
            log.warn("Discarding record with invalid connector status key {}", key);
            return;
        }

        if (value == null) {
            log.trace("Removing status for connector {}", connector);
            remove(connector);
            return;
        }

        ConnectorStatus status = parseConnectorStatus(connector, value);
        if (status == null)
            return;

        synchronized (this) {
            log.trace("Received connector {} status update {}", connector, status);
            CacheEntry<ConnectorStatus> entry = getOrAdd(connector);
            entry.put(status);
        }
    }

    private void readTaskStatus(String key, byte[] value) {
        ConnectorTaskId id = parseConnectorTaskId(key);
        if (id == null) {
            log.warn("Discarding record with invalid task status key {}", key);
            return;
        }

        if (value == null) {
            log.trace("Removing task status for {}", id);
            remove(id);
            return;
        }

        TaskStatus status = parseTaskStatus(id, value);
        if (status == null) {
            log.warn("Failed to parse task status with key {}", key);
            return;
        }

        synchronized (this) {
            log.trace("Received task {} status update {}", id, status);
            CacheEntry<TaskStatus> entry = getOrAdd(id);
            entry.put(status);
        }
    }

    // visible for testing
    void read(ConsumerRecord<String, byte[]> record) {
        String key = record.key();
        if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
            readConnectorStatus(key, record.value());
        } else if (key.startsWith(TASK_STATUS_PREFIX)) {
            readTaskStatus(key, record.value());
        } else {
            log.warn("Discarding record with invalid key {}", key);
        }
    }

    private static class CacheEntry<T extends AbstractStatus> {
        private T value = null;
        private int sequence = 0;
        private boolean deleted = false;

        public int increment() {
            return ++sequence;
        }

        public void put(T value) {
            this.value = value;
        }

        public T get() {
            return value;
        }

        public void delete() {
            this.deleted = true;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public boolean canWriteSafely(T status) {
            return value == null
                    || value.workerId().equals(status.workerId())
                    || value.generation() <= status.generation();
        }

        public boolean canWriteSafely(T status, int sequence) {
            return canWriteSafely(status) && this.sequence == sequence;
        }

    }

}
