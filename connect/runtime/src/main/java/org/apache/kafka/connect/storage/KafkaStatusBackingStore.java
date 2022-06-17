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
import org.apache.kafka.common.config.TopicConfig;
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
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.apache.kafka.connect.util.Table;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

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

    public static final String TASK_STATUS_PREFIX = "status-task-";
    public static final String CONNECTOR_STATUS_PREFIX = "status-connector-";
    public static final String TOPIC_STATUS_PREFIX = "status-topic-";
    public static final String TOPIC_STATUS_SEPARATOR = ":connector-";

    public static final String STATE_KEY_NAME = "state";
    public static final String TRACE_KEY_NAME = "trace";
    public static final String WORKER_ID_KEY_NAME = "worker_id";
    public static final String GENERATION_KEY_NAME = "generation";

    public static final String TOPIC_STATE_KEY = "topic";
    public static final String TOPIC_NAME_KEY = "name";
    public static final String TOPIC_CONNECTOR_KEY = "connector";
    public static final String TOPIC_TASK_KEY = "task";
    public static final String TOPIC_DISCOVER_TIMESTAMP_KEY = "discoverTimestamp";

    private static final Schema STATUS_SCHEMA_V0 = SchemaBuilder.struct()
            .field(STATE_KEY_NAME, Schema.STRING_SCHEMA)
            .field(TRACE_KEY_NAME, SchemaBuilder.string().optional().build())
            .field(WORKER_ID_KEY_NAME, Schema.STRING_SCHEMA)
            .field(GENERATION_KEY_NAME, Schema.INT32_SCHEMA)
            .build();

    private static final Schema TOPIC_STATUS_VALUE_SCHEMA_V0 = SchemaBuilder.struct()
            .field(TOPIC_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TOPIC_CONNECTOR_KEY, Schema.STRING_SCHEMA)
            .field(TOPIC_TASK_KEY, Schema.INT32_SCHEMA)
            .field(TOPIC_DISCOVER_TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .build();

    private static final Schema TOPIC_STATUS_SCHEMA_V0 = SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            TOPIC_STATUS_VALUE_SCHEMA_V0
    ).build();

    private final Time time;
    private final Converter converter;
    //visible for testing
    protected final Table<String, Integer, CacheEntry<TaskStatus>> tasks;
    protected final Map<String, CacheEntry<ConnectorStatus>> connectors;
    protected final ConcurrentMap<String, ConcurrentMap<String, TopicStatus>> topics;
    private final Supplier<TopicAdmin> topicAdminSupplier;

    private String statusTopic;
    private KafkaBasedLog<String, byte[]> kafkaLog;
    private int generation;
    private SharedTopicAdmin ownTopicAdmin;

    @Deprecated
    public KafkaStatusBackingStore(Time time, Converter converter) {
        this(time, converter, null);
    }

    public KafkaStatusBackingStore(Time time, Converter converter, Supplier<TopicAdmin> topicAdminSupplier) {
        this.time = time;
        this.converter = converter;
        this.tasks = new Table<>();
        this.connectors = new HashMap<>();
        this.topics = new ConcurrentHashMap<>();
        this.topicAdminSupplier = topicAdminSupplier;
    }

    // visible for testing
    KafkaStatusBackingStore(Time time, Converter converter, String statusTopic, KafkaBasedLog<String, byte[]> kafkaLog) {
        this(time, converter);
        this.kafkaLog = kafkaLog;
        this.statusTopic = statusTopic;
    }

    @Override
    public void configure(final WorkerConfig config) {
        this.statusTopic = config.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
        if (this.statusTopic == null || this.statusTopic.trim().length() == 0)
            throw new ConfigException("Must specify topic for connector status.");

        String clusterId = ConnectUtils.lookupKafkaClusterId(config);
        Map<String, Object> originals = config.originals();
        Map<String, Object> producerProps = new HashMap<>(originals);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // we handle retries in this class
        // By default, Connect disables idempotent behavior for all producers, even though idempotence became
        // default for Kafka producers. This is to ensure Connect continues to work with many Kafka broker versions, including older brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"); // disable idempotence since retries is force to 0
        ConnectUtils.addMetricsContextProperties(producerProps, config, clusterId);

        Map<String, Object> consumerProps = new HashMap<>(originals);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);

        Map<String, Object> adminProps = new HashMap<>(originals);
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);
        Supplier<TopicAdmin> adminSupplier;
        if (topicAdminSupplier != null) {
            adminSupplier = topicAdminSupplier;
        } else {
            // Create our own topic admin supplier that we'll close when we're stopped
            ownTopicAdmin = new SharedTopicAdmin(adminProps);
            adminSupplier = ownTopicAdmin;
        }

        Map<String, Object> topicSettings = config instanceof DistributedConfig
                                            ? ((DistributedConfig) config).statusStorageTopicSettings()
                                            : Collections.emptyMap();
        NewTopic topicDescription = TopicAdmin.defineTopic(statusTopic)
                .config(topicSettings) // first so that we override user-supplied settings as needed
                .compacted()
                .partitions(config.getInt(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG))
                .replicationFactor(config.getShort(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG))
                .build();

        Callback<ConsumerRecord<String, byte[]>> readCallback = (error, record) -> read(record);
        this.kafkaLog = createKafkaBasedLog(statusTopic, producerProps, consumerProps, readCallback, topicDescription, adminSupplier);
    }

    private KafkaBasedLog<String, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps,
                                                              Callback<ConsumerRecord<String, byte[]>> consumedCallback,
                                                              final NewTopic topicDescription, Supplier<TopicAdmin> adminSupplier) {
        java.util.function.Consumer<TopicAdmin> createTopics = admin -> {
            log.debug("Creating admin client to manage Connect internal status topic");
            // Create the topic if it doesn't exist
            Set<String> newTopics = admin.createTopics(topicDescription);
            if (!newTopics.contains(topic)) {
                // It already existed, so check that the topic cleanup policy is compact only and not delete
                log.debug("Using admin client to check cleanup policy of '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
                admin.verifyTopicCleanupPolicyOnlyCompact(topic,
                        DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connector and task statuses");
            }
        };
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, adminSupplier, consumedCallback, time, createTopics);
    }

    @Override
    public void start() {
        kafkaLog.start();

        // read to the end on startup to ensure that api requests see the most recent states
        kafkaLog.readToEnd();
    }

    @Override
    public void stop() {
        try {
            kafkaLog.stop();
        } finally {
            if (ownTopicAdmin != null) {
                ownTopicAdmin.close();
            }
        }
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
    public void put(final TopicStatus status) {
        sendTopicStatus(status.connector(), status.topic(), status);
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

    private void sendTopicStatus(final String connector, final String topic, final TopicStatus status) {
        String key = TOPIC_STATUS_PREFIX + topic + TOPIC_STATUS_SEPARATOR + connector;

        final byte[] value = serializeTopicStatus(status);

        kafkaLog.send(key, value, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) return;
                // TODO: retry more gracefully and not forever
                if (exception instanceof RetriableException) {
                    kafkaLog.send(key, value, this);
                } else {
                    log.error("Failed to write status update", exception);
                }
            }
        });
    }

    private <V extends AbstractStatus<?>> void send(final String key,
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

    private void removeTopic(String topic, String connector) {
        ConcurrentMap<String, TopicStatus> activeTopics = topics.get(connector);
        if (activeTopics == null) {
            return;
        }
        activeTopics.remove(topic);
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
    public TopicStatus getTopic(String connector, String topic) {
        ConcurrentMap<String, TopicStatus> activeTopics = topics.get(Objects.requireNonNull(connector));
        return activeTopics != null ? activeTopics.get(Objects.requireNonNull(topic)) : null;
    }

    @Override
    public Collection<TopicStatus> getAllTopics(String connector) {
        ConcurrentMap<String, TopicStatus> activeTopics = topics.get(Objects.requireNonNull(connector));
        return activeTopics != null
               ? Collections.unmodifiableCollection(Objects.requireNonNull(activeTopics.values()))
               : Collections.emptySet();
    }

    @Override
    public void deleteTopic(String connector, String topic) {
        sendTopicStatus(Objects.requireNonNull(connector), Objects.requireNonNull(topic), null);
    }

    @Override
    public synchronized Set<String> connectors() {
        return new HashSet<>(connectors.keySet());
    }

    private ConnectorStatus parseConnectorStatus(String connector, byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(statusTopic, data);
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
            SchemaAndValue schemaAndValue = converter.toConnectData(statusTopic, data);
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

    protected TopicStatus parseTopicStatus(byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(statusTopic, data);
            if (!(schemaAndValue.value() instanceof Map)) {
                log.error("Invalid topic status value {}", schemaAndValue.value());
                return null;
            }
            @SuppressWarnings("unchecked")
            Object innerValue = ((Map<String, Object>) schemaAndValue.value()).get(TOPIC_STATE_KEY);
            if (!(innerValue instanceof Map)) {
                log.error("Invalid topic status value {} for field {}", innerValue, TOPIC_STATE_KEY);
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> topicStatusMetadata = (Map<String, Object>) innerValue;
            return new TopicStatus((String) topicStatusMetadata.get(TOPIC_NAME_KEY),
                    (String) topicStatusMetadata.get(TOPIC_CONNECTOR_KEY),
                    ((Long) topicStatusMetadata.get(TOPIC_TASK_KEY)).intValue(),
                    (long) topicStatusMetadata.get(TOPIC_DISCOVER_TIMESTAMP_KEY));
        } catch (Exception e) {
            log.error("Failed to deserialize topic status", e);
            return null;
        }
    }

    private byte[] serialize(AbstractStatus<?> status) {
        Struct struct = new Struct(STATUS_SCHEMA_V0);
        struct.put(STATE_KEY_NAME, status.state().name());
        if (status.trace() != null)
            struct.put(TRACE_KEY_NAME, status.trace());
        struct.put(WORKER_ID_KEY_NAME, status.workerId());
        struct.put(GENERATION_KEY_NAME, status.generation());
        return converter.fromConnectData(statusTopic, STATUS_SCHEMA_V0, struct);
    }

    //visible for testing
    protected byte[] serializeTopicStatus(TopicStatus status) {
        if (status == null) {
            // This should send a tombstone record that will represent delete
            return null;
        }
        Struct struct = new Struct(TOPIC_STATUS_VALUE_SCHEMA_V0);
        struct.put(TOPIC_NAME_KEY, status.topic());
        struct.put(TOPIC_CONNECTOR_KEY, status.connector());
        struct.put(TOPIC_TASK_KEY, status.task());
        struct.put(TOPIC_DISCOVER_TIMESTAMP_KEY, status.discoverTimestamp());
        return converter.fromConnectData(
                statusTopic,
                TOPIC_STATUS_SCHEMA_V0,
                Collections.singletonMap(TOPIC_STATE_KEY, struct));
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
        if (connector.isEmpty()) {
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

    private void readTopicStatus(String key, byte[] value) {
        int delimiterPos = key.indexOf(':');
        int beginPos = TOPIC_STATUS_PREFIX.length();
        if (beginPos > delimiterPos) {
            log.warn("Discarding record with invalid topic status key {}", key);
            return;
        }

        String topic = key.substring(beginPos, delimiterPos);
        if (topic.isEmpty()) {
            log.warn("Discarding record with invalid topic status key containing empty topic {}", key);
            return;
        }

        beginPos = delimiterPos + TOPIC_STATUS_SEPARATOR.length();
        int endPos = key.length();
        if (beginPos > endPos) {
            log.warn("Discarding record with invalid topic status key {}", key);
            return;
        }

        String connector = key.substring(beginPos);
        if (connector.isEmpty()) {
            log.warn("Discarding record with invalid topic status key containing empty connector {}", key);
            return;
        }

        if (value == null) {
            log.trace("Removing status for topic {} and connector {}", topic, connector);
            removeTopic(topic, connector);
            return;
        }

        TopicStatus status = parseTopicStatus(value);
        if (status == null) {
            log.warn("Failed to parse topic status with key {}", key);
            return;
        }

        log.trace("Received topic status update {}", status);
        topics.computeIfAbsent(connector, k -> new ConcurrentHashMap<>())
                .put(topic, status);
    }

    // visible for testing
    void read(ConsumerRecord<String, byte[]> record) {
        String key = record.key();
        if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
            readConnectorStatus(key, record.value());
        } else if (key.startsWith(TASK_STATUS_PREFIX)) {
            readTaskStatus(key, record.value());
        } else if (key.startsWith(TOPIC_STATUS_PREFIX)) {
            readTopicStatus(key, record.value());
        } else {
            log.warn("Discarding record with invalid key {}", key);
        }
    }

    private static class CacheEntry<T extends AbstractStatus<?>> {
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
