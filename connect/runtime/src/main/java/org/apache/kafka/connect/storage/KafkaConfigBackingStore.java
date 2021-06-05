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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * <p>
 * Provides persistent storage of Kafka Connect connector configurations in a Kafka topic.
 * </p>
 * <p>
 * This class manages both connector and task configurations. It tracks three types of configuration entries:
 * <p/>
 * 1. Connector config: map of string -> string configurations passed to the Connector class, with support for
 * expanding this format if necessary. (Kafka key: connector-[connector-id]).
 * These configs are *not* ephemeral. They represent the source of truth. If the entire Connect
 * cluster goes down, this is all that is really needed to recover.
 * 2. Task configs: map of string -> string configurations passed to the Task class, with support for expanding
 * this format if necessary. (Kafka key: task-[connector-id]-[task-id]).
 * These configs are ephemeral; they are stored here to a) disseminate them to all workers while
 * ensuring agreement and b) to allow faster cluster/worker recovery since the common case
 * of recovery (restoring a connector) will simply result in the same configuration as before
 * the failure.
 * 3. Task commit "configs": records indicating that previous task config entries should be committed and all task
 * configs for a connector can be applied. (Kafka key: commit-[connector-id].
 * This config has two effects. First, it records the number of tasks the connector is currently
 * running (and can therefore increase/decrease parallelism). Second, because each task config
 * is stored separately but they need to be applied together to ensure each partition is assigned
 * to a single task, this record also indicates that task configs for the specified connector
 * can be "applied" or "committed".
 * </p>
 * <p>
 * This configuration is expected to be stored in a *single partition* and *compacted* topic. Using a single partition
 * ensures we can enforce ordering on messages, allowing Kafka to be used as a write ahead log. Compaction allows
 * us to clean up outdated configurations over time. However, this combination has some important implications for
 * the implementation of this class and the configuration state that it may expose.
 * </p>
 * <p>
 * Connector configurations are independent of all other configs, so they are handled easily. Writing a single record
 * is already atomic, so these can be applied as soon as they are read. One connectors config does not affect any
 * others, and they do not need to coordinate with the connector's task configuration at all.
 * </p>
 * <p>
 * The most obvious implication for task configs is the need for the commit messages. Because Kafka does not
 * currently have multi-record transactions or support atomic batch record writes, task commit messages are required
 * to ensure that readers do not end up using inconsistent configs. For example, consider if a connector wrote configs
 * for its tasks, then was reconfigured and only managed to write updated configs for half its tasks. If task configs
 * were applied immediately you could be using half the old configs and half the new configs. In that condition, some
 * partitions may be double-assigned because the old config and new config may use completely different assignments.
 * Therefore, when reading the log, we must buffer config updates for a connector's tasks and only apply atomically them
 * once a commit message has been read.
 * </p>
 * <p>
 * However, there are also further challenges. This simple buffering approach would work fine as long as the entire log was
 * always available, but we would like to be able to enable compaction so our configuration topic does not grow
 * indefinitely. Compaction may break a normal log because old entries will suddenly go missing. A new worker reading
 * from the beginning of the log in order to build up the full current configuration will see task commits, but some
 * records required for those commits will have been removed because the same keys have subsequently been rewritten.
 * For example, if you have a sequence of record keys [connector-foo-config, task-foo-1-config, task-foo-2-config,
 * commit-foo (2 tasks), task-foo-1-config, commit-foo (1 task)], we can end up with a compacted log containing
 * [connector-foo-config, task-foo-2-config, commit-foo (2 tasks), task-foo-1-config, commit-foo (1 task)]. When read
 * back, the first commit will see an invalid state because the first task-foo-1-config has been cleaned up.
 * </p>
 * <p>
 * Compaction can further complicate things if writing new task configs fails mid-write. Consider a similar scenario
 * as the previous one, but in this case both the first and second update will write 2 task configs. However, the
 * second write fails half of the way through:
 * [connector-foo-config, task-foo-1-config, task-foo-2-config, commit-foo (2 tasks), task-foo-1-config]. Now compaction
 * occurs and we're left with
 * [connector-foo-config, task-foo-2-config, commit-foo (2 tasks), task-foo-1-config]. At the first commit, we don't
 * have a complete set of configs. And because of the failure, there is no second commit. We are left in an inconsistent
 * state with no obvious way to resolve the issue -- we can try to keep on reading, but the failed node may never
 * recover and write the updated config. Meanwhile, other workers may have seen the entire log; they will see the second
 * task-foo-1-config waiting to be applied, but will otherwise think everything is ok -- they have a valid set of task
 * configs for connector "foo".
 * </p>
 * <p>
 * Because we can encounter these inconsistencies and addressing them requires support from the rest of the system
 * (resolving the task configuration inconsistencies requires support from the connector instance to regenerate updated
 * configs), this class exposes not only the current set of configs, but also which connectors have inconsistent data.
 * This allows users of this class (i.e., Herder implementations) to take action to resolve any inconsistencies. These
 * inconsistencies should be rare (as described above, due to compaction combined with leader failures in the middle
 * of updating task configurations).
 * </p>
 * <p>
 * Note that the expectation is that this config storage system has only a single writer at a time.
 * The caller (Herder) must ensure this is the case. In distributed mode this will require forwarding config change
 * requests to the leader in the cluster (i.e. the worker group coordinated by the Kafka broker).
 * </p>
 * <p>
 * Since processing of the config log occurs in a background thread, callers must take care when using accessors.
 * To simplify handling this correctly, this class only exposes a mechanism to snapshot the current state of the cluster.
 * Updates may continue to be applied (and callbacks invoked) in the background. Callers must take care that they are
 * using a consistent snapshot and only update when it is safe. In particular, if task configs are updated which require
 * synchronization across workers to commit offsets and update the configuration, callbacks and updates during the
 * rebalance must be deferred.
 * </p>
 */
public class KafkaConfigBackingStore implements ConfigBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigBackingStore.class);

    public static final String TARGET_STATE_PREFIX = "target-state-";

    public static String TARGET_STATE_KEY(String connectorName) {
        return TARGET_STATE_PREFIX + connectorName;
    }

    public static final String CONNECTOR_PREFIX = "connector-";

    public static String CONNECTOR_KEY(String connectorName) {
        return CONNECTOR_PREFIX + connectorName;
    }

    public static final String TASK_PREFIX = "task-";

    public static String TASK_KEY(ConnectorTaskId taskId) {
        return TASK_PREFIX + taskId.connector() + "-" + taskId.task();
    }

    public static final String COMMIT_TASKS_PREFIX = "commit-";

    public static String COMMIT_TASKS_KEY(String connectorName) {
        return COMMIT_TASKS_PREFIX + connectorName;
    }

    public static final String SESSION_KEY_KEY = "session-key";

    // Note that while using real serialization for values as we have here, but ad hoc string serialization for keys,
    // isn't ideal, we use this approach because it avoids any potential problems with schema evolution or
    // converter/serializer changes causing keys to change. We need to absolutely ensure that the keys remain precisely
    // the same.
    public static final Schema CONNECTOR_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build())
            .build();
    public static final Schema TASK_CONFIGURATION_V0 = CONNECTOR_CONFIGURATION_V0;
    public static final Schema CONNECTOR_TASKS_COMMIT_V0 = SchemaBuilder.struct()
            .field("tasks", Schema.INT32_SCHEMA)
            .build();
    public static final Schema TARGET_STATE_V0 = SchemaBuilder.struct()
            .field("state", Schema.STRING_SCHEMA)
            .build();
    // The key is logically a byte array, but we can't use the JSON converter to (de-)serialize that without a schema.
    // So instead, we base 64-encode it before serializing and decode it after deserializing.
    public static final Schema SESSION_KEY_V0 = SchemaBuilder.struct()
            .field("key", Schema.STRING_SCHEMA)
            .field("algorithm", Schema.STRING_SCHEMA)
            .field("creation-timestamp", Schema.INT64_SCHEMA)
            .build();

    private static final long READ_TO_END_TIMEOUT_MS = 30000;

    private final Object lock;
    private final Converter converter;
    private volatile boolean started;
    // Although updateListener is not final, it's guaranteed to be visible to any thread after its
    // initialization as long as we always read the volatile variable "started" before we access the listener.
    private UpdateListener updateListener;

    private final String topic;
    // Data is passed to the log already serialized. We use a converter to handle translating to/from generic Connect
    // format to serialized form
    private final KafkaBasedLog<String, byte[]> configLog;
    // Connector -> # of tasks
    private final Map<String, Integer> connectorTaskCounts = new HashMap<>();
    // Connector and task configs: name or id -> config map
    private final Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
    private final Map<ConnectorTaskId, Map<String, String>> taskConfigs = new HashMap<>();
    private final Supplier<TopicAdmin> topicAdminSupplier;
    private SharedTopicAdmin ownTopicAdmin;

    // Set of connectors where we saw a task commit with an incomplete set of task config updates, indicating the data
    // is in an inconsistent state and we cannot safely use them until they have been refreshed.
    private final Set<String> inconsistent = new HashSet<>();
    // The most recently read offset. This does not take into account deferred task updates/commits, so we may have
    // outstanding data to be applied.
    private volatile long offset;
    // The most recently read session key, to use for validating internal REST requests.
    private volatile SessionKey sessionKey;

    // Connector -> Map[ConnectorTaskId -> Configs]
    private final Map<String, Map<ConnectorTaskId, Map<String, String>>> deferredTaskUpdates = new HashMap<>();

    private final Map<String, TargetState> connectorTargetStates = new HashMap<>();

    private final WorkerConfigTransformer configTransformer;

    @Deprecated
    public KafkaConfigBackingStore(Converter converter, WorkerConfig config, WorkerConfigTransformer configTransformer) {
        this(converter, config, configTransformer, null);
    }

    public KafkaConfigBackingStore(Converter converter, WorkerConfig config, WorkerConfigTransformer configTransformer, Supplier<TopicAdmin> adminSupplier) {
        this.lock = new Object();
        this.started = false;
        this.converter = converter;
        this.offset = -1;
        this.topicAdminSupplier = adminSupplier;

        this.topic = config.getString(DistributedConfig.CONFIG_TOPIC_CONFIG);
        if (this.topic == null || this.topic.trim().length() == 0)
            throw new ConfigException("Must specify topic for connector configuration.");

        configLog = setupAndCreateKafkaBasedLog(this.topic, config);
        this.configTransformer = configTransformer;
    }

    @Override
    public void setUpdateListener(UpdateListener listener) {
        this.updateListener = listener;
    }

    @Override
    public void start() {
        log.info("Starting KafkaConfigBackingStore");
        // Before startup, callbacks are *not* invoked. You can grab a snapshot after starting -- just take care that
        // updates can continue to occur in the background
        configLog.start();

        int partitionCount = configLog.partitionCount();
        if (partitionCount > 1) {
            String msg = String.format("Topic '%s' supplied via the '%s' property is required "
                    + "to have a single partition in order to guarantee consistency of "
                    + "connector configurations, but found %d partitions.",
                    topic, DistributedConfig.CONFIG_TOPIC_CONFIG, partitionCount);
            throw new ConfigException(msg);
        }

        started = true;
        log.info("Started KafkaConfigBackingStore");
    }

    @Override
    public void stop() {
        log.info("Closing KafkaConfigBackingStore");
        try {
            configLog.stop();
        } finally {
            if (ownTopicAdmin != null) {
                ownTopicAdmin.close();
            }
        }
        log.info("Closed KafkaConfigBackingStore");
    }

    /**
     * Get a snapshot of the current state of the cluster.
     */
    @Override
    public ClusterConfigState snapshot() {
        synchronized (lock) {
            // Only a shallow copy is performed here; in order to avoid accidentally corrupting the worker's view
            // of the config topic, any nested structures should be copied before making modifications
            return new ClusterConfigState(
                    offset,
                    sessionKey,
                    new HashMap<>(connectorTaskCounts),
                    new HashMap<>(connectorConfigs),
                    new HashMap<>(connectorTargetStates),
                    new HashMap<>(taskConfigs),
                    new HashSet<>(inconsistent),
                    configTransformer
            );
        }
    }

    @Override
    public boolean contains(String connector) {
        synchronized (lock) {
            return connectorConfigs.containsKey(connector);
        }
    }

    /**
     * Write this connector configuration to persistent storage and wait until it has been acknowledged and read back by
     * tailing the Kafka log with a consumer.
     *
     * @param connector  name of the connector to write data for
     * @param properties the configuration to write
     */
    @Override
    public void putConnectorConfig(String connector, Map<String, String> properties) {
        log.debug("Writing connector configuration for connector '{}'", connector);
        Struct connectConfig = new Struct(CONNECTOR_CONFIGURATION_V0);
        connectConfig.put("properties", properties);
        byte[] serializedConfig = converter.fromConnectData(topic, CONNECTOR_CONFIGURATION_V0, connectConfig);
        updateConnectorConfig(connector, serializedConfig);
    }

    /**
     * Remove configuration for a given connector.
     * @param connector name of the connector to remove
     */
    @Override
    public void removeConnectorConfig(String connector) {
        log.debug("Removing connector configuration for connector '{}'", connector);
        try {
            configLog.send(CONNECTOR_KEY(connector), null);
            configLog.send(TARGET_STATE_KEY(connector), null);
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to remove connector configuration from Kafka: ", e);
            throw new ConnectException("Error removing connector configuration from Kafka", e);
        }
    }

    @Override
    public void removeTaskConfigs(String connector) {
        throw new UnsupportedOperationException("Removal of tasks is not currently supported");
    }

    private void updateConnectorConfig(String connector, byte[] serializedConfig) {
        try {
            configLog.send(CONNECTOR_KEY(connector), serializedConfig);
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write connector configuration to Kafka: ", e);
            throw new ConnectException("Error writing connector configuration to Kafka", e);
        }
    }

    /**
     * Write these task configurations and associated commit messages, unless an inconsistency is found that indicates
     * that we would be leaving one of the referenced connectors with an inconsistent state.
     *
     * @param connector the connector to write task configuration
     * @param configs list of task configurations for the connector
     * @throws ConnectException if the task configurations do not resolve inconsistencies found in the existing root
     *                          and task configurations.
     */
    @Override
    public void putTaskConfigs(String connector, List<Map<String, String>> configs) {
        // Make sure we're at the end of the log. We should be the only writer, but we want to make sure we don't have
        // any outstanding lagging data to consume.
        try {
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write root configuration to Kafka: ", e);
            throw new ConnectException("Error writing root configuration to Kafka", e);
        }

        int taskCount = configs.size();

        // Start sending all the individual updates
        int index = 0;
        for (Map<String, String> taskConfig: configs) {
            Struct connectConfig = new Struct(TASK_CONFIGURATION_V0);
            connectConfig.put("properties", taskConfig);
            byte[] serializedConfig = converter.fromConnectData(topic, TASK_CONFIGURATION_V0, connectConfig);
            log.debug("Writing configuration for connector '{}' task {}", connector, index);
            ConnectorTaskId connectorTaskId = new ConnectorTaskId(connector, index);
            configLog.send(TASK_KEY(connectorTaskId), serializedConfig);
            index++;
        }

        // Finally, send the commit to update the number of tasks and apply the new configs, then wait until we read to
        // the end of the log
        try {
            // Read to end to ensure all the task configs have been written
            if (taskCount > 0) {
                configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            // Write the commit message
            Struct connectConfig = new Struct(CONNECTOR_TASKS_COMMIT_V0);
            connectConfig.put("tasks", taskCount);
            byte[] serializedConfig = converter.fromConnectData(topic, CONNECTOR_TASKS_COMMIT_V0, connectConfig);
            log.debug("Writing commit for connector '{}' with {} tasks.", connector, taskCount);
            configLog.send(COMMIT_TASKS_KEY(connector), serializedConfig);

            // Read to end to ensure all the commit messages have been written
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write root configuration to Kafka: ", e);
            throw new ConnectException("Error writing root configuration to Kafka", e);
        }
    }

    @Override
    public void refresh(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            configLog.readToEnd().get(timeout, unit);
        } catch (InterruptedException | ExecutionException e) {
            throw new ConnectException("Error trying to read to end of config log", e);
        }
    }

    @Override
    public void putTargetState(String connector, TargetState state) {
        Struct connectTargetState = new Struct(TARGET_STATE_V0);
        connectTargetState.put("state", state.name());
        byte[] serializedTargetState = converter.fromConnectData(topic, TARGET_STATE_V0, connectTargetState);
        log.debug("Writing target state {} for connector {}", state, connector);
        configLog.send(TARGET_STATE_KEY(connector), serializedTargetState);
    }

    @Override
    public void putSessionKey(SessionKey sessionKey) {
        log.debug("Distributing new session key");
        Struct sessionKeyStruct = new Struct(SESSION_KEY_V0);
        sessionKeyStruct.put("key", Base64.getEncoder().encodeToString(sessionKey.key().getEncoded()));
        sessionKeyStruct.put("algorithm", sessionKey.key().getAlgorithm());
        sessionKeyStruct.put("creation-timestamp", sessionKey.creationTimestamp());
        byte[] serializedSessionKey = converter.fromConnectData(topic, SESSION_KEY_V0, sessionKeyStruct);
        try {
            configLog.send(SESSION_KEY_KEY, serializedSessionKey);
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write session key to Kafka: ", e);
            throw new ConnectException("Error writing session key to Kafka", e);
        }
    }

    // package private for testing
    KafkaBasedLog<String, byte[]> setupAndCreateKafkaBasedLog(String topic, final WorkerConfig config) {
        String clusterId = ConnectUtils.lookupKafkaClusterId(config);
        Map<String, Object> originals = config.originals();
        Map<String, Object> producerProps = new HashMap<>(originals);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
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
                                            ? ((DistributedConfig) config).configStorageTopicSettings()
                                            : Collections.emptyMap();
        NewTopic topicDescription = TopicAdmin.defineTopic(topic)
                .config(topicSettings) // first so that we override user-supplied settings as needed
                .compacted()
                .partitions(1)
                .replicationFactor(config.getShort(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG))
                .build();

        return createKafkaBasedLog(topic, producerProps, consumerProps, new ConsumeCallback(), topicDescription, adminSupplier);
    }

    private KafkaBasedLog<String, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps,
                                                              Callback<ConsumerRecord<String, byte[]>> consumedCallback,
                                                              final NewTopic topicDescription, Supplier<TopicAdmin> adminSupplier) {
        java.util.function.Consumer<TopicAdmin> createTopics = admin -> {
            log.debug("Creating admin client to manage Connect internal config topic");
            // Create the topic if it doesn't exist
            Set<String> newTopics = admin.createTopics(topicDescription);
            if (!newTopics.contains(topic)) {
                // It already existed, so check that the topic cleanup policy is compact only and not delete
                log.debug("Using admin client to check cleanup policy of '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
                admin.verifyTopicCleanupPolicyOnlyCompact(topic,
                        DistributedConfig.CONFIG_TOPIC_CONFIG, "connector configurations");
            }
        };
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, adminSupplier, consumedCallback, Time.SYSTEM, createTopics);
    }

    @SuppressWarnings("unchecked")
    private class ConsumeCallback implements Callback<ConsumerRecord<String, byte[]>> {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> record) {
            if (error != null) {
                log.error("Unexpected in consumer callback for KafkaConfigBackingStore: ", error);
                return;
            }

            final SchemaAndValue value;
            try {
                value = converter.toConnectData(topic, record.value());
            } catch (DataException e) {
                log.error("Failed to convert config data to Kafka Connect format: ", e);
                return;
            }
            // Make the recorded offset match the API used for positions in the consumer -- return the offset of the
            // *next record*, not the last one consumed.
            offset = record.offset() + 1;

            if (record.key().startsWith(TARGET_STATE_PREFIX)) {
                String connectorName = record.key().substring(TARGET_STATE_PREFIX.length());
                boolean removed = false;
                synchronized (lock) {
                    if (value.value() == null) {
                        // When connector configs are removed, we also write tombstones for the target state.
                        log.debug("Removed target state for connector {} due to null value in topic.", connectorName);
                        connectorTargetStates.remove(connectorName);
                        removed = true;

                        // If for some reason we still have configs for the connector, add back the default
                        // STARTED state to ensure each connector always has a valid target state.
                        if (connectorConfigs.containsKey(connectorName))
                            connectorTargetStates.put(connectorName, TargetState.STARTED);
                    } else {
                        if (!(value.value() instanceof Map)) {
                            log.error("Found target state ({}) in wrong format: {}",  record.key(), value.value().getClass());
                            return;
                        }
                        Object targetState = ((Map<String, Object>) value.value()).get("state");
                        if (!(targetState instanceof String)) {
                            log.error("Invalid data for target state for connector '{}': 'state' field should be a Map but is {}",
                                    connectorName, targetState == null ? null : targetState.getClass());
                            return;
                        }

                        try {
                            TargetState state = TargetState.valueOf((String) targetState);
                            log.debug("Setting target state for connector '{}' to {}", connectorName, targetState);
                            connectorTargetStates.put(connectorName, state);
                        } catch (IllegalArgumentException e) {
                            log.error("Invalid target state for connector '{}': {}", connectorName, targetState);
                            return;
                        }
                    }
                }

                // Note that we do not notify the update listener if the target state has been removed.
                // Instead we depend on the removal callback of the connector config itself to notify the worker.
                if (started && !removed)
                    updateListener.onConnectorTargetStateChange(connectorName);

            } else if (record.key().startsWith(CONNECTOR_PREFIX)) {
                String connectorName = record.key().substring(CONNECTOR_PREFIX.length());
                boolean removed = false;
                synchronized (lock) {
                    if (value.value() == null) {
                        // Connector deletion will be written as a null value
                        log.info("Successfully processed removal of connector '{}'", connectorName);
                        connectorConfigs.remove(connectorName);
                        connectorTaskCounts.remove(connectorName);
                        taskConfigs.keySet().removeIf(taskId -> taskId.connector().equals(connectorName));
                        removed = true;
                    } else {
                        // Connector configs can be applied and callbacks invoked immediately
                        if (!(value.value() instanceof Map)) {
                            log.error("Found configuration for connector '{}' in wrong format: {}", record.key(), value.value().getClass());
                            return;
                        }
                        Object newConnectorConfig = ((Map<String, Object>) value.value()).get("properties");
                        if (!(newConnectorConfig instanceof Map)) {
                            log.error("Invalid data for config for connector '{}': 'properties' field should be a Map but is {}",
                                      connectorName, newConnectorConfig == null ? null : newConnectorConfig.getClass());
                            return;
                        }
                        log.debug("Updating configuration for connector '{}'", connectorName);
                        connectorConfigs.put(connectorName, (Map<String, String>) newConnectorConfig);

                        // Set the initial state of the connector to STARTED, which ensures that any connectors
                        // which were created with 0.9 Connect will be initialized in the STARTED state.
                        if (!connectorTargetStates.containsKey(connectorName))
                            connectorTargetStates.put(connectorName, TargetState.STARTED);
                    }
                }
                if (started) {
                    if (removed)
                        updateListener.onConnectorConfigRemove(connectorName);
                    else
                        updateListener.onConnectorConfigUpdate(connectorName);
                }
            } else if (record.key().startsWith(TASK_PREFIX)) {
                synchronized (lock) {
                    ConnectorTaskId taskId = parseTaskId(record.key());
                    if (taskId == null) {
                        log.error("Ignoring task configuration because {} couldn't be parsed as a task config key", record.key());
                        return;
                    }
                    if (value.value() == null) {
                        log.error("Ignoring task configuration for task {} because it is unexpectedly null", taskId);
                        return;
                    }
                    if (!(value.value() instanceof Map)) {
                        log.error("Ignoring task configuration for task {} because the value is not a Map but is {}", taskId, value.value().getClass());
                        return;
                    }

                    Object newTaskConfig = ((Map<String, Object>) value.value()).get("properties");
                    if (!(newTaskConfig instanceof Map)) {
                        log.error("Invalid data for config of task {} 'properties' field should be a Map but is {}", taskId, newTaskConfig.getClass());
                        return;
                    }

                    Map<ConnectorTaskId, Map<String, String>> deferred = deferredTaskUpdates.get(taskId.connector());
                    if (deferred == null) {
                        deferred = new HashMap<>();
                        deferredTaskUpdates.put(taskId.connector(), deferred);
                    }
                    log.debug("Storing new config for task {}; this will wait for a commit message before the new config will take effect.", taskId);
                    deferred.put(taskId, (Map<String, String>) newTaskConfig);
                }
            } else if (record.key().startsWith(COMMIT_TASKS_PREFIX)) {
                String connectorName = record.key().substring(COMMIT_TASKS_PREFIX.length());
                List<ConnectorTaskId> updatedTasks = new ArrayList<>();
                synchronized (lock) {
                    // Apply any outstanding deferred task updates for the given connector. Note that just because we
                    // encounter a commit message does not mean it will result in consistent output. In particular due to
                    // compaction, there may be cases where . For example if we have the following sequence of writes:
                    //
                    // 1. Write connector "foo"'s config
                    // 2. Write connector "foo", task 1's config <-- compacted
                    // 3. Write connector "foo", task 2's config
                    // 4. Write connector "foo" task commit message
                    // 5. Write connector "foo", task 1's config
                    // 6. Write connector "foo", task 2's config
                    // 7. Write connector "foo" task commit message
                    //
                    // then when a new worker starts up, if message 2 had been compacted, then when message 4 is applied
                    // "foo" will not have a complete set of configs. Only when message 7 is applied will the complete
                    // configuration be available. Worse, if the leader died while writing messages 5, 6, and 7 such that
                    // only 5 was written, then there may be nothing that will finish writing the configs and get the
                    // log back into a consistent state.
                    //
                    // It is expected that the user of this class (i.e., the Herder) will take the necessary action to
                    // resolve this (i.e., get the connector to recommit its configuration). This inconsistent state is
                    // exposed in the snapshots provided via ClusterConfigState so they are easy to handle.
                    if (!(value.value() instanceof Map)) { // Schema-less, so we get maps instead of structs
                        log.error("Ignoring connector tasks configuration commit for connector '{}' because it is in the wrong format: {}", connectorName, value.value());
                        return;
                    }
                    Map<ConnectorTaskId, Map<String, String>> deferred = deferredTaskUpdates.get(connectorName);

                    int newTaskCount = intValue(((Map<String, Object>) value.value()).get("tasks"));

                    // Validate the configs we're supposed to update to ensure we're getting a complete configuration
                    // update of all tasks that are expected based on the number of tasks in the commit message.
                    Set<Integer> taskIdSet = taskIds(connectorName, deferred);
                    if (!completeTaskIdSet(taskIdSet, newTaskCount)) {
                        // Given the logic for writing commit messages, we should only hit this condition due to compacted
                        // historical data, in which case we would not have applied any updates yet and there will be no
                        // task config data already committed for the connector, so we shouldn't have to clear any data
                        // out. All we need to do is add the flag marking it inconsistent.
                        log.debug("We have an incomplete set of task configs for connector '{}' probably due to compaction. So we are not doing anything with the new configuration.", connectorName);
                        inconsistent.add(connectorName);
                    } else {
                        if (deferred != null) {
                            taskConfigs.putAll(deferred);
                            updatedTasks.addAll(deferred.keySet());
                        }
                        inconsistent.remove(connectorName);
                    }
                    // Always clear the deferred entries, even if we didn't apply them. If they represented an inconsistent
                    // update, then we need to see a completely fresh set of configs after this commit message, so we don't
                    // want any of these outdated configs
                    if (deferred != null)
                        deferred.clear();

                    connectorTaskCounts.put(connectorName, newTaskCount);
                }

                if (started)
                    updateListener.onTaskConfigUpdate(updatedTasks);
            } else if (record.key().equals(SESSION_KEY_KEY)) {
                if (value.value() == null) {
                    log.error("Ignoring session key because it is unexpectedly null");
                    return;
                }
                if (!(value.value() instanceof Map)) {
                    log.error("Ignoring session key because the value is not a Map but is {}", value.value().getClass());
                    return;
                }

                Map<String, Object> valueAsMap = (Map<String, Object>) value.value();

                Object sessionKey = valueAsMap.get("key");
                if (!(sessionKey instanceof String)) {
                    log.error("Invalid data for session key 'key' field should be a String but is {}", sessionKey.getClass());
                    return;
                }
                byte[] key = Base64.getDecoder().decode((String) sessionKey);

                Object keyAlgorithm = valueAsMap.get("algorithm");
                if (!(keyAlgorithm instanceof String)) {
                    log.error("Invalid data for session key 'algorithm' field should be a String but it is {}", keyAlgorithm.getClass());
                    return;
                }

                Object creationTimestamp = valueAsMap.get("creation-timestamp");
                if (!(creationTimestamp instanceof Long)) {
                    log.error("Invalid data for session key 'creation-timestamp' field should be a long but it is {}", creationTimestamp.getClass());
                    return;
                }
                KafkaConfigBackingStore.this.sessionKey = new SessionKey(
                        new SecretKeySpec(key, (String) keyAlgorithm),
                        (long) creationTimestamp
                );

                if (started)
                    updateListener.onSessionKeyUpdate(KafkaConfigBackingStore.this.sessionKey);
            } else {
                log.error("Discarding config update record with invalid key: {}", record.key());
            }
        }

    }

    private ConnectorTaskId parseTaskId(String key) {
        String[] parts = key.split("-");
        if (parts.length < 3) return null;

        try {
            int taskNum = Integer.parseInt(parts[parts.length - 1]);
            String connectorName = Utils.join(Arrays.copyOfRange(parts, 1, parts.length - 1), "-");
            return new ConnectorTaskId(connectorName, taskNum);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Given task configurations, get a set of integer task IDs for the connector.
     */
    private Set<Integer> taskIds(String connector, Map<ConnectorTaskId, Map<String, String>> configs) {
        Set<Integer> tasks = new TreeSet<>();
        if (configs == null) {
            return tasks;
        }
        for (ConnectorTaskId taskId : configs.keySet()) {
            assert taskId.connector().equals(connector);
            tasks.add(taskId.task());
        }
        return tasks;
    }

    private boolean completeTaskIdSet(Set<Integer> idSet, int expectedSize) {
        // Note that we do *not* check for the exact set. This is an important implication of compaction. If we start out
        // with 2 tasks, then reduce to 1, we'll end up with log entries like:
        //
        // 1. Connector "foo" config
        // 2. Connector "foo", task 1 config
        // 3. Connector "foo", task 2 config
        // 4. Connector "foo", commit 2 tasks
        // 5. Connector "foo", task 1 config
        // 6. Connector "foo", commit 1 tasks
        //
        // However, due to compaction we could end up with a log that looks like this:
        //
        // 1. Connector "foo" config
        // 3. Connector "foo", task 2 config
        // 5. Connector "foo", task 1 config
        // 6. Connector "foo", commit 1 tasks
        //
        // which isn't incorrect, but would appear in this code to have an extra task configuration. Instead, we just
        // validate that all the configs specified by the commit message are present. This should be fine because the
        // logic for writing configs ensures all the task configs are written (and reads them back) before writing the
        // commit message.

        if (idSet.size() < expectedSize)
            return false;

        for (int i = 0; i < expectedSize; i++)
            if (!idSet.contains(i))
                return false;
        return true;
    }

    // Convert an integer value extracted from a schemaless struct to an int. This handles potentially different
    // encodings by different Converters.
    private static int intValue(Object value) {
        if (value instanceof Integer)
            return (int) value;
        else if (value instanceof Long)
            return (int) (long) value;
        else
            throw new ConnectException("Expected integer value to be either Integer or Long");
    }
}

