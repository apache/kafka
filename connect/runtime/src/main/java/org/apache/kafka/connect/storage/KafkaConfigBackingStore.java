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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.TargetState.PAUSED;
import static org.apache.kafka.connect.runtime.TargetState.STOPPED;
import static org.apache.kafka.connect.util.ConnectUtils.className;

/**
 * <p>
 * Provides persistent storage of Kafka Connect connector configurations in a Kafka topic.
 * </p>
 * <p>
 * This class manages both connector and task configurations, among other various configurations. It tracks seven types
 * of records:
 * <ol>
 *   <li> Connector config: map of string -> string configurations passed to the Connector class, with support for
 *   expanding this format if necessary. (Kafka key: connector-[connector-id]).
 *   These configs are *not* ephemeral. They represent the source of truth. If the entire Connect
 *   cluster goes down, this is all that is really needed to recover.
 *   <li> Task configs: map of string -> string configurations passed to the Task class, with support for expanding
 *   this format if necessary. (Kafka key: task-[connector-id]-[task-id]).
 *   These configs are ephemeral; they are stored here to:
 *     <ul>
 *       <li> disseminate them to all workers while ensuring agreement
 *       <li> to allow faster cluster/worker recovery since the common case of recovery (restoring a connector) will simply
 *       result in the same task configuration as before the failure.
 *     </ul>
 *   <li> Task commit "configs": records indicating that previous task config entries should be committed and all task
 *   configs for a connector can be applied. (Kafka key: commit-[connector-id].
 *   This config has two effects. First, it records the number of tasks the connector is currently
 *   running (and can therefore increase/decrease parallelism). Second, because each task config
 *   is stored separately but they need to be applied together to ensure each partition is assigned
 *   to a single task, this record also indicates that task configs for the specified connector
 *   can be "applied" or "committed".
 *   <li> Connector target states: records indicating the {@link TargetState} for a connector
 *   <li> {@link RestartRequest Restart requests}: records representing requests to restart a connector and / or its
 *   tasks. See <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181308623">KIP-745</a> for more
 *   details.
 *   <li> Task count records: an integer value that that tracks the number of task producers (for source connectors) that
 *   will have to be fenced out if a connector is reconfigured before bringing up any tasks with the new set of task
 *   configurations. This is required for exactly-once support for source connectors, see
 *   <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors">KIP-618</a>
 *   for more details.
 *   <li> Session key records: A {@link SessionKey} generated by the leader of the cluster when
 *   {@link ConnectProtocolCompatibility#SESSIONED} is the Connect protocol being used by the cluster. This session key
 *   is used to verify internal REST requests between workers in the cluster. See
 *   <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-507%3A+Securing+Internal+Connect+REST+Endpoints">KIP-507</a>
 *   for more details.
 * </ol>
 * <p/>
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
 * The most obvious implication for task configs is the need for the commit messages. This class doesn't currently make
 * use of Kafka's multi-record transactions and instead uses task commit messages to ensure that readers do not
 * end up using inconsistent configs. For example, consider if a connector wrote configs for its tasks,
 * then was reconfigured and only managed to write updated configs for half its tasks. If task configs
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
 * This allows users of this class (i.e., {@link Herder} implementations) to take action to resolve any inconsistencies. These
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
public class KafkaConfigBackingStore extends KafkaTopicBasedBackingStore implements ConfigBackingStore {
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

    public static final String TASK_COUNT_RECORD_PREFIX = "tasks-fencing-";

    public static String TASK_COUNT_RECORD_KEY(String connectorName) {
        return TASK_COUNT_RECORD_PREFIX + connectorName;
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
    public static final Schema TARGET_STATE_V1 = SchemaBuilder.struct()
            .field("state", Schema.STRING_SCHEMA)
            .field("state.v2", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    public static final Schema TASK_COUNT_RECORD_V0 = SchemaBuilder.struct()
            .field("task-count", Schema.INT32_SCHEMA)
            .build();
    // The key is logically a byte array, but we can't use the JSON converter to (de-)serialize that without a schema.
    // So instead, we base 64-encode it before serializing and decode it after deserializing.
    public static final Schema SESSION_KEY_V0 = SchemaBuilder.struct()
            .field("key", Schema.STRING_SCHEMA)
            .field("algorithm", Schema.STRING_SCHEMA)
            .field("creation-timestamp", Schema.INT64_SCHEMA)
            .build();

    public static final String RESTART_PREFIX = "restart-connector-";

    public static String RESTART_KEY(String connectorName) {
        return RESTART_PREFIX + connectorName;
    }

    public static final boolean ONLY_FAILED_DEFAULT = false;
    public static final boolean INCLUDE_TASKS_DEFAULT = false;
    public static final String ONLY_FAILED_FIELD_NAME = "only-failed";
    public static final String INCLUDE_TASKS_FIELD_NAME = "include-tasks";
    public static final Schema RESTART_REQUEST_V0 = SchemaBuilder.struct()
            .field(INCLUDE_TASKS_FIELD_NAME, Schema.BOOLEAN_SCHEMA)
            .field(ONLY_FAILED_FIELD_NAME, Schema.BOOLEAN_SCHEMA)
            .build();

    public static final String LOGGER_CLUSTER_PREFIX = "logger-cluster-";
    public static String LOGGER_CLUSTER_KEY(String namespace) {
        return LOGGER_CLUSTER_PREFIX + namespace;
    }
    public static final Schema LOGGER_LEVEL_V0 = SchemaBuilder.struct()
            .field("level", Schema.STRING_SCHEMA)
            .build();

    // Visible for testing
    static final long READ_WRITE_TOTAL_TIMEOUT_MS = 30000;

    private final Object lock;
    private final Converter converter;
    private volatile boolean started;
    // Although updateListener is not final, it's guaranteed to be visible to any thread after its
    // initialization as long as we always read the volatile variable "started" before we access the listener.
    private ConfigBackingStore.UpdateListener updateListener;

    private final Map<String, Object> baseProducerProps;

    private final String topic;
    // Data is passed to the log already serialized. We use a converter to handle translating to/from generic Connect
    // format to serialized form
    private final KafkaBasedLog<String, byte[]> configLog;
    // Connector -> # of tasks
    final Map<String, Integer> connectorTaskCounts = new HashMap<>();
    // Connector and task configs: name or id -> config map
    final Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
    final Map<ConnectorTaskId, Map<String, String>> taskConfigs = new HashMap<>();
    private final Supplier<TopicAdmin> topicAdminSupplier;
    private SharedTopicAdmin ownTopicAdmin;
    private final String clientId;

    // Set of connectors where we saw a task commit with an incomplete set of task config updates, indicating the data
    // is in an inconsistent state and we cannot safely use them until they have been refreshed.
    final Set<String> inconsistent = new HashSet<>();
    // The most recently read offset. This does not take into account deferred task updates/commits, so we may have
    // outstanding data to be applied.
    private volatile long offset;
    // The most recently read session key, to use for validating internal REST requests.
    private volatile SessionKey sessionKey;

    // Connector -> Map[ConnectorTaskId -> Configs]
    // visible for testing
    final Map<String, Map<ConnectorTaskId, Map<String, String>>> deferredTaskUpdates = new HashMap<>();

    final Map<String, TargetState> connectorTargetStates = new HashMap<>();

    final Map<String, Integer> connectorTaskCountRecords = new HashMap<>();
    final Map<String, Integer> connectorTaskConfigGenerations = new HashMap<>();
    final Set<String> connectorsPendingFencing = new HashSet<>();

    private final WorkerConfigTransformer configTransformer;

    private final boolean usesFencableWriter;
    private volatile Producer<String, byte[]> fencableProducer;
    private final Map<String, Object> fencableProducerProps;
    private final Time time;

    @Deprecated
    public KafkaConfigBackingStore(Converter converter, DistributedConfig config, WorkerConfigTransformer configTransformer) {
        this(converter, config, configTransformer, null, "connect-distributed-");
    }

    public KafkaConfigBackingStore(Converter converter, DistributedConfig config, WorkerConfigTransformer configTransformer, Supplier<TopicAdmin> adminSupplier, String clientIdBase) {
        this(converter, config, configTransformer, adminSupplier, clientIdBase, Time.SYSTEM);
    }

    @SuppressWarnings("this-escape")
    KafkaConfigBackingStore(Converter converter, DistributedConfig config, WorkerConfigTransformer configTransformer, Supplier<TopicAdmin> adminSupplier, String clientIdBase, Time time) {
        this.lock = new Object();
        this.started = false;
        this.converter = converter;
        this.offset = -1;
        this.topicAdminSupplier = adminSupplier;
        this.clientId = Objects.requireNonNull(clientIdBase) + "configs";
        this.time = time;

        this.baseProducerProps = baseProducerProps(config);
        // By default, Connect disables idempotent behavior for all producers, even though idempotence became
        // default for Kafka producers. This is to ensure Connect continues to work with many Kafka broker versions, including older brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        baseProducerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        this.fencableProducerProps = fencableProducerProps(config);

        this.usesFencableWriter = config.transactionalLeaderEnabled();
        this.topic = config.getString(DistributedConfig.CONFIG_TOPIC_CONFIG);
        if (this.topic == null || this.topic.trim().length() == 0)
            throw new ConfigException("Must specify topic for connector configuration.");

        configLog = setupAndCreateKafkaBasedLog(this.topic, config);
        this.configTransformer = configTransformer;
    }

    @Override
    public void setUpdateListener(ConfigBackingStore.UpdateListener listener) {
        this.updateListener = listener;
    }

    @Override
    public void start() {
        log.info("Starting KafkaConfigBackingStore");
        // Before startup, callbacks are *not* invoked. You can grab a snapshot after starting -- just take care that
        // updates can continue to occur in the background
        try {
            configLog.start();
        } catch (UnsupportedVersionException e) {
            throw new ConnectException(
                    "Enabling exactly-once support for source connectors requires a Kafka broker version that allows "
                            + "admin clients to read consumer offsets. Please either disable the worker's exactly-once "
                            + "support for source connectors, or use a newer Kafka broker version.",
                    e
            );
        }

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

        relinquishWritePrivileges();
        Utils.closeQuietly(ownTopicAdmin, "admin for config topic");
        Utils.closeQuietly(configLog::stop, "KafkaBasedLog for config topic");

        log.info("Closed KafkaConfigBackingStore");
    }

    @Override
    public void claimWritePrivileges() {
        if (usesFencableWriter && fencableProducer == null) {
            try {
                fencableProducer = createFencableProducer();
                fencableProducer.initTransactions();
            } catch (Exception e) {
                relinquishWritePrivileges();
                throw new ConnectException("Failed to create and initialize fencable producer for config topic", e);
            }
        }
    }

    private Map<String, Object> baseProducerProps(WorkerConfig workerConfig) {
        Map<String, Object> producerProps = new HashMap<>(workerConfig.originals());
        String kafkaClusterId = workerConfig.kafkaClusterId();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        ConnectUtils.addMetricsContextProperties(producerProps, workerConfig, kafkaClusterId);
        return producerProps;
    }

    // Visible for testing
    Map<String, Object> fencableProducerProps(DistributedConfig workerConfig) {
        Map<String, Object> result = new HashMap<>(baseProducerProps(workerConfig));

        result.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-leader");
        // Always require producer acks to all to ensure durable writes
        result.put(ProducerConfig.ACKS_CONFIG, "all");
        // We can set this to 5 instead of 1 without risking reordering because we are using an idempotent producer
        result.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        ConnectUtils.ensureProperty(
                result, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
                "for the worker's config topic producer when exactly-once source support is enabled or in preparation to be enabled",
                false
        );
        ConnectUtils.ensureProperty(
                result, ProducerConfig.TRANSACTIONAL_ID_CONFIG, workerConfig.transactionalProducerId(),
                "for the worker's config topic producer when exactly-once source support is enabled or in preparation to be enabled",
                true
        );

        return result;
    }

    // Visible in order to be mocked during testing
    Producer<String, byte[]> createFencableProducer() {
        return new KafkaProducer<>(fencableProducerProps);
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
                    new HashMap<>(connectorTaskCountRecords),
                    new HashMap<>(connectorTaskConfigGenerations),
                    new HashSet<>(connectorsPendingFencing),
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
     * Write this connector configuration (and optionally a target state) to persistent storage and wait until it has been acknowledged and read
     * back by tailing the Kafka log with a consumer. {@link #claimWritePrivileges()} must be successfully invoked before calling
     * this method if the worker is configured to use a fencable producer for writes to the config topic.
     *
     * @param connector  name of the connector to write data for
     * @param properties the configuration to write
     * @param targetState the desired target state for the connector; may be {@code null} if no target state change is desired. Note that the default
     *                    target state is {@link TargetState#STARTED} if no target state exists previously
     * @throws IllegalStateException if {@link #claimWritePrivileges()} is required, but was not successfully invoked before
     * this method was called
     * @throws PrivilegedWriteException if the worker is configured to use a fencable producer for writes to the config topic
     * and the write fails
     */
    @Override
    public void putConnectorConfig(String connector, Map<String, String> properties, TargetState targetState) {
        log.debug("Writing connector configuration for connector '{}'", connector);
        Struct connectConfig = new Struct(CONNECTOR_CONFIGURATION_V0);
        connectConfig.put("properties", properties);
        byte[] serializedConfig = converter.fromConnectData(topic, CONNECTOR_CONFIGURATION_V0, connectConfig);
        try {
            Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
            List<ProducerKeyValue> keyValues = new ArrayList<>();
            if (targetState != null) {
                log.debug("Writing target state {} for connector {}", targetState, connector);
                keyValues.add(new ProducerKeyValue(TARGET_STATE_KEY(connector), serializeTargetState(targetState)));
            }
            keyValues.add(new ProducerKeyValue(CONNECTOR_KEY(connector), serializedConfig));
            sendPrivileged(keyValues, timer);
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write connector configuration to Kafka: ", e);
            throw new ConnectException("Error writing connector configuration to Kafka", e);
        }
    }

    /**
     * Remove configuration for a given connector. {@link #claimWritePrivileges()} must be successfully invoked before calling
     * this method if the worker is configured to use a fencable producer for writes to the config topic.
     * @param connector name of the connector to remove
     * @throws IllegalStateException if {@link #claimWritePrivileges()} is required, but was not successfully invoked before
     * this method was called
     * @throws PrivilegedWriteException if the worker is configured to use a fencable producer for writes to the config topic
     * and the write fails
     */
    @Override
    public void removeConnectorConfig(String connector) {
        log.debug("Removing connector configuration for connector '{}'", connector);
        try {
            Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
            List<ProducerKeyValue> keyValues = Arrays.asList(
                    new ProducerKeyValue(CONNECTOR_KEY(connector), null),
                    new ProducerKeyValue(TARGET_STATE_KEY(connector), null)
            );
            sendPrivileged(keyValues, timer);

            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to remove connector configuration from Kafka: ", e);
            throw new ConnectException("Error removing connector configuration from Kafka", e);
        }
    }

    @Override
    public void removeTaskConfigs(String connector) {
        throw new UnsupportedOperationException("Removal of tasks is not currently supported");
    }

    /**
     * Write these task configurations and associated commit messages, unless an inconsistency is found that indicates
     * that we would be leaving one of the referenced connectors with an inconsistent state. {@link #claimWritePrivileges()}
     * must be successfully invoked before calling this method if the worker is configured to use a fencable producer for
     * writes to the config topic.
     *
     * @param connector the connector to write task configuration
     * @param configs list of task configurations for the connector
     * @throws ConnectException if the task configurations do not resolve inconsistencies found in the existing root
     *                          and task configurations.
     * @throws IllegalStateException if {@link #claimWritePrivileges()} is required, but was not successfully invoked before
     * this method was called
     * @throws PrivilegedWriteException if the worker is configured to use a fencable producer for writes to the config topic
     * and the write fails
     */
    @Override
    public void putTaskConfigs(String connector, List<Map<String, String>> configs) {
        Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
        // Make sure we're at the end of the log. We should be the only writer, but we want to make sure we don't have
        // any outstanding lagging data to consume.
        try {
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
            timer.update();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write root configuration to Kafka: ", e);
            throw new ConnectException("Error writing root configuration to Kafka", e);
        }

        int taskCount = configs.size();

        // Send all the individual updates
        int index = 0;
        List<ProducerKeyValue> keyValues = new ArrayList<>();
        for (Map<String, String> taskConfig: configs) {
            Struct connectConfig = new Struct(TASK_CONFIGURATION_V0);
            connectConfig.put("properties", taskConfig);
            byte[] serializedConfig = converter.fromConnectData(topic, TASK_CONFIGURATION_V0, connectConfig);
            log.debug("Writing configuration for connector '{}' task {}", connector, index);
            ConnectorTaskId connectorTaskId = new ConnectorTaskId(connector, index);
            keyValues.add(new ProducerKeyValue(TASK_KEY(connectorTaskId), serializedConfig));
            index++;
        }

        try {
            sendPrivileged(keyValues, timer);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Failed to write task configurations to Kafka", e);
            throw new ConnectException("Error writing task configurations to Kafka", e);
        }

        // Finally, send the commit to update the number of tasks and apply the new configs, then wait until we read to
        // the end of the log
        try {
            // Read to end to ensure all the task configs have been written
            if (taskCount > 0) {
                configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
                timer.update();
            }
            // Write the commit message
            Struct connectConfig = new Struct(CONNECTOR_TASKS_COMMIT_V0);
            connectConfig.put("tasks", taskCount);
            byte[] serializedConfig = converter.fromConnectData(topic, CONNECTOR_TASKS_COMMIT_V0, connectConfig);
            log.debug("Writing commit for connector '{}' with {} tasks.", connector, taskCount);

            sendPrivileged(COMMIT_TASKS_KEY(connector), serializedConfig, timer);

            // Read to end to ensure all the commit messages have been written
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
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

    /**
     * Write a new {@link TargetState} for the connector. Note that {@link #claimWritePrivileges()} does not need to be
     * invoked before invoking this method.
     * @param connector the name of the connector
     * @param state the desired target state for the connector
     */
    @Override
    public void putTargetState(String connector, TargetState state) {
        log.debug("Writing target state {} for connector {}", state, connector);
        try {
            configLog.sendWithReceipt(TARGET_STATE_KEY(connector), serializeTargetState(state))
                .get(READ_WRITE_TOTAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write target state to Kafka", e);
            throw new ConnectException("Error writing target state to Kafka", e);
        }
    }

    private byte[] serializeTargetState(TargetState state) {
        Struct connectTargetState = new Struct(TARGET_STATE_V1);
        // Older workers don't support the STOPPED state; fall back on PAUSED
        connectTargetState.put("state", state == STOPPED ? PAUSED.name() : state.name());
        connectTargetState.put("state.v2", state.name());
        return converter.fromConnectData(topic, TARGET_STATE_V1, connectTargetState);
    }

    /**
     * Write a task count record for a connector to persistent storage and wait until it has been acknowledged and read back by
     * tailing the Kafka log with a consumer. {@link #claimWritePrivileges()} must be successfully invoked before calling this method
     * if the worker is configured to use a fencable producer for writes to the config topic.
     * @param connector name of the connector
     * @param taskCount number of tasks used by the connector
     * @throws IllegalStateException if {@link #claimWritePrivileges()} is required, but was not successfully invoked before
     * this method was called
     * @throws PrivilegedWriteException if the worker is configured to use a fencable producer for writes to the config topic
     * and the write fails
     */
    @Override
    public void putTaskCountRecord(String connector, int taskCount) {
        Struct taskCountRecord = new Struct(TASK_COUNT_RECORD_V0);
        taskCountRecord.put("task-count", taskCount);
        byte[] serializedTaskCountRecord = converter.fromConnectData(topic, TASK_COUNT_RECORD_V0, taskCountRecord);
        log.debug("Writing task count record {} for connector {}", taskCount, connector);
        try {
            Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
            sendPrivileged(TASK_COUNT_RECORD_KEY(connector), serializedTaskCountRecord, timer);
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write task count record with {} tasks for connector {} to Kafka: ", taskCount, connector, e);
            throw new ConnectException("Error writing task count record to Kafka", e);
        }
    }

    /**
     * Write a session key to persistent storage and wait until it has been acknowledged and read back by tailing the Kafka log
     * with a consumer. {@link #claimWritePrivileges()} must be successfully invoked before calling this method if the worker
     * is configured to use a fencable producer for writes to the config topic.
     * @param sessionKey the session key to distributed
     * @throws IllegalStateException if {@link #claimWritePrivileges()} is required, but was not successfully invoked before
     * this method was called
     * @throws PrivilegedWriteException if the worker is configured to use a fencable producer for writes to the config topic
     * and the write fails
     */
    @Override
    public void putSessionKey(SessionKey sessionKey) {
        log.debug("Distributing new session key");
        Struct sessionKeyStruct = new Struct(SESSION_KEY_V0);
        sessionKeyStruct.put("key", Base64.getEncoder().encodeToString(sessionKey.key().getEncoded()));
        sessionKeyStruct.put("algorithm", sessionKey.key().getAlgorithm());
        sessionKeyStruct.put("creation-timestamp", sessionKey.creationTimestamp());
        byte[] serializedSessionKey = converter.fromConnectData(topic, SESSION_KEY_V0, sessionKeyStruct);
        try {
            Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
            sendPrivileged(SESSION_KEY_KEY, serializedSessionKey, timer);
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write session key to Kafka: ", e);
            throw new ConnectException("Error writing session key to Kafka", e);
        }
    }

    /**
     * Write a restart request for the connector and optionally its tasks to persistent storage and wait until it has been
     * acknowledged and read back by tailing the Kafka log with a consumer. {@link #claimWritePrivileges()} must be successfully
     * invoked before calling this method if the worker is configured to use a fencable producer for writes to the config topic.
     * @param restartRequest the restart request details
     */
    @Override
    public void putRestartRequest(RestartRequest restartRequest) {
        log.debug("Writing {} to Kafka", restartRequest);
        String key = RESTART_KEY(restartRequest.connectorName());
        Struct value = new Struct(RESTART_REQUEST_V0);
        value.put(INCLUDE_TASKS_FIELD_NAME, restartRequest.includeTasks());
        value.put(ONLY_FAILED_FIELD_NAME, restartRequest.onlyFailed());
        byte[] serializedValue = converter.fromConnectData(topic, value.schema(), value);
        try {
            Timer timer = time.timer(READ_WRITE_TOTAL_TIMEOUT_MS);
            sendPrivileged(key, serializedValue, timer);
            configLog.readToEnd().get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write {} to Kafka: ", restartRequest, e);
            throw new ConnectException("Error writing " + restartRequest + " to Kafka", e);
        }
    }

    @Override
    public void putLoggerLevel(String namespace, String level) {
        log.debug("Writing level {} for logging namespace {} to Kafka", level, namespace);
        Struct value = new Struct(LOGGER_LEVEL_V0);
        value.put("level", level);
        byte[] serializedValue = converter.fromConnectData(topic, value.schema(), value);
        try {
            configLog.sendWithReceipt(LOGGER_CLUSTER_KEY(namespace), serializedValue).get(READ_WRITE_TOTAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write logger level to Kafka", e);
            throw new ConnectException("Error writing logger level to Kafka", e);
        }
    }

    // package private for testing
    KafkaBasedLog<String, byte[]> setupAndCreateKafkaBasedLog(String topic, final WorkerConfig config) {
        String clusterId = config.kafkaClusterId();
        Map<String, Object> originals = config.originals();

        Map<String, Object> producerProps = new HashMap<>(baseProducerProps);
        producerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);

        Map<String, Object> consumerProps = new HashMap<>(originals);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        ConnectUtils.addMetricsContextProperties(consumerProps, config, clusterId);
        if (config.exactlyOnceSourceEnabled()) {
            ConnectUtils.ensureProperty(
                    consumerProps, ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString(),
                    "for the worker's config topic consumer when exactly-once source support is enabled",
                    true
            );
        }

        Map<String, Object> adminProps = new HashMap<>(originals);
        ConnectUtils.addMetricsContextProperties(adminProps, config, clusterId);
        adminProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
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

        return createKafkaBasedLog(topic, producerProps, consumerProps, new ConsumeCallback(), topicDescription, adminSupplier, config, time);
    }

    /**
     * Send a single record to the config topic synchronously. Note that {@link #claimWritePrivileges()} must be
     * successfully invoked before calling this method if this store is configured to use a fencable writer.
     * @param key the record key
     * @param value the record value
     * @param timer Timer bounding how long this method can block. The timer is updated before the method returns.
     */
    private void sendPrivileged(String key, byte[] value, Timer timer) throws ExecutionException, InterruptedException, TimeoutException {
        sendPrivileged(Collections.singletonList(new ProducerKeyValue(key, value)), timer);
    }

    /**
     * Send one or more records to the config topic synchronously. Note that {@link #claimWritePrivileges()} must be
     * successfully invoked before calling this method if this store is configured to use a fencable writer.
     * @param keyValues the list of producer record key/value pairs
     * @param timer Timer bounding how long this method can block. The timer is updated before the method returns.
     */
    private void sendPrivileged(List<ProducerKeyValue> keyValues, Timer timer) throws ExecutionException, InterruptedException, TimeoutException {
        if (!usesFencableWriter) {
            List<Future<RecordMetadata>> producerFutures = new ArrayList<>();
            keyValues.forEach(
                    keyValue -> producerFutures.add(configLog.sendWithReceipt(keyValue.key, keyValue.value))
            );

            timer.update();
            for (Future<RecordMetadata> future : producerFutures) {
                future.get(timer.remainingMs(), TimeUnit.MILLISECONDS);
                timer.update();
            }

            return;
        }

        if (fencableProducer == null) {
            throw new IllegalStateException("Cannot produce to config topic without claiming write privileges first");
        }

        try {
            fencableProducer.beginTransaction();
            keyValues.forEach(
                    keyValue -> fencableProducer.send(new ProducerRecord<>(topic, keyValue.key, keyValue.value))
            );
            fencableProducer.commitTransaction();
            timer.update();
        } catch (Exception e) {
            log.warn("Failed to perform fencable send to config topic", e);
            relinquishWritePrivileges();
            throw new PrivilegedWriteException("Failed to perform fencable send to config topic", e);
        }
    }

    private static class ProducerKeyValue {
        final String key;
        final byte[] value;

        ProducerKeyValue(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private void relinquishWritePrivileges() {
        if (fencableProducer != null) {
            Utils.closeQuietly(() -> fencableProducer.close(Duration.ZERO), "fencable producer for config topic");
            fencableProducer = null;
        }
    }

    @Override
    protected String getTopicConfig() {
        return DistributedConfig.CONFIG_TOPIC_CONFIG;
    }

    @Override
    protected String getTopicPurpose() {
        return "connector configurations";
    }

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
                processTargetStateRecord(connectorName, value);
            } else if (record.key().startsWith(CONNECTOR_PREFIX)) {
                String connectorName = record.key().substring(CONNECTOR_PREFIX.length());
                processConnectorConfigRecord(connectorName, value);
            } else if (record.key().startsWith(TASK_PREFIX)) {
                ConnectorTaskId taskId = parseTaskId(record.key());
                if (taskId == null) {
                    log.error("Ignoring task configuration because {} couldn't be parsed as a task config key", record.key());
                    return;
                }
                processTaskConfigRecord(taskId, value);
            } else if (record.key().startsWith(COMMIT_TASKS_PREFIX)) {
                String connectorName = record.key().substring(COMMIT_TASKS_PREFIX.length());
                processTasksCommitRecord(connectorName, value);
            } else if (record.key().startsWith(RESTART_PREFIX)) {
                RestartRequest request = recordToRestartRequest(record, value);
                // Only notify the listener if this backing store is already successfully started (having caught up the first time)
                if (request != null && started) {
                    updateListener.onRestartRequest(request);
                }
            } else if (record.key().startsWith(TASK_COUNT_RECORD_PREFIX)) {
                String connectorName = record.key().substring(TASK_COUNT_RECORD_PREFIX.length());
                processTaskCountRecord(connectorName, value);
            } else if (record.key().equals(SESSION_KEY_KEY)) {
                processSessionKeyRecord(value);
            } else if (record.key().startsWith(LOGGER_CLUSTER_PREFIX)) {
                String loggingNamespace = record.key().substring(LOGGER_CLUSTER_PREFIX.length());
                processLoggerLevelRecord(loggingNamespace, value);
            } else {
                log.error("Discarding config update record with invalid key: {}", record.key());
            }
        }

    }

    private void processTargetStateRecord(String connectorName, SchemaAndValue value) {
        boolean removed = false;
        boolean stateChanged = true;
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
                    log.error("Ignoring target state for connector '{}' because it is in the wrong format: {}", connectorName, className(value.value()));
                    return;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> valueMap = (Map<String, Object>) value.value();
                Object targetState = valueMap.get("state.v2");
                if (targetState != null && !(targetState instanceof String)) {
                    log.error("Invalid data for target state for connector '{}': 'state.v2' field should be a String but is {}",
                            connectorName, className(targetState));
                    // We don't return here; it's still possible that there's a value we can use in the older state field
                    targetState = null;
                }
                if (targetState == null) {
                    // This record may have been written by an older worker; fall back on the older state field
                    targetState = valueMap.get("state");
                    if (!(targetState instanceof String)) {
                        log.error("Invalid data for target state for connector '{}': 'state' field should be a String but is {}",
                                connectorName, className(targetState));
                        return;
                    }
                }

                try {
                    TargetState state = TargetState.valueOf((String) targetState);
                    log.debug("Setting target state for connector '{}' to {}", connectorName, targetState);
                    TargetState prevState = connectorTargetStates.put(connectorName, state);
                    stateChanged = !state.equals(prevState);
                } catch (IllegalArgumentException e) {
                    log.error("Invalid target state for connector '{}': {}", connectorName, targetState);
                    return;
                }
            }
        }

        // Note that we do not notify the update listener if the target state has been removed.
        // Instead we depend on the removal callback of the connector config itself to notify the worker.
        // We also don't notify the update listener if the connector doesn't exist yet - a scenario that can
        // occur if an explicit initial target state is specified in the connector creation request.
        if (started && !removed && stateChanged && connectorConfigs.containsKey(connectorName))
            updateListener.onConnectorTargetStateChange(connectorName);
    }

    private void processConnectorConfigRecord(String connectorName, SchemaAndValue value) {
        boolean removed = false;
        synchronized (lock) {
            if (value.value() == null) {
                // Connector deletion will be written as a null value
                log.info("Successfully processed removal of connector '{}'", connectorName);
                connectorConfigs.remove(connectorName);
                connectorTaskCounts.remove(connectorName);
                taskConfigs.keySet().removeIf(taskId -> taskId.connector().equals(connectorName));
                deferredTaskUpdates.remove(connectorName);
                removed = true;
            } else {
                // Connector configs can be applied and callbacks invoked immediately
                if (!(value.value() instanceof Map)) {
                    log.error("Ignoring configuration for connector '{}' because it is in the wrong format: {}", connectorName, className(value.value()));
                    return;
                }
                @SuppressWarnings("unchecked")
                Object newConnectorConfig = ((Map<String, Object>) value.value()).get("properties");
                if (!(newConnectorConfig instanceof Map)) {
                    log.error("Invalid data for config for connector '{}': 'properties' field should be a Map but is {}",
                            connectorName, className(newConnectorConfig));
                    return;
                }
                log.debug("Updating configuration for connector '{}'", connectorName);
                @SuppressWarnings("unchecked")
                Map<String, String> stringsConnectorConfig = (Map<String, String>) newConnectorConfig;
                connectorConfigs.put(connectorName, stringsConnectorConfig);

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
    }

    private void processTaskConfigRecord(ConnectorTaskId taskId, SchemaAndValue value) {
        synchronized (lock) {
            if (value.value() == null) {
                log.error("Ignoring task configuration for task {} because it is unexpectedly null", taskId);
                return;
            }
            if (!(value.value() instanceof Map)) {
                log.error("Ignoring task configuration for task {} because the value is not a Map but is {}", taskId, className(value.value()));
                return;
            }

            @SuppressWarnings("unchecked")
            Object newTaskConfig = ((Map<String, Object>) value.value()).get("properties");
            if (!(newTaskConfig instanceof Map)) {
                log.error("Invalid data for config of task {} 'properties' field should be a Map but is {}", taskId, className(newTaskConfig));
                return;
            }

            Map<ConnectorTaskId, Map<String, String>> deferred = deferredTaskUpdates.computeIfAbsent(taskId.connector(), k -> new HashMap<>());
            log.debug("Storing new config for task {}; this will wait for a commit message before the new config will take effect.", taskId);
            @SuppressWarnings("unchecked")
            Map<String, String> stringsTaskConfig = (Map<String, String>) newTaskConfig;
            deferred.put(taskId, stringsTaskConfig);
        }
    }

    private void processTasksCommitRecord(String connectorName, SchemaAndValue value) {
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
                log.error("Ignoring connector tasks configuration commit for connector '{}' because it is in the wrong format: {}", connectorName, className(value.value()));
                return;
            }
            Map<ConnectorTaskId, Map<String, String>> deferred = deferredTaskUpdates.get(connectorName);

            @SuppressWarnings("unchecked")
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
                    connectorTaskConfigGenerations.compute(connectorName, (ignored, generation) -> generation != null ? generation + 1 : 0);
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

        // If task configs appear after the latest task count record, the connector needs a new round of zombie fencing
        // before it can start tasks with these configs
        connectorsPendingFencing.add(connectorName);
        if (started)
            updateListener.onTaskConfigUpdate(updatedTasks);
    }

    @SuppressWarnings("unchecked")
    RestartRequest recordToRestartRequest(ConsumerRecord<String, byte[]> record, SchemaAndValue value) {
        String connectorName = record.key().substring(RESTART_PREFIX.length());
        if (!(value.value() instanceof Map)) {
            log.error("Ignoring restart request because the value is not a Map but is {}", className(value.value()));
            return null;
        }

        Map<String, Object> valueAsMap = (Map<String, Object>) value.value();

        Object failed = valueAsMap.get(ONLY_FAILED_FIELD_NAME);
        boolean onlyFailed;
        if (!(failed instanceof Boolean)) {
            log.warn("Invalid data for restart request '{}' field should be a Boolean but is {}, defaulting to {}", ONLY_FAILED_FIELD_NAME, className(failed), ONLY_FAILED_DEFAULT);
            onlyFailed = ONLY_FAILED_DEFAULT;
        } else {
            onlyFailed = (Boolean) failed;
        }

        Object withTasks = valueAsMap.get(INCLUDE_TASKS_FIELD_NAME);
        boolean includeTasks;
        if (!(withTasks instanceof Boolean)) {
            log.warn("Invalid data for restart request '{}' field should be a Boolean but is {}, defaulting to {}", INCLUDE_TASKS_FIELD_NAME, className(withTasks), INCLUDE_TASKS_DEFAULT);
            includeTasks = INCLUDE_TASKS_DEFAULT;
        } else {
            includeTasks = (Boolean) withTasks;
        }
        return new RestartRequest(connectorName, onlyFailed, includeTasks);
    }

    private void processTaskCountRecord(String connectorName, SchemaAndValue value) {
        if (!(value.value() instanceof Map)) {
            log.error("Ignoring task count record for connector '{}' because it is in the wrong format: {}",  connectorName, className(value.value()));
            return;
        }
        @SuppressWarnings("unchecked")
        int taskCount = intValue(((Map<String, Object>) value.value()).get("task-count"));

        log.debug("Setting task count record for connector '{}' to {}", connectorName, taskCount);
        connectorTaskCountRecords.put(connectorName, taskCount);
        // If a task count record appears after the latest task configs, the connectors doesn't need a round of zombie
        // fencing before it can start tasks with the latest configs
        connectorsPendingFencing.remove(connectorName);
    }

    private void processSessionKeyRecord(SchemaAndValue value) {
        if (value.value() == null) {
            log.error("Ignoring session key because it is unexpectedly null");
            return;
        }
        if (!(value.value() instanceof Map)) {
            log.error("Ignoring session key because the value is not a Map but is {}", className(value.value()));
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> valueAsMap = (Map<String, Object>) value.value();

        Object sessionKey = valueAsMap.get("key");
        if (!(sessionKey instanceof String)) {
            log.error("Invalid data for session key 'key' field should be a String but is {}", className(sessionKey));
            return;
        }
        byte[] key = Base64.getDecoder().decode((String) sessionKey);

        Object keyAlgorithm = valueAsMap.get("algorithm");
        if (!(keyAlgorithm instanceof String)) {
            log.error("Invalid data for session key 'algorithm' field should be a String but it is {}", className(keyAlgorithm));
            return;
        }

        Object creationTimestamp = valueAsMap.get("creation-timestamp");
        if (!(creationTimestamp instanceof Long)) {
            log.error("Invalid data for session key 'creation-timestamp' field should be a long but it is {}", className(creationTimestamp));
            return;
        }
        KafkaConfigBackingStore.this.sessionKey = new SessionKey(
                new SecretKeySpec(key, (String) keyAlgorithm),
                (long) creationTimestamp
        );

        if (started)
            updateListener.onSessionKeyUpdate(KafkaConfigBackingStore.this.sessionKey);
    }

    private void processLoggerLevelRecord(String namespace, SchemaAndValue value) {
        if (value.value() == null) {
            log.error("Ignoring logging level for namespace {} because it is unexpectedly null", namespace);
            return;
        }
        if (!(value.value() instanceof Map)) {
            log.error("Ignoring logging level for namespace {} because the value is not a Map but is {}", namespace, className(value.value()));
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> valueAsMap = (Map<String, Object>) value.value();

        Object level = valueAsMap.get("level");
        if (!(level instanceof String)) {
            log.error("Invalid data for logging level key 'level' field with namespace {}; should be a String but it is {}", namespace, className(level));
            return;
        }

        if (started) {
            updateListener.onLoggingLevelUpdate(namespace, (String) level);
        } else {
            // TRACE level since there may be many of these records in the config topic
            log.trace(
                    "Ignoring old logging level {} for namespace {} that was writen to the config topic before this worker completed startup",
                    level,
                    namespace
            );
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

