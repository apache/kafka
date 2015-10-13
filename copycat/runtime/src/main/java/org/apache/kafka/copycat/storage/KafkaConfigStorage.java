/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaAndValue;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.DataException;
import org.apache.kafka.copycat.runtime.distributed.ClusterConfigState;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.KafkaBasedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 *     Provides persistent storage of Copycat connector configurations in a Kafka topic.
 * </p>
 * <p>
 *     This class manages both connector and task configurations. It tracks three types of configuration entries:
 *
 *     1. Root config: list of connector IDs, each with the current number of tasks (Kafka key: root).
 *                          This config is used only to track which task configs are in a consistent state and is
 *                          therefore effectively ephemeral -- if it were deleted, we would treat everything as
 *                          inconsistent until all connector's tasks were updated and a new record specifying
 *                          the number of tasks per connector was written.
 *     2. Connector config: map of string -> string configurations passed to the Connector class, with support for
 *                          expanding this format if necessary. (Kafka key: connector-[connector-id]).
 *                          These configs are *not* ephemeral. They represent the source of truth. If the entire Copycat
 *                          cluster goes down, this is all that is really needed to recover.
 *     3. Task configs: map of string -> string configurations passed to the Task class, with support for expanding
 *                          this format if necessary. (Kafka key: task-[connector-id]-[task-id]).
 *                          These configs are ephemeral; they are stored here to a) disseminate them to all workers while
 *                          ensuring agreement and b) to allow faster cluster/worker recovery since the common case
 *                          of recovery (restoring a connector) will simply result in the same configuration as before
 *                          the failure.
 * </p>
 * <p>
 *     The root configuration is used to trigger *task* updates since we require a combination of a compacted topic and atomic
 *     updates. To accomplish this, we do *not* apply updates to each configuration immediately as they are read from the
 *     Kafka log by this class. Instead, the are accumulated in a buffer in this class until we can be sure it is safe
 *     to atomically apply all outstanding updates, which is indicated by an update of the root config. This prevents
 *     problematic partial updates (e.g. some task configurations are updated, the leader dies, and others are left in
 *     their old state; if the new leader used the current state, some partitions could be assigned to multiple workers).
 * </p>
 * <p>
 *     Note that this has two implications. First, every instance tailing the log must check for inconsistencies --
 *     under normal circumstances they could not encounter them, but when they first load the log they could find it in
 *     an inconsistent state. Second, the leader instance (as determined by the calling Herder) must ensure that any
 *     inconsistencies are resolved, which it may do by requesting any necessary reconfiguration, writing the new values,
 *     and then updating the root config. Note that due to the way the root configuration is used, the leader *must*
 *     ensure that *all* inconsistent configs have been correctly updated before proceeding.
 * </p>
 * <p>
 *     Connector config changes are handled differently since they are single entries -- any writes are atomic, so they
 *     can safely be applied immediately and do not require the root config to be rewritten.
 * </p>
 * <p>
 *     Note that the expectation is that this config storage system has only a single writer at a time.
 *     The caller (Herder) must ensure this is the case. In distributed mode this will require forwarding config change
 *     requests to a leader.
 * </p>
 * <p>
 *     Since processing of the config log occurs in a background thread, callers must take care when using accessors.
 *     To simplify handling this correctly, this class only exposes a mechanism to snapshot the current state of the cluster.
 *     Updates may continue to be applied (and callbacks invoked) in the background. Callers must take care that they are
 *     using a consistent snapshot and only update when it is safe. In particular, if task configs are updated which require
 *     synchronization across workers to commit offsets and update the configuration, callbacks and updates during the
 *     rebalance must be deferred.
 * </p>
 */
public class KafkaConfigStorage {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigStorage.class);

    public static final String CONFIG_TOPIC_CONFIG = "config.storage.topic";

    public static final String ROOT_KEY = "root";
    public static final String CONNECTOR_PREFIX = "connector-";
    public static String CONNECTOR_KEY(String connectorName) {
        return CONNECTOR_PREFIX + connectorName;
    }
    public static final String TASK_PREFIX = "task-";
    public static String TASK_KEY(ConnectorTaskId taskId) {
        return TASK_PREFIX + taskId.connector() + "-" + taskId.task();
    }

    // Note that while using real serialization for values as we have here, but ad hoc string serialization for keys,
    // isn't ideal, we use this approach because it avoids any potential problems with schema evolution or
    // converter/serializer changes causing keys to change. We need to absolutely ensure that the keys remain precisely
    // the same.
    public static final Schema ROOT_V0 = SchemaBuilder.struct()
            .field("connectors", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    public static final Schema CONNECTOR_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA))
            .build();
    public static final Schema TASK_CONFIGURATION_V0 = CONNECTOR_CONFIGURATION_V0;

    private static final long READ_TO_END_TIMEOUT_MS = 30000;

    private final Object lock;
    private boolean starting;
    private final Converter converter;
    private final Callback<String> connectorConfigCallback;
    private final Callback<List<ConnectorTaskId>> tasksConfigCallback;
    private String topic;
    // Data is passed to the log already serialized. We use a converter to handle translating to/from generic Copycat
    // format to serialized form
    private KafkaBasedLog<String, byte[]> configLog;
    // Root config: connector -> # of tasks
    private Map<String, Integer> rootConfig = new HashMap<>();
    // Connector and task configs: name or id -> config map
    private Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
    private Map<ConnectorTaskId, Map<String, String>> taskConfigs = new HashMap<>();
    // Offsets for most recently applied root and connector offsets, which are included in snapshots and can be used
    // to efficiently ensure different worker nodes are working with the same set of configs
    private long rootOffset;
    private long connectorOffset;

    private Map<ConnectorTaskId, Map<String, String>> deferredTaskUpdates = new HashMap<>();


    public KafkaConfigStorage(Converter converter, Callback<String> connectorConfigCallback, Callback<List<ConnectorTaskId>> tasksConfigCallback) {
        this.lock = new Object();
        this.starting = false;
        this.converter = converter;
        this.connectorConfigCallback = connectorConfigCallback;
        this.tasksConfigCallback = tasksConfigCallback;

        rootOffset = -1;
        connectorOffset = -1;
    }

    public void configure(Map<String, ?> configs) {
        if (configs.get(CONFIG_TOPIC_CONFIG) == null)
            throw new CopycatException("Must specify topic for Copycat connector configuration.");
        topic = (String) configs.get(CONFIG_TOPIC_CONFIG);

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(configs);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(configs);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        configLog = createKafkaBasedLog(topic, producerProps, consumerProps, consumedCallback);
    }

    public void start() {
        log.info("Starting KafkaConfigStorage");
        // During startup, callbacks are *not* invoked. You can grab a snapshot after starting -- just take care that
        // updates can continue to occur in the background
        starting = true;
        configLog.start();
        starting = false;
        log.info("Started KafkaConfigStorage");
    }

    public void stop() {
        log.info("Closing KafkaConfigStorage");
        configLog.stop();
        log.info("Closed KafkaConfigStorage");
    }

    /**
     * Get a snapshot of the current state of the cluster.
     */
    public ClusterConfigState snapshot() {
        synchronized (lock) {
            // Doing a shallow copy of the data is safe here because the complex nested data that is copied should all be
            // immutable configs
            return new ClusterConfigState(
                    rootOffset,
                    connectorOffset,
                    new HashMap<>(rootConfig),
                    new HashMap<>(connectorConfigs),
                    new HashMap<>(taskConfigs)
            );
        }
    }

    /**
     * Write this connector configuration to persistent storage and wait until it has been acknowledge and read back by
     * tailing the Kafka log with a consumer.
     * @param connector name of the connector to write data for
     * @param properties the configuration to write
     */
    public void putConnectorConfig(String connector, Map<String, String> properties) {
        Struct copycatConfig = new Struct(CONNECTOR_CONFIGURATION_V0);
        copycatConfig.put("properties", properties);
        byte[] serializedConfig = converter.fromCopycatData(topic, CONNECTOR_CONFIGURATION_V0, copycatConfig);

        try {
            configLog.send(CONNECTOR_KEY(connector), serializedConfig);
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write connector configuration to Kafka: ", e);
            throw new CopycatException("Error writing connector configuration to Kafka", e);
        }
    }

    /**
     * Write these task configurations and update the root configuration, unless an inconsistency is found that indicates
     * that not all connectors that need reconfiguration have been reconfigured.
     *
     * @param configs map containing task configurations
     * @throws CopycatException if the task configurations do not resolve inconsistencies found in the existing root
     *         and task configurations.
     */
    public void putTaskConfigs(Map<ConnectorTaskId, Map<String, String>> configs) {
        // Make sure we're at the end of the log. We should be the only writer, but we want to make sure we don't have
        // any outstanding lagging data to consume.
        try {
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write root configuration to Kafka: ", e);
            throw new CopycatException("Error writing root configuration to Kafka", e);
        }

        // In theory, there is only a single writer and we shouldn't need this lock since the background thread should
        // not invoke any callbacks that would conflict, but in practice this guards against inconsistencies due to
        // the root config being updated.
        Map<String, Integer> newRootConfig = new HashMap<>();
        synchronized (lock) {
            // Validate tasks in this assignment. Any task configuration updates should include updates for *all* tasks
            // in the connector -- we should have all task IDs 0 - N-1 within a connector if any task is included here
            Map<String, Set<Integer>> updatedConfigIdsByConnector = new HashMap<>();
            for (Map.Entry<ConnectorTaskId, Map<String, String>> taskConfigEntry : configs.entrySet()) {
                ConnectorTaskId taskId = taskConfigEntry.getKey();
                if (!updatedConfigIdsByConnector.containsKey(taskId.connector()))
                    updatedConfigIdsByConnector.put(taskId.connector(), new TreeSet<Integer>());
                updatedConfigIdsByConnector.get(taskId.connector()).add(taskId.task());
            }
            Map<String, Integer> updatedRootConfigEntries = new HashMap<>();
            for (Map.Entry<String, Set<Integer>> taskConfigSetEntry : updatedConfigIdsByConnector.entrySet()) {
                for (Integer elem : taskConfigSetEntry.getValue()) {
                    if (elem < 0 || elem >= taskConfigSetEntry.getValue().size()) {
                        log.error("Submitted task configuration contain invalid range of task IDs, ignoring this submission");
                        throw new CopycatException("Error writing task configurations: found some connectors with invalid connectors");
                    }
                }
                updatedRootConfigEntries.put(taskConfigSetEntry.getKey(), taskConfigSetEntry.getValue().size());
            }

            // Generate new root config
            newRootConfig.putAll(rootConfig);
            newRootConfig.putAll(updatedRootConfigEntries);

            // Second half of validation: if we find some missing task configurations even with a) outstanding deferred
            // updates and b) these updates, then we shouldn't generate an updated root config
            boolean inconsistent = false;
            for (Map.Entry<String, Integer> newRootEntry : newRootConfig.entrySet()) {
                String connName = newRootEntry.getKey();
                for (int taskIndex = 0; taskIndex < newRootEntry.getValue(); taskIndex++) {
                    ConnectorTaskId taskId = new ConnectorTaskId(connName, taskIndex);
                    // Note that we *do not* include deferred updates here. This is important because a half completed
                    // write of updated task configs could leave some left over data in the list of deferred task updates
                    // which would then make this appear to have a complete set of task configs. In order to ensure correctness,
                    // this method call or the existing task configs need to ensure everything that is inconsistent is
                    // addressed in one batch.
                    Map<String, String> taskConfig = configs.containsKey(taskId) ? configs.get(taskId) : taskConfigs.get(taskId);
                    if (taskConfig == null) {
                        log.error("Found inconsistent/incomplete assignment, missing config for task " + taskId);
                        inconsistent = true;
                    }
                }
            }
            if (inconsistent)
                throw new CopycatException("Error writing task configurations: found some connectors with incomplete task assignments");
        }

        // Start sending all the individual updates
        for (Map.Entry<ConnectorTaskId, Map<String, String>> taskConfigEntry : configs.entrySet()) {
            Struct copycatConfig = new Struct(TASK_CONFIGURATION_V0);
            copycatConfig.put("properties", taskConfigEntry.getValue());
            byte[] serializedConfig = converter.fromCopycatData(topic, TASK_CONFIGURATION_V0, copycatConfig);
            configLog.send(TASK_KEY(taskConfigEntry.getKey()), serializedConfig);
        }

        // Finally, send the root update, then wait until we read to the end of the log
        try {
            // Read to end to ensure all the task configs have been written
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Struct copycatConfig = new Struct(ROOT_V0);
            copycatConfig.put("connectors", newRootConfig);
            byte[] serializedConfig = converter.fromCopycatData(topic, ROOT_V0, copycatConfig);
            configLog.send(ROOT_KEY, serializedConfig);

            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write root configuration to Kafka: ", e);
            throw new CopycatException("Error writing root configuration to Kafka", e);
        }
    }

    private KafkaBasedLog<String, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps, Callback<ConsumerRecord<String, byte[]>> consumedCallback) {
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, consumedCallback, new SystemTime());
    }

    private final Callback<ConsumerRecord<String, byte[]>> consumedCallback = new Callback<ConsumerRecord<String, byte[]>>() {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> record) {
            if (error != null) {
                log.error("Unexpected in consumer callback for KafkaConfigStorage: ", error);
                return;
            }

            final SchemaAndValue value;
            try {
                value = converter.toCopycatData(topic, record.value());
            } catch (DataException e) {
                log.error("Failed to convert config data to Copycat format: ", e);
                return;
            }

            if (record.key().equals(ROOT_KEY)) {
                List<ConnectorTaskId> updatedTasks;
                synchronized (lock) {
                    // Apply any outstanding deferred task updates and then update the root config. This *can* result in
                    // an inconsistent output, which will have to be resolved by some external mechanism ensuring reconfiguration
                    // of the right connectors takes place.
                    //
                    // Note that we *MUST* always apply root updates. They should only occur when they are valid and if we
                    // didn't, we could lose track of some connectors.
                    if (!(value.value() instanceof Map)) { // Schema-less, so we get maps instead of structs
                        log.error("Ignoring root configuration because it is in the wrong format: " + value.value());
                        return;
                    }

                    taskConfigs.putAll(deferredTaskUpdates);
                    Object newRootConfig = ((Map<String, Object>) value.value()).get("connectors");
                    if (!(newRootConfig instanceof Map)) {
                        log.error("Invalid data for root config: connectors field should be a Map but is " + newRootConfig.getClass());
                        return;
                    }
                    rootConfig = (Map<String, Integer>) newRootConfig;
                    rootOffset = record.offset();
                    updatedTasks = new ArrayList<>(deferredTaskUpdates.keySet());
                    deferredTaskUpdates.clear();
                }

                if (!starting)
                    tasksConfigCallback.onCompletion(null, updatedTasks);
            } else if (record.key().startsWith(CONNECTOR_PREFIX)) {
                String connectorName = record.key().substring(CONNECTOR_PREFIX.length());
                synchronized (lock) {
                    // Connector configs can be applied and callbacks invoked immediately
                    if (!(value.value() instanceof Map)) {
                        log.error("Found connector configuration (" + record.key() + ") in wrong format: " + value.value().getClass());
                        return;
                    }
                    Object newConnectorConfig = ((Map<String, Object>) value.value()).get("properties");
                    if (!(newConnectorConfig instanceof Map)) {
                        log.error("Invalid data for connector config: properties filed should be a Map but is " + newConnectorConfig.getClass());
                        return;
                    }
                    connectorConfigs.put(connectorName, (Map<String, String>) newConnectorConfig);
                    connectorOffset = record.offset();
                }
                if (!starting)
                    connectorConfigCallback.onCompletion(null, connectorName);
            } else if (record.key().startsWith(TASK_PREFIX)) {
                synchronized (lock) {
                    ConnectorTaskId taskId = parseTaskId(record.key());
                    if (taskId == null) {
                        log.error("Ignoring task configuration because " + record.key() + " couldn't be parsed as a task config key");
                        return;
                    }
                    if (!(value.value() instanceof Map)) {
                        log.error("Ignoring task configuration because it is in the wrong format: " + value.value());
                        return;
                    }

                    Object newTaskConfig = ((Map<String, Object>) value.value()).get("properties");
                    if (!(newTaskConfig instanceof Map)) {
                        log.error("Invalid data for task config: properties filed should be a Map but is " + newTaskConfig.getClass());
                        return;
                    }

                    deferredTaskUpdates.put(taskId, (Map<String, String>) newTaskConfig);
                }
            } else {
                log.error("Discarding config update record with invalid key: " + record.key());
            }
        }
    };

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

    private static class ConfigUpdate {
        final String target;
        final SchemaAndValue value;

        ConfigUpdate(String target, SchemaAndValue value) {
            this.target = target;
            this.value = value;
        }
    }
}

