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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode.HYBRID_FAIL;
import static org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode.HYBRID_WARN;
import static org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode.ONLY_SCAN;
import static org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode.SERVICE_LOAD;

/**
 * Common base class providing configuration for Kafka Connect workers, whether standalone or distributed.
 */
public class WorkerConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(WorkerConfig.class);

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC = 
                "A list of host/port pairs used to establish the initial connection to the Kafka cluster. "
                        + "Clients use this list to bootstrap and discover the full set of Kafka brokers. "
                        + "While the order of servers in the list does not matter, we recommend including more than one server to ensure resilience if any servers are down. "
                        + "This list does not need to contain the entire set of brokers, as Kafka clients automatically manage and update connections to the cluster efficiently. "
                        + "This list must be in the form <code>host1:port1,host2:port2,...</code>.";
    public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;
    public static final String CLIENT_DNS_LOOKUP_DOC = CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC;

    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    public static final String KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";
    public static final String VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro.";

    public static final String HEADER_CONVERTER_CLASS_CONFIG = "header.converter";
    public static final String HEADER_CONVERTER_CLASS_DOC =
            "HeaderConverter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the header values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro. By default, the SimpleHeaderConverter is used to serialize" +
                    " header values to strings and deserialize them by inferring the schemas.";
    public static final String HEADER_CONVERTER_CLASS_DEFAULT = SimpleHeaderConverter.class.getName();

    public static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG
            = "task.shutdown.graceful.timeout.ms";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC =
            "Amount of time to wait for tasks to shutdown gracefully. This is the total amount of time,"
                    + " not per task. All task have shutdown triggered, then they are waited on sequentially.";
    private static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT = "5000";

    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms";
    private static final String OFFSET_COMMIT_INTERVAL_MS_DOC
            = "Interval at which to try committing offsets for tasks.";
    public static final long OFFSET_COMMIT_INTERVAL_MS_DEFAULT = 60000L;

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";
    private static final String OFFSET_COMMIT_TIMEOUT_MS_DOC
            = "Maximum number of milliseconds to wait for records to flush and partition offset data to be"
            + " committed to offset storage before cancelling the process and restoring the offset "
            + "data to be committed in a future attempt. This property has no effect for source connectors "
            + "running with exactly-once support.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    public static final String PLUGIN_PATH_CONFIG = "plugin.path";
    protected static final String PLUGIN_PATH_DOC = "List of paths separated by commas (,) that "
            + "contain plugins (connectors, converters, transformations). The list should consist"
            + " of top level directories that include any combination of: \n"
            + "a) directories immediately containing jars with plugins and their dependencies\n"
            + "b) uber-jars with plugins and their dependencies\n"
            + "c) directories immediately containing the package directory structure of classes of "
            + "plugins and their dependencies\n"
            + "Note: symlinks will be followed to discover dependencies or plugins.\n"
            + "Examples: plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,"
            + "/opt/connectors\n" 
            + "Do not use config provider variables in this property, since the raw path is used "
            + "by the worker's scanner before config providers are initialized and used to "
            + "replace variables.";

    public static final String PLUGIN_DISCOVERY_CONFIG = "plugin.discovery";
    protected static final String PLUGIN_DISCOVERY_DOC = "Method to use to discover plugins present in the classpath "
            + "and plugin.path configuration. This can be one of multiple values with the following meanings:\n"
            + "* " + ONLY_SCAN + ": Discover plugins only by reflection. "
            + "Plugins which are not discoverable by ServiceLoader will not impact worker startup.\n"
            + "* " + HYBRID_WARN + ": Discover plugins reflectively and by ServiceLoader. "
            + "Plugins which are not discoverable by ServiceLoader will print warnings during worker startup.\n"
            + "* " + HYBRID_FAIL + ": Discover plugins reflectively and by ServiceLoader. "
            + "Plugins which are not discoverable by ServiceLoader will cause worker startup to fail.\n"
            + "* " + SERVICE_LOAD + ": Discover plugins only by ServiceLoader. Faster startup than other modes. "
            + "Plugins which are not discoverable by ServiceLoader may not be usable.";

    public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";
    protected static final String CONFIG_PROVIDERS_DOC =
            "Comma-separated names of <code>ConfigProvider</code> classes, loaded and used "
            + "in the order specified. Implementing the interface  "
            + "<code>ConfigProvider</code> allows you to replace variable references in connector configurations, "
            + "such as for externalized secrets. ";

    public static final String CONNECTOR_CLIENT_POLICY_CLASS_CONFIG = "connector.client.config.override.policy";
    public static final String CONNECTOR_CLIENT_POLICY_CLASS_DOC =
        "Class name or alias of implementation of <code>ConnectorClientConfigOverridePolicy</code>. Defines what client configurations can be "
        + "overridden by the connector. The default implementation is `All`, meaning connector configurations can override all client properties. "
        + "The other possible policies in the framework include `None` to disallow connectors from overriding client properties, "
        + "and `Principal` to allow connectors to override only client principals.";
    public static final String CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT = "All";


    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    public static final String TOPIC_TRACKING_ENABLE_CONFIG = "topic.tracking.enable";
    protected static final String TOPIC_TRACKING_ENABLE_DOC = "Enable tracking the set of active "
            + "topics per connector during runtime.";
    protected static final boolean TOPIC_TRACKING_ENABLE_DEFAULT = true;

    public static final String TOPIC_TRACKING_ALLOW_RESET_CONFIG = "topic.tracking.allow.reset";
    protected static final String TOPIC_TRACKING_ALLOW_RESET_DOC = "If set to true, it allows "
            + "user requests to reset the set of active topics per connector.";
    protected static final boolean TOPIC_TRACKING_ALLOW_RESET_DEFAULT = true;

    public static final String CONNECT_KAFKA_CLUSTER_ID = "connect.kafka.cluster.id";
    public static final String CONNECT_GROUP_ID = "connect.group.id";

    public static final String TOPIC_CREATION_ENABLE_CONFIG = "topic.creation.enable";
    protected static final String TOPIC_CREATION_ENABLE_DOC = "Whether to allow "
            + "automatic creation of topics used by source connectors, when source connectors "
            + "are configured with `" + TOPIC_CREATION_PREFIX + "` properties. Each task will use an "
            + "admin client to create its topics and will not depend on the Kafka brokers "
            + "to create topics automatically.";
    protected static final boolean TOPIC_CREATION_ENABLE_DEFAULT = true;

    /**
     * Get a basic ConfigDef for a WorkerConfig. This includes all the common settings. Subclasses can use this to
     * bootstrap their own ConfigDef.
     * @return a ConfigDef with all the common options specified
     */
    protected static ConfigDef baseConfigDef() {
        ConfigDef result = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        Type.STRING,
                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        Importance.MEDIUM,
                        CLIENT_DNS_LOOKUP_DOC)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, KEY_CONVERTER_CLASS_DOC)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, VALUE_CONVERTER_CLASS_DOC)
                .define(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Type.LONG,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT, Importance.LOW,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
                .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC)
                .define(PLUGIN_PATH_CONFIG,
                        Type.LIST,
                        null,
                        Importance.LOW,
                        PLUGIN_PATH_DOC)
                .define(PLUGIN_DISCOVERY_CONFIG,
                        Type.STRING,
                        PluginDiscoveryMode.HYBRID_WARN.toString(),
                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(PluginDiscoveryMode.class)),
                        Importance.LOW,
                        PLUGIN_DISCOVERY_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG, Type.LONG,
                        30000, atLeast(0), Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG, Type.INT,
                        2, atLeast(1), Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG, Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG, Type.LIST,
                        JmxReporter.class.getName(), Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        HEADER_CONVERTER_CLASS_DEFAULT,
                        Importance.LOW, HEADER_CONVERTER_CLASS_DOC)
                .define(CONFIG_PROVIDERS_CONFIG, Type.LIST,
                        Collections.emptyList(),
                        Importance.LOW, CONFIG_PROVIDERS_DOC)
                .define(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, Type.STRING, CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT,
                        Importance.MEDIUM, CONNECTOR_CLIENT_POLICY_CLASS_DOC)
                .define(TOPIC_CREATION_ENABLE_CONFIG, Type.BOOLEAN, TOPIC_CREATION_ENABLE_DEFAULT, Importance.LOW,
                        TOPIC_CREATION_ENABLE_DOC)
                // security support
                .withClientSslSupport();
        addTopicTrackingConfig(result);
        RestServerConfig.addPublicConfig(result);
        return result;
    }

    public static void addTopicTrackingConfig(ConfigDef configDef) {
        configDef
                .define(
                        TOPIC_TRACKING_ENABLE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        TOPIC_TRACKING_ENABLE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TOPIC_TRACKING_ENABLE_DOC
                ).define(
                        TOPIC_TRACKING_ALLOW_RESET_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        TOPIC_TRACKING_ALLOW_RESET_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TOPIC_TRACKING_ALLOW_RESET_DOC);
    }

    private String kafkaClusterId;

    // Visible for testing
    static String lookupKafkaClusterId(WorkerConfig config) {
        log.info("Creating Kafka admin client");
        try (Admin adminClient = Admin.create(config.originals())) {
            return lookupKafkaClusterId(adminClient);
        }
    }

    // Visible for testing
    static String lookupKafkaClusterId(Admin adminClient) {
        log.debug("Looking up Kafka cluster ID");
        try {
            KafkaFuture<String> clusterIdFuture = adminClient.describeCluster().clusterId();
            if (clusterIdFuture == null) {
                log.info("Kafka cluster version is too old to return cluster ID");
                return null;
            }
            log.debug("Fetching Kafka cluster ID");
            String kafkaClusterId = clusterIdFuture.get();
            log.info("Kafka cluster ID: {}", kafkaClusterId);
            return kafkaClusterId;
        } catch (InterruptedException e) {
            throw new ConnectException("Unexpectedly interrupted when looking up Kafka cluster info", e);
        } catch (ExecutionException e) {
            throw new ConnectException("Failed to connect to and describe Kafka cluster. "
                                       + "Check worker's broker connection and security properties.", e);
        }
    }

    private void logInternalConverterRemovalWarnings(Map<String, String> props) {
        List<String> removedProperties = new ArrayList<>();
        for (String property : Arrays.asList("internal.key.converter", "internal.value.converter")) {
            if (props.containsKey(property)) {
                removedProperties.add(property);
            }
            removedProperties.addAll(originalsWithPrefix(property + ".").keySet());
        }
        if (!removedProperties.isEmpty()) {
            log.warn(
                    "The worker has been configured with one or more internal converter properties ({}). "
                            + "Support for these properties was deprecated in version 2.0 and removed in version 3.0, "
                            + "and specifying them will have no effect. "
                            + "Instead, an instance of the JsonConverter with schemas.enable "
                            + "set to false will be used. For more information, please visit "
                            + "https://kafka.apache.org/documentation/#upgrade and consult the upgrade notes"
                            + "for the 3.0 release.",
                    removedProperties);
        }
    }

    private void logPluginPathConfigProviderWarning(Map<String, String> rawOriginals) {
        String rawPluginPath = rawOriginals.get(PLUGIN_PATH_CONFIG);
        // Can't use AbstractConfig::originalsStrings here since some values may be null, which
        // causes that method to fail
        String transformedPluginPath = Objects.toString(originals().get(PLUGIN_PATH_CONFIG), null);
        if (!Objects.equals(rawPluginPath, transformedPluginPath)) {
            log.warn(
                "Variables cannot be used in the 'plugin.path' property, since the property is "
                + "used by plugin scanning before the config providers that replace the " 
                + "variables are initialized. The raw value '{}' was used for plugin scanning, as " 
                + "opposed to the transformed value '{}', and this may cause unexpected results.",
                rawPluginPath,
                transformedPluginPath
            );
        }
    }

    /**
     * @return the {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG bootstrap servers} property
     * used by the worker when instantiating Kafka clients for connectors and tasks (unless overridden)
     * and its internal topics (if running in distributed mode)
     */
    public String bootstrapServers() {
        return String.join(",", getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    public Integer rebalanceTimeout() {
        return null;
    }

    public boolean topicCreationEnable() {
        return getBoolean(TOPIC_CREATION_ENABLE_CONFIG);
    }

    /**
     * Whether this worker is configured with exactly-once support for source connectors.
     * The default implementation returns {@code false} and should be overridden by subclasses
     * if the worker mode for the subclass provides exactly-once support for source connectors.
     * @return whether exactly-once support is enabled for source connectors on this worker
     */
    public boolean exactlyOnceSourceEnabled() {
        return false;
    }

    /**
     * Get the internal topic used by this worker to store source connector offsets.
     * The default implementation returns {@code null} and should be overridden by subclasses
     * if the worker mode for the subclass uses an internal offsets topic.
     * @return the name of the internal offsets topic, or {@code null} if the worker does not use
     * an internal offsets topic
     */
    public String offsetsTopic() {
        return null;
    }

    /**
     * Determine whether this worker supports per-connector source offsets topics.
     * The default implementation returns {@code false} and should be overridden by subclasses
     * if the worker mode for the subclass supports per-connector offsets topics.
     * @return whether the worker supports per-connector offsets topics
     */
    public boolean connectorOffsetsTopicsPermitted() {
        return false;
    }

    /**
     * @return the offset commit interval for tasks created by this worker
     */
    public long offsetCommitInterval() {
        return getLong(OFFSET_COMMIT_INTERVAL_MS_CONFIG);
    }

    /**
     * Get the {@link CommonClientConfigs#GROUP_ID_CONFIG group ID} used by this worker to form a cluster.
     * The default implementation returns {@code null} and should be overridden by subclasses
     * if the worker mode for the subclass is capable of forming a cluster using Kafka's group coordination API.
     * @return the group ID for the worker's cluster, or {@code null} if the worker is not capable of forming a cluster.
     */
    public String groupId() {
        return null;
    }

    public String kafkaClusterId() {
        if (kafkaClusterId == null) {
            kafkaClusterId = lookupKafkaClusterId(this);
        }
        return kafkaClusterId;
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public static String pluginPath(Map<String, String> props) {
        return props.get(WorkerConfig.PLUGIN_PATH_CONFIG);
    }

    public static PluginDiscoveryMode pluginDiscovery(Map<String, String> props) {
        String value = props.getOrDefault(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.HYBRID_WARN.toString());
        try {
            return PluginDiscoveryMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Invalid " + PLUGIN_DISCOVERY_CONFIG + " value, must be one of "
                    + Arrays.toString(Utils.enumOptions(PluginDiscoveryMode.class)));
        }
    }

    @SuppressWarnings("this-escape")
    public WorkerConfig(ConfigDef definition, Map<String, String> props) {
        super(definition, props, Utils.castToStringObjectMap(props), true);
        logInternalConverterRemovalWarnings(props);
        logPluginPathConfigProviderWarning(props);
    }

    @Override
    public Map<String, Object> originals() {
        Map<String, Object> map = super.originals();
        map.remove(AbstractConfig.CONFIG_PROVIDERS_CONFIG);
        return map;
    }
}
