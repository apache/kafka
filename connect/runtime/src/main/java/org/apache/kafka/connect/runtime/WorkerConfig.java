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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Common base class providing configuration for Kafka Connect workers, whether standalone or distributed.
 */
public class WorkerConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(WorkerConfig.class);

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC
            = "A list of host/port pairs to use for establishing the initial connection to the Kafka "
            + "cluster. The client will make use of all servers irrespective of which servers are "
            + "specified here for bootstrapping&mdash;this list only impacts the initial hosts used "
            + "to discover the full set of servers. This list should be in the form "
            + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the "
            + "initial connection to discover the full cluster membership (which may change "
            + "dynamically), this list need not contain the full set of servers (you may want more "
            + "than one, though, in case a server is down).";
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

    /**
     * @deprecated As of 2.0.0
     */
    @Deprecated
    public static final String INTERNAL_KEY_CONVERTER_CLASS_CONFIG = "internal.key.converter";
    public static final String INTERNAL_KEY_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the keys in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation." +
                    " Deprecated; will be removed in an upcoming version.";

    /**
     * @deprecated As of 2.0.0
     */
    @Deprecated
    public static final String INTERNAL_VALUE_CONVERTER_CLASS_CONFIG = "internal.value.converter";
    public static final String INTERNAL_VALUE_CONVERTER_CLASS_DOC =
            "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka." +
                    " This controls the format of the values in messages written to or read from Kafka, and since this is" +
                    " independent of connectors it allows any connector to work with any serialization format." +
                    " Examples of common formats include JSON and Avro." +
                    " This setting controls the format used for internal bookkeeping data used by the framework, such as" +
                    " configs and offsets, so users can typically use any functioning Converter implementation." +
                    " Deprecated; will be removed in an upcoming version.";

    private static final Class<? extends Converter> INTERNAL_CONVERTER_DEFAULT = JsonConverter.class;

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
            + "data to be committed in a future attempt.";
    public static final long OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000L;

    /**
     * @deprecated As of 1.1.0.
     */
    @Deprecated
    public static final String REST_HOST_NAME_CONFIG = "rest.host.name";
    private static final String REST_HOST_NAME_DOC
            = "Hostname for the REST API. If this is set, it will only bind to this interface.";

    /**
     * @deprecated As of 1.1.0.
     */
    @Deprecated
    public static final String REST_PORT_CONFIG = "rest.port";
    private static final String REST_PORT_DOC
            = "Port for the REST API to listen on.";
    public static final int REST_PORT_DEFAULT = 8083;

    public static final String LISTENERS_CONFIG = "listeners";
    private static final String LISTENERS_DOC
            = "List of comma-separated URIs the REST API will listen on. The supported protocols are HTTP and HTTPS.\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists: HTTP://myhost:8083,HTTPS://myhost:8084";

    public static final String REST_ADVERTISED_HOST_NAME_CONFIG = "rest.advertised.host.name";
    private static final String REST_ADVERTISED_HOST_NAME_DOC
            = "If this is set, this is the hostname that will be given out to other workers to connect to.";

    public static final String REST_ADVERTISED_PORT_CONFIG = "rest.advertised.port";
    private static final String REST_ADVERTISED_PORT_DOC
            = "If this is set, this is the port that will be given out to other workers to connect to.";

    public static final String REST_ADVERTISED_LISTENER_CONFIG = "rest.advertised.listener";
    private static final String REST_ADVERTISED_LISTENER_DOC
            = "Sets the advertised listener (HTTP or HTTPS) which will be given to other workers to use.";

    public static final String ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG = "access.control.allow.origin";
    protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DOC =
            "Value to set the Access-Control-Allow-Origin header to for REST API requests." +
                    "To enable cross origin access, set this to the domain of the application that should be permitted" +
                    " to access the API, or '*' to allow access from any domain. The default value only allows access" +
                    " from the domain of the REST API.";
    protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT = "";

    public static final String ACCESS_CONTROL_ALLOW_METHODS_CONFIG = "access.control.allow.methods";
    protected static final String ACCESS_CONTROL_ALLOW_METHODS_DOC =
        "Sets the methods supported for cross origin requests by setting the Access-Control-Allow-Methods header. "
        + "The default value of the Access-Control-Allow-Methods header allows cross origin requests for GET, POST and HEAD.";
    protected static final String ACCESS_CONTROL_ALLOW_METHODS_DEFAULT = "";

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
            + "/opt/connectors";

    public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";
    protected static final String CONFIG_PROVIDERS_DOC =
            "Comma-separated names of <code>ConfigProvider</code> classes, loaded and used "
            + "in the order specified. Implementing the interface  "
            + "<code>ConfigProvider</code> allows you to replace variable references in connector configurations, "
            + "such as for externalized secrets. ";

    public static final String REST_EXTENSION_CLASSES_CONFIG = "rest.extension.classes";
    protected static final String REST_EXTENSION_CLASSES_DOC =
            "Comma-separated names of <code>ConnectRestExtension</code> classes, loaded and called "
            + "in the order specified. Implementing the interface  "
            + "<code>ConnectRestExtension</code> allows you to inject into Connect's REST API user defined resources like filters. "
            + "Typically used to add custom capability like logging, security, etc. ";

    public static final String CONNECTOR_CLIENT_POLICY_CLASS_CONFIG = "connector.client.config.override.policy";
    public static final String CONNECTOR_CLIENT_POLICY_CLASS_DOC =
        "Class name or alias of implementation of <code>ConnectorClientConfigOverridePolicy</code>. Defines what client configurations can be "
        + "overriden by the connector. The default implementation is `None`. The other possible policies in the framework include `All` "
        + "and `Principal`. ";
    public static final String CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT = "None";


    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * Get a basic ConfigDef for a WorkerConfig. This includes all the common settings. Subclasses can use this to
     * bootstrap their own ConfigDef.
     * @return a ConfigDef with all the common options specified
     */
    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, BOOTSTRAP_SERVERS_DEFAULT,
                        Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        Type.STRING,
                        ClientDnsLookup.DEFAULT.toString(),
                        in(ClientDnsLookup.DEFAULT.toString(),
                           ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        Importance.MEDIUM,
                        CLIENT_DNS_LOOKUP_DOC)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, KEY_CONVERTER_CLASS_DOC)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        Importance.HIGH, VALUE_CONVERTER_CLASS_DOC)
                .define(INTERNAL_KEY_CONVERTER_CLASS_CONFIG, Type.CLASS, INTERNAL_CONVERTER_DEFAULT,
                        Importance.LOW, INTERNAL_KEY_CONVERTER_CLASS_DOC)
                .define(INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS, INTERNAL_CONVERTER_DEFAULT,
                        Importance.LOW, INTERNAL_VALUE_CONVERTER_CLASS_DOC)
                .define(TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG, Type.LONG,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DEFAULT, Importance.LOW,
                        TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG, Type.LONG, OFFSET_COMMIT_INTERVAL_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_INTERVAL_MS_DOC)
                .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, Type.LONG, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                        Importance.LOW, OFFSET_COMMIT_TIMEOUT_MS_DOC)
                .define(REST_HOST_NAME_CONFIG, Type.STRING, null, Importance.LOW, REST_HOST_NAME_DOC)
                .define(REST_PORT_CONFIG, Type.INT, REST_PORT_DEFAULT, Importance.LOW, REST_PORT_DOC)
                .define(LISTENERS_CONFIG, Type.LIST, null, Importance.LOW, LISTENERS_DOC)
                .define(REST_ADVERTISED_HOST_NAME_CONFIG, Type.STRING,  null, Importance.LOW, REST_ADVERTISED_HOST_NAME_DOC)
                .define(REST_ADVERTISED_PORT_CONFIG, Type.INT,  null, Importance.LOW, REST_ADVERTISED_PORT_DOC)
                .define(REST_ADVERTISED_LISTENER_CONFIG, Type.STRING,  null, Importance.LOW, REST_ADVERTISED_LISTENER_DOC)
                .define(ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, Type.STRING,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT, Importance.LOW,
                        ACCESS_CONTROL_ALLOW_ORIGIN_DOC)
                .define(ACCESS_CONTROL_ALLOW_METHODS_CONFIG, Type.STRING,
                        ACCESS_CONTROL_ALLOW_METHODS_DEFAULT, Importance.LOW,
                        ACCESS_CONTROL_ALLOW_METHODS_DOC)
                .define(PLUGIN_PATH_CONFIG,
                        Type.LIST,
                        null,
                        Importance.LOW,
                        PLUGIN_PATH_DOC)
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
                        "", Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
                        ConfigDef.Type.STRING, "none", ConfigDef.Importance.LOW, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
                .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS,
                        HEADER_CONVERTER_CLASS_DEFAULT,
                        Importance.LOW, HEADER_CONVERTER_CLASS_DOC)
                .define(CONFIG_PROVIDERS_CONFIG, Type.LIST,
                        Collections.emptyList(),
                        Importance.LOW, CONFIG_PROVIDERS_DOC)
                .define(REST_EXTENSION_CLASSES_CONFIG, Type.LIST, "",
                        Importance.LOW, REST_EXTENSION_CLASSES_DOC)
                .define(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, Type.STRING, CONNECTOR_CLIENT_POLICY_CLASS_DEFAULT,
                        Importance.MEDIUM, CONNECTOR_CLIENT_POLICY_CLASS_DOC);
    }

    private void logInternalConverterDeprecationWarnings(Map<String, String> props) {
        String[] deprecatedConfigs = new String[] {
            INTERNAL_KEY_CONVERTER_CLASS_CONFIG,
            INTERNAL_VALUE_CONVERTER_CLASS_CONFIG
        };
        for (String config : deprecatedConfigs) {
            if (props.containsKey(config)) {
                Class<?> internalConverterClass = getClass(config);
                logDeprecatedProperty(config, internalConverterClass.getCanonicalName(), INTERNAL_CONVERTER_DEFAULT.getCanonicalName(), null);
                if (internalConverterClass.equals(INTERNAL_CONVERTER_DEFAULT)) {
                    // log the properties for this converter ...
                    for (Map.Entry<String, Object> propEntry : originalsWithPrefix(config + ".").entrySet()) {
                        String prop = propEntry.getKey();
                        String propValue = propEntry.getValue().toString();
                        String defaultValue = JsonConverterConfig.SCHEMAS_ENABLE_CONFIG.equals(prop) ? "false" : null;
                        logDeprecatedProperty(config + "." + prop, propValue, defaultValue, config);
                    }
                }
            }
        }
    }

    private void logDeprecatedProperty(String propName, String propValue, String defaultValue, String prefix) {
        String prefixNotice = prefix != null
            ? " (along with all configuration for '" + prefix + "')"
            : "";
        if (defaultValue != null && defaultValue.equalsIgnoreCase(propValue)) {
            log.info(
                "Worker configuration property '{}'{} is deprecated and may be removed in an upcoming release. "
                    + "The specified value '{}' matches the default, so this property can be safely removed from the worker configuration.",
                propName,
                prefixNotice,
                propValue
            );
        } else if (defaultValue != null) {
            log.warn(
                "Worker configuration property '{}'{} is deprecated and may be removed in an upcoming release. "
                    + "The specified value '{}' does NOT match the default and recommended value '{}'.",
                propName,
                prefixNotice,
                propValue,
                defaultValue
            );
        } else {
            log.warn(
                "Worker configuration property '{}'{} is deprecated and may be removed in an upcoming release.",
                propName,
                prefixNotice
            );
        }
    }

    public Integer getRebalanceTimeout() {
        return null;
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public static List<String> pluginLocations(Map<String, String> props) {
        String locationList = props.get(WorkerConfig.PLUGIN_PATH_CONFIG);
        return locationList == null
                         ? new ArrayList<String>()
                         : Arrays.asList(COMMA_WITH_WHITESPACE.split(locationList.trim(), -1));
    }

    public WorkerConfig(ConfigDef definition, Map<String, String> props) {
        super(definition, props);
        logInternalConverterDeprecationWarnings(props);
    }

}
