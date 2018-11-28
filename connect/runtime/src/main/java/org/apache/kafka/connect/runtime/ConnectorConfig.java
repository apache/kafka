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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * <p>
 * Configuration options for Connectors. These only include Kafka Connect system-level configuration
 * options (e.g. Connector class name, timeouts used by Connect to control the connector) but does
 * not include Connector-specific options (e.g. database connection settings).
 * </p>
 * <p>
 * Note that some of these options are not required for all connectors. For example TOPICS_CONFIG
 * is sink-specific.
 * </p>
 */
public class ConnectorConfig extends AbstractConfig {
    protected static final String COMMON_GROUP = "Common";
    protected static final String TRANSFORMS_GROUP = "Transforms";
    protected static final String ERROR_GROUP = "Error Handling";

    public static final String NAME_CONFIG = "name";
    private static final String NAME_DOC = "Globally unique name to use for this connector.";
    private static final String NAME_DISPLAY = "Connector name";

    public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
    private static final String CONNECTOR_CLASS_DOC =
            "Name or alias of the class for this connector. Must be a subclass of org.apache.kafka.connect.connector.Connector. " +
                    "If the connector is org.apache.kafka.connect.file.FileStreamSinkConnector, you can either specify this full name, " +
                    " or use \"FileStreamSink\" or \"FileStreamSinkConnector\" to make the configuration a bit shorter";
    private static final String CONNECTOR_CLASS_DISPLAY = "Connector class";

    public static final String KEY_CONVERTER_CLASS_CONFIG = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
    public static final String KEY_CONVERTER_CLASS_DOC = WorkerConfig.KEY_CONVERTER_CLASS_DOC;
    public static final String KEY_CONVERTER_CLASS_DISPLAY = "Key converter class";

    public static final String VALUE_CONVERTER_CLASS_CONFIG = WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
    public static final String VALUE_CONVERTER_CLASS_DOC = WorkerConfig.VALUE_CONVERTER_CLASS_DOC;
    public static final String VALUE_CONVERTER_CLASS_DISPLAY = "Value converter class";

    public static final String HEADER_CONVERTER_CLASS_CONFIG = WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG;
    public static final String HEADER_CONVERTER_CLASS_DOC = WorkerConfig.HEADER_CONVERTER_CLASS_DOC;
    public static final String HEADER_CONVERTER_CLASS_DISPLAY = "Header converter class";
    // The Connector config should not have a default for the header converter, since the absence of a config property means that
    // the worker config settings should be used. Thus, we set the default to null here.
    public static final String HEADER_CONVERTER_CLASS_DEFAULT = null;

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
    public static final int TASKS_MAX_DEFAULT = 1;
    private static final int TASKS_MIN_CONFIG = 1;

    private static final String TASK_MAX_DISPLAY = "Tasks max";

    public static final String TRANSFORMS_CONFIG = "transforms";
    private static final String TRANSFORMS_DOC = "Aliases for the transformations to be applied to records.";
    private static final String TRANSFORMS_DISPLAY = "Transforms";

    public static final String CONFIG_RELOAD_ACTION_CONFIG = "config.action.reload";
    private static final String CONFIG_RELOAD_ACTION_DOC =
            "The action that Connect should take on the connector when changes in external " +
            "configuration providers result in a change in the connector's configuration properties. " +
            "A value of 'none' indicates that Connect will do nothing. " +
            "A value of 'restart' indicates that Connect should restart/reload the connector with the " +
            "updated configuration properties." +
            "The restart may actually be scheduled in the future if the external configuration provider " +
            "indicates that a configuration value will expire in the future.";

    private static final String CONFIG_RELOAD_ACTION_DISPLAY = "Reload Action";
    public static final String CONFIG_RELOAD_ACTION_NONE = Herder.ConfigReloadAction.NONE.name().toLowerCase(Locale.ROOT);
    public static final String CONFIG_RELOAD_ACTION_RESTART = Herder.ConfigReloadAction.RESTART.name().toLowerCase(Locale.ROOT);

    public static final String ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout";
    public static final String ERRORS_RETRY_TIMEOUT_DISPLAY = "Retry Timeout for Errors";
    public static final int ERRORS_RETRY_TIMEOUT_DEFAULT = 0;
    public static final String ERRORS_RETRY_TIMEOUT_DOC = "The maximum duration in milliseconds that a failed operation " +
            "will be reattempted. The default is 0, which means no retries will be attempted. Use -1 for infinite retries.";

    public static final String ERRORS_RETRY_MAX_DELAY_CONFIG = "errors.retry.delay.max.ms";
    public static final String ERRORS_RETRY_MAX_DELAY_DISPLAY = "Maximum Delay Between Retries for Errors";
    public static final int ERRORS_RETRY_MAX_DELAY_DEFAULT = 60000;
    public static final String ERRORS_RETRY_MAX_DELAY_DOC = "The maximum duration in milliseconds between consecutive retry attempts. " +
            "Jitter will be added to the delay once this limit is reached to prevent thundering herd issues.";

    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final String ERRORS_TOLERANCE_DISPLAY = "Error Tolerance";
    public static final ToleranceType ERRORS_TOLERANCE_DEFAULT = ToleranceType.NONE;
    public static final String ERRORS_TOLERANCE_DOC = "Behavior for tolerating errors during connector operation. 'none' is the default value " +
            "and signals that any error will result in an immediate connector task failure; 'all' changes the behavior to skip over problematic records.";

    public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
    public static final String ERRORS_LOG_ENABLE_DISPLAY = "Log Errors";
    public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;
    public static final String ERRORS_LOG_ENABLE_DOC = "If true, write each error and the details of the failed operation and problematic record " +
            "to the Connect application log. This is 'false' by default, so that only errors that are not tolerated are reported.";

    public static final String ERRORS_LOG_INCLUDE_MESSAGES_CONFIG = "errors.log.include.messages";
    public static final String ERRORS_LOG_INCLUDE_MESSAGES_DISPLAY = "Log Error Details";
    public static final boolean ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT = false;
    public static final String ERRORS_LOG_INCLUDE_MESSAGES_DOC = "Whether to the include in the log the Connect record that resulted in " +
            "a failure. This is 'false' by default, which will prevent record keys, values, and headers from being written to log files, " +
            "although some information such as topic and partition number will still be logged.";

    private final EnrichedConnectorConfig enrichedConfig;
    private static class EnrichedConnectorConfig extends AbstractConfig {
        EnrichedConnectorConfig(ConfigDef configDef, Map<String, String> props) {
            super(configDef, props);
        }

        @Override
        public Object get(String key) {
            return super.get(key);
        }
    }

    public static ConfigDef configDef() {
        int orderInGroup = 0;
        int orderInErrorGroup = 0;
        return new ConfigDef()
                .define(NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, nonEmptyStringWithoutControlChars(), Importance.HIGH, NAME_DOC, COMMON_GROUP, ++orderInGroup, Width.MEDIUM, NAME_DISPLAY)
                .define(CONNECTOR_CLASS_CONFIG, Type.STRING, Importance.HIGH, CONNECTOR_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.LONG, CONNECTOR_CLASS_DISPLAY)
                .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT, atLeast(TASKS_MIN_CONFIG), Importance.HIGH, TASKS_MAX_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, TASK_MAX_DISPLAY)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, KEY_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, KEY_CONVERTER_CLASS_DISPLAY)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, VALUE_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, VALUE_CONVERTER_CLASS_DISPLAY)
                .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS, HEADER_CONVERTER_CLASS_DEFAULT, Importance.LOW, HEADER_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, HEADER_CONVERTER_CLASS_DISPLAY)
                .define(TRANSFORMS_CONFIG, Type.LIST, Collections.emptyList(), ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.Validator() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public void ensureValid(String name, Object value) {
                        final List<String> transformAliases = (List<String>) value;
                        if (transformAliases.size() > new HashSet<>(transformAliases).size()) {
                            throw new ConfigException(name, value, "Duplicate alias provided.");
                        }
                    }

                    @Override
                    public String toString() {
                        return "unique transformation aliases";
                    }
                }), Importance.LOW, TRANSFORMS_DOC, TRANSFORMS_GROUP, ++orderInGroup, Width.LONG, TRANSFORMS_DISPLAY)
                .define(CONFIG_RELOAD_ACTION_CONFIG, Type.STRING, CONFIG_RELOAD_ACTION_RESTART,
                        in(CONFIG_RELOAD_ACTION_NONE, CONFIG_RELOAD_ACTION_RESTART), Importance.LOW,
                        CONFIG_RELOAD_ACTION_DOC, COMMON_GROUP, ++orderInGroup, Width.MEDIUM, CONFIG_RELOAD_ACTION_DISPLAY)
                .define(ERRORS_RETRY_TIMEOUT_CONFIG, Type.LONG, ERRORS_RETRY_TIMEOUT_DEFAULT, Importance.MEDIUM,
                        ERRORS_RETRY_TIMEOUT_DOC, ERROR_GROUP, ++orderInErrorGroup, Width.MEDIUM, ERRORS_RETRY_TIMEOUT_DISPLAY)
                .define(ERRORS_RETRY_MAX_DELAY_CONFIG, Type.LONG, ERRORS_RETRY_MAX_DELAY_DEFAULT, Importance.MEDIUM,
                        ERRORS_RETRY_MAX_DELAY_DOC, ERROR_GROUP, ++orderInErrorGroup, Width.MEDIUM, ERRORS_RETRY_MAX_DELAY_DISPLAY)
                .define(ERRORS_TOLERANCE_CONFIG, Type.STRING, ERRORS_TOLERANCE_DEFAULT.value(),
                        in(ToleranceType.NONE.value(), ToleranceType.ALL.value()), Importance.MEDIUM,
                        ERRORS_TOLERANCE_DOC, ERROR_GROUP, ++orderInErrorGroup, Width.SHORT, ERRORS_TOLERANCE_DISPLAY)
                .define(ERRORS_LOG_ENABLE_CONFIG, Type.BOOLEAN, ERRORS_LOG_ENABLE_DEFAULT, Importance.MEDIUM,
                        ERRORS_LOG_ENABLE_DOC, ERROR_GROUP, ++orderInErrorGroup, Width.SHORT, ERRORS_LOG_ENABLE_DISPLAY)
                .define(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, Type.BOOLEAN, ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT, Importance.MEDIUM,
                        ERRORS_LOG_INCLUDE_MESSAGES_DOC, ERROR_GROUP, ++orderInErrorGroup, Width.SHORT, ERRORS_LOG_INCLUDE_MESSAGES_DISPLAY);
    }

    public ConnectorConfig(Plugins plugins) {
        this(plugins, new HashMap<String, String>());
    }

    public ConnectorConfig(Plugins plugins, Map<String, String> props) {
        this(plugins, configDef(), props);
    }

    public ConnectorConfig(Plugins plugins, ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
        enrichedConfig = new EnrichedConnectorConfig(
                enrich(plugins, configDef, props, true),
                props
        );
    }

    @Override
    public Object get(String key) {
        return enrichedConfig.get(key);
    }

    public long errorRetryTimeout() {
        return getLong(ERRORS_RETRY_TIMEOUT_CONFIG);
    }

    public long errorMaxDelayInMillis() {
        return getLong(ERRORS_RETRY_MAX_DELAY_CONFIG);
    }

    public ToleranceType errorToleranceType() {
        String tolerance = getString(ERRORS_TOLERANCE_CONFIG);
        for (ToleranceType type: ToleranceType.values()) {
            if (type.name().equalsIgnoreCase(tolerance)) {
                return type;
            }
        }
        return ERRORS_TOLERANCE_DEFAULT;
    }

    public boolean enableErrorLog() {
        return getBoolean(ERRORS_LOG_ENABLE_CONFIG);
    }

    public boolean includeRecordDetailsInErrorLog() {
        return getBoolean(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG);
    }

    /**
     * Returns the initialized list of {@link Transformation} which are specified in {@link #TRANSFORMS_CONFIG}.
     */
    public <R extends ConnectRecord<R>> List<Transformation<R>> transformations() {
        final List<String> transformAliases = getList(TRANSFORMS_CONFIG);

        final List<Transformation<R>> transformations = new ArrayList<>(transformAliases.size());
        for (String alias : transformAliases) {
            final String prefix = TRANSFORMS_CONFIG + "." + alias + ".";
            try {
                @SuppressWarnings("unchecked")
                final Transformation<R> transformation = getClass(prefix + "type").asSubclass(Transformation.class)
                        .getDeclaredConstructor().newInstance();
                transformation.configure(originalsWithPrefix(prefix));
                transformations.add(transformation);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        return transformations;
    }

    /**
     * Returns an enriched {@link ConfigDef} building upon the {@code ConfigDef}, using the current configuration specified in {@code props} as an input.
     * <p>
     * {@code requireFullConfig} specifies whether required config values that are missing should cause an exception to be thrown.
     */
    public static ConfigDef enrich(Plugins plugins, ConfigDef baseConfigDef, Map<String, String> props, boolean requireFullConfig) {
        Object transformAliases = ConfigDef.parseType(TRANSFORMS_CONFIG, props.get(TRANSFORMS_CONFIG), Type.LIST);
        if (!(transformAliases instanceof List)) {
            return baseConfigDef;
        }

        ConfigDef newDef = new ConfigDef(baseConfigDef);
        LinkedHashSet<?> uniqueTransformAliases = new LinkedHashSet<>((List<?>) transformAliases);
        for (Object o : uniqueTransformAliases) {
            if (!(o instanceof String)) {
                throw new ConfigException("Item in " + TRANSFORMS_CONFIG + " property is not of "
                        + "type String");
            }
            String alias = (String) o;
            final String prefix = TRANSFORMS_CONFIG + "." + alias + ".";
            final String group = TRANSFORMS_GROUP + ": " + alias;
            int orderInGroup = 0;

            final String transformationTypeConfig = prefix + "type";
            final ConfigDef.Validator typeValidator = new ConfigDef.Validator() {
                @Override
                public void ensureValid(String name, Object value) {
                    getConfigDefFromTransformation(transformationTypeConfig, (Class) value);
                }
            };
            newDef.define(transformationTypeConfig, Type.CLASS, ConfigDef.NO_DEFAULT_VALUE, typeValidator, Importance.HIGH,
                    "Class for the '" + alias + "' transformation.", group, orderInGroup++, Width.LONG, "Transformation type for " + alias,
                    Collections.<String>emptyList(), new TransformationClassRecommender(plugins));

            final ConfigDef transformationConfigDef;
            try {
                final String className = props.get(transformationTypeConfig);
                final Class<?> cls = (Class<?>) ConfigDef.parseType(transformationTypeConfig, className, Type.CLASS);
                transformationConfigDef = getConfigDefFromTransformation(transformationTypeConfig, cls);
            } catch (ConfigException e) {
                if (requireFullConfig) {
                    throw e;
                } else {
                    continue;
                }
            }

            newDef.embed(prefix, group, orderInGroup, transformationConfigDef);
        }

        return newDef;
    }

    /**
     * Return {@link ConfigDef} from {@code transformationCls}, which is expected to be a non-null {@code Class<Transformation>},
     * by instantiating it and invoking {@link Transformation#config()}.
     */
    static ConfigDef getConfigDefFromTransformation(String key, Class<?> transformationCls) {
        if (transformationCls == null || !Transformation.class.isAssignableFrom(transformationCls)) {
            throw new ConfigException(key, String.valueOf(transformationCls), "Not a Transformation");
        }
        Transformation transformation;
        try {
            transformation = transformationCls.asSubclass(Transformation.class).newInstance();
        } catch (Exception e) {
            throw new ConfigException(key, String.valueOf(transformationCls), "Error getting config definition from Transformation: " + e.getMessage());
        }
        ConfigDef configDef = transformation.config();
        if (null == configDef) {
            throw new ConnectException(
                String.format(
                    "%s.config() must return a ConfigDef that is not null.",
                    transformationCls.getName()
                )
            );
        }
        return configDef;
    }

    /**
     * Recommend bundled transformations.
     */
    static final class TransformationClassRecommender implements ConfigDef.Recommender {
        private final Plugins plugins;

        TransformationClassRecommender(Plugins plugins) {
            this.plugins = plugins;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            List<Object> transformationPlugins = new ArrayList<>();
            for (PluginDesc<Transformation> plugin : plugins.transformations()) {
                transformationPlugins.add(plugin.pluginClass());
            }
            return Collections.unmodifiableList(transformationPlugins);
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

}
