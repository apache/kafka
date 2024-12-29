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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.PluginsRecommenders;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginLoadingException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.ConcreteSubClassValidator;
import org.apache.kafka.connect.util.InstantiableClassValidator;

import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
    private static final Logger log = LoggerFactory.getLogger(ConnectorConfig.class);

    protected static final String COMMON_GROUP = "Common";
    protected static final String TRANSFORMS_GROUP = "Transforms";
    protected static final String PREDICATES_GROUP = "Predicates";
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

    public static final String CONNECTOR_VERSION = "connector." + WorkerConfig.PLUGIN_VERSION_SUFFIX;
    private static final String CONNECTOR_VERSION_DOC = "Version of the connector.";
    private static final String CONNECTOR_VERSION_DISPLAY = "Connector version";
    private static final ConfigDef.Validator CONNECTOR_VERSION_VALIDATOR = new PluginVersionValidator();

    public static final String KEY_CONVERTER_CLASS_CONFIG = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
    public static final String KEY_CONVERTER_CLASS_DOC = WorkerConfig.KEY_CONVERTER_CLASS_DOC;
    public static final String KEY_CONVERTER_CLASS_DISPLAY = "Key converter class";
    private static final ConfigDef.Validator KEY_CONVERTER_CLASS_VALIDATOR = ConfigDef.CompositeValidator.of(
            ConcreteSubClassValidator.forSuperClass(Converter.class),
            new InstantiableClassValidator()
    );

    public static final String KEY_CONVERTER_VERSION_CONFIG = WorkerConfig.KEY_CONVERTER_VERSION;
    private static final String KEY_CONVERTER_VERSION_DOC = "Version of the key converter.";
    private static final String KEY_CONVERTER_VERSION_DISPLAY = "Key converter version";
    private static final ConfigDef.Validator KEY_CONVERTER_VERSION_VALIDATOR = new PluginVersionValidator();


    public static final String VALUE_CONVERTER_CLASS_CONFIG = WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
    public static final String VALUE_CONVERTER_CLASS_DOC = WorkerConfig.VALUE_CONVERTER_CLASS_DOC;
    public static final String VALUE_CONVERTER_CLASS_DISPLAY = "Value converter class";
    private static final ConfigDef.Validator VALUE_CONVERTER_CLASS_VALIDATOR = ConfigDef.CompositeValidator.of(
            ConcreteSubClassValidator.forSuperClass(Converter.class),
            new InstantiableClassValidator()
    );

    public static final String VALUE_CONVERTER_VERSION_CONFIG = WorkerConfig.VALUE_CONVERTER_VERSION;
    private static final String VALUE_CONVERTER_VERSION_DOC = "Version of the value converter.";
    private static final String VALUE_CONVERTER_VERSION_DISPLAY = "Value converter version";
    private static final ConfigDef.Validator VALUE_CONVERTER_VERSION_VALIDATOR = new PluginVersionValidator();

    public static final String HEADER_CONVERTER_CLASS_CONFIG = WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG;
    public static final String HEADER_CONVERTER_CLASS_DOC = WorkerConfig.HEADER_CONVERTER_CLASS_DOC;
    public static final String HEADER_CONVERTER_CLASS_DISPLAY = "Header converter class";
    private static final ConfigDef.Validator HEADER_CONVERTER_CLASS_VALIDATOR = ConfigDef.CompositeValidator.of(
            ConcreteSubClassValidator.forSuperClass(HeaderConverter.class),
            new InstantiableClassValidator()
    );

    public static final String HEADER_CONVERTER_VERSION_CONFIG = WorkerConfig.HEADER_CONVERTER_VERSION;
    private static final String HEADER_CONVERTER_VERSION_DOC = "Version of the header converter.";
    private static final String HEADER_CONVERTER_VERSION_DISPLAY = "Header converter version";
    private static final ConfigDef.Validator HEADER_CONVERTER_VERSION_VALIDATOR = new PluginVersionValidator();

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
    public static final int TASKS_MAX_DEFAULT = 1;
    private static final int TASKS_MIN_CONFIG = 1;

    private static final String TASK_MAX_DISPLAY = "Tasks max";

    public static final String TASKS_MAX_ENFORCE_CONFIG = "tasks.max.enforce";
    private static final String TASKS_MAX_ENFORCE_DOC =
            "(Deprecated) Whether to enforce that the tasks.max property is respected by the connector. "
                    + "By default, connectors that generate too many tasks will fail, and existing sets of tasks that exceed the tasks.max property will also be failed. "
                    + "If this property is set to false, then connectors will be allowed to generate more than the maximum number of tasks, and existing sets of "
                    + "tasks that exceed the tasks.max property will be allowed to run. "
                    + "This property is deprecated and will be removed in an upcoming major release.";
    public static final boolean TASKS_MAX_ENFORCE_DEFAULT = true;
    private static final String TASKS_MAX_ENFORCE_DISPLAY = "Enforce tasks max";

    public static final String TRANSFORMS_CONFIG = "transforms";
    private static final String TRANSFORMS_DOC = "Aliases for the transformations to be applied to records.";
    private static final String TRANSFORMS_DISPLAY = "Transforms";

    public static final String PREDICATES_CONFIG = "predicates";
    private static final String PREDICATES_DOC = "Aliases for the predicates used by transformations.";
    private static final String PREDICATES_DISPLAY = "Predicates";

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
    public static final String ERRORS_LOG_INCLUDE_MESSAGES_DOC = "Whether to include in the log the Connect record that resulted in a failure. " +
            "For sink records, the topic, partition, offset, and timestamp will be logged. " +
            "For source records, the key and value (and their schemas), all headers, and the timestamp, Kafka topic, Kafka partition, source partition, " +
            "and source offset will be logged. " +
            "This is 'false' by default, which will prevent record keys, values, and headers from being written to log files.";


    public static final String CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX = "producer.override.";
    public static final String CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX = "consumer.override.";
    public static final String CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX = "admin.override.";
    public static final String PREDICATES_PREFIX = "predicates.";

    private static final PluginsRecommenders EMPTY_RECOMMENDER = new PluginsRecommenders();
    private static final ConverterDefaults CONVERTER_DEFAULTS = new ConverterDefaults(null, null);

    private final ConnectorConfig.EnrichedConnectorConfig enrichedConfig;

    private static class EnrichedConnectorConfig extends AbstractConfig {
        EnrichedConnectorConfig(ConfigDef configDef, Map<String, String> props) {
            super(configDef, props);
        }

        @Override
        public Object get(String key) {
            return super.get(key);
        }
    }

    protected static ConfigDef configDef(
            String defaultConnectorVersion,
            ConverterDefaults keyConverterDefaults,
            ConverterDefaults valueConverterDefaults,
            ConverterDefaults headerConverterDefaults,
            PluginsRecommenders recommender
    ) {
        int orderInGroup = 0;
        int orderInErrorGroup = 0;
        return new ConfigDef()
                .define(NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, nonEmptyStringWithoutControlChars(), Importance.HIGH, NAME_DOC, COMMON_GROUP, ++orderInGroup, Width.MEDIUM, NAME_DISPLAY)
                .define(CONNECTOR_CLASS_CONFIG, Type.STRING, Importance.HIGH, CONNECTOR_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.LONG, CONNECTOR_CLASS_DISPLAY)
                .define(CONNECTOR_VERSION, Type.STRING, defaultConnectorVersion, CONNECTOR_VERSION_VALIDATOR, Importance.MEDIUM, CONNECTOR_VERSION_DOC, COMMON_GROUP, ++orderInGroup, Width.MEDIUM, CONNECTOR_VERSION_DISPLAY, recommender.connectorPluginVersionRecommender())
                .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT, atLeast(TASKS_MIN_CONFIG), Importance.HIGH, TASKS_MAX_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, TASK_MAX_DISPLAY)
                .define(TASKS_MAX_ENFORCE_CONFIG, Type.BOOLEAN, TASKS_MAX_ENFORCE_DEFAULT, Importance.LOW, TASKS_MAX_ENFORCE_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, TASKS_MAX_ENFORCE_DISPLAY)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS, keyConverterDefaults.type, KEY_CONVERTER_CLASS_VALIDATOR, Importance.LOW, KEY_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, KEY_CONVERTER_CLASS_DISPLAY, recommender.converterPluginRecommender())
                .define(KEY_CONVERTER_VERSION_CONFIG, Type.STRING, keyConverterDefaults.version, KEY_CONVERTER_VERSION_VALIDATOR, Importance.LOW, KEY_CONVERTER_VERSION_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, KEY_CONVERTER_VERSION_DISPLAY, recommender.keyConverterPluginVersionRecommender())
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS, valueConverterDefaults.type, VALUE_CONVERTER_CLASS_VALIDATOR, Importance.LOW, VALUE_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, VALUE_CONVERTER_CLASS_DISPLAY, recommender.converterPluginRecommender())
                .define(VALUE_CONVERTER_VERSION_CONFIG, Type.STRING, valueConverterDefaults.version, VALUE_CONVERTER_VERSION_VALIDATOR, Importance.LOW, VALUE_CONVERTER_VERSION_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, VALUE_CONVERTER_VERSION_DISPLAY, recommender.valueConverterPluginVersionRecommender())
                .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS, headerConverterDefaults.type, HEADER_CONVERTER_CLASS_VALIDATOR, Importance.LOW, HEADER_CONVERTER_CLASS_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, HEADER_CONVERTER_CLASS_DISPLAY, recommender.headerConverterPluginRecommender())
                .define(HEADER_CONVERTER_VERSION_CONFIG, Type.STRING, headerConverterDefaults.version, HEADER_CONVERTER_VERSION_VALIDATOR, Importance.LOW, HEADER_CONVERTER_VERSION_DOC, COMMON_GROUP, ++orderInGroup, Width.SHORT, HEADER_CONVERTER_VERSION_DISPLAY, recommender.headerConverterPluginVersionRecommender())
                .define(TRANSFORMS_CONFIG, Type.LIST, Collections.emptyList(), aliasValidator("transformation"), Importance.LOW, TRANSFORMS_DOC, TRANSFORMS_GROUP, ++orderInGroup, Width.LONG, TRANSFORMS_DISPLAY)
                .define(PREDICATES_CONFIG, Type.LIST, Collections.emptyList(), aliasValidator("predicate"), Importance.LOW, PREDICATES_DOC, PREDICATES_GROUP, ++orderInGroup, Width.LONG, PREDICATES_DISPLAY)
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

    public static ConfigDef configDef() {
        return configDef(null, CONVERTER_DEFAULTS, CONVERTER_DEFAULTS, CONVERTER_DEFAULTS, EMPTY_RECOMMENDER);
    }

    // ConfigDef with additional defaults and recommenders
    public static ConfigDef enrichedConfigDef(Plugins plugins, Map<String, String> connProps, WorkerConfig workerConfig) {
        PluginsRecommenders recommender = new PluginsRecommenders(plugins);
        ConverterDefaults keyConverterDefaults = converterDefaults(plugins, KEY_CONVERTER_CLASS_CONFIG,
                WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, WorkerConfig.KEY_CONVERTER_VERSION, connProps, workerConfig, Converter.class);
        ConverterDefaults valueConverterDefaults = converterDefaults(plugins, VALUE_CONVERTER_CLASS_CONFIG,
                WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, WorkerConfig.VALUE_CONVERTER_VERSION, connProps, workerConfig, Converter.class);
        ConverterDefaults headerConverterDefaults = converterDefaults(plugins, HEADER_CONVERTER_CLASS_CONFIG,
                WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, WorkerConfig.HEADER_CONVERTER_VERSION, connProps, workerConfig, HeaderConverter.class);
        return configDef(plugins.latestVersion(connProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)),
                keyConverterDefaults, valueConverterDefaults, headerConverterDefaults, recommender);
    }

    public static ConfigDef enrichedConfigDef(Plugins plugins, String connectorClass) {
        return configDef(plugins.latestVersion(connectorClass), CONVERTER_DEFAULTS, CONVERTER_DEFAULTS, CONVERTER_DEFAULTS, EMPTY_RECOMMENDER);
    }

    private static ConfigDef.CompositeValidator aliasValidator(String kind) {
        return ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), ConfigDef.LambdaValidator.with(
            (name, value) -> {
                @SuppressWarnings("unchecked")
                final List<String> aliases = (List<String>) value;
                if (aliases.size() > new HashSet<>(aliases).size()) {
                    throw new ConfigException(name, value, "Duplicate alias provided.");
                }
            },
            () -> "unique " + kind + " aliases"));
    }

    public ConnectorConfig(Plugins plugins) {
        this(plugins, Collections.emptyMap());
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
        for (ToleranceType type : ToleranceType.values()) {
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

    public int tasksMax() {
        return getInt(TASKS_MAX_CONFIG);
    }

    public boolean enforceTasksMax() {
        return getBoolean(TASKS_MAX_ENFORCE_CONFIG);
    }

    /**
     * Returns the initialized list of {@link TransformationStage} which apply the
     * {@link Transformation transformations} and {@link Predicate predicates}
     * as they are specified in the {@link #TRANSFORMS_CONFIG} and {@link #PREDICATES_CONFIG}
     */
    public <R extends ConnectRecord<R>> List<TransformationStage<R>> transformationStages() {
        final List<String> transformAliases = getList(TRANSFORMS_CONFIG);

        final List<TransformationStage<R>> transformations = new ArrayList<>(transformAliases.size());
        for (String alias : transformAliases) {
            final String prefix = TRANSFORMS_CONFIG + "." + alias + ".";

            try {
                @SuppressWarnings("unchecked")
                final Transformation<R> transformation = Utils.newInstance(getClass(prefix + "type"), Transformation.class);
                Map<String, Object> configs = originalsWithPrefix(prefix);
                Object predicateAlias = configs.remove(TransformationStage.PREDICATE_CONFIG);
                Object negate = configs.remove(TransformationStage.NEGATE_CONFIG);
                transformation.configure(configs);
                if (predicateAlias != null) {
                    String predicatePrefix = PREDICATES_PREFIX + predicateAlias + ".";
                    @SuppressWarnings("unchecked")
                    Predicate<R> predicate = Utils.newInstance(getClass(predicatePrefix + "type"), Predicate.class);
                    predicate.configure(originalsWithPrefix(predicatePrefix));
                    transformations.add(new TransformationStage<>(predicate, negate != null && Boolean.parseBoolean(negate.toString()), transformation));
                } else {
                    transformations.add(new TransformationStage<>(transformation));
                }
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
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ConfigDef enrich(Plugins plugins, ConfigDef baseConfigDef, Map<String, String> props, boolean requireFullConfig) {
        ConfigDef newDef = new ConfigDef(baseConfigDef);
        new EnrichablePlugin<Transformation<?>>("Transformation", TRANSFORMS_CONFIG, TRANSFORMS_GROUP, (Class) Transformation.class,
                props, requireFullConfig) {

            @Override
            protected Set<PluginDesc<Transformation<?>>> plugins() {
                return plugins.transformations();
            }

            @Override
            protected ConfigDef initialConfigDef() {
                // All Transformations get these config parameters implicitly
                return super.initialConfigDef()
                        .define(TransformationStage.PREDICATE_CONFIG, Type.STRING, null, Importance.MEDIUM,
                                "The alias of a predicate used to determine whether to apply this transformation.")
                        .define(TransformationStage.NEGATE_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM,
                                "Whether the configured predicate should be negated.");
            }

            @Override
            protected Stream<Map.Entry<String, ConfigDef.ConfigKey>> configDefsForClass(String typeConfig) {
                return super.configDefsForClass(typeConfig)
                        .filter(entry -> {
                            // The implicit parameters mask any from the transformer with the same name
                            if (TransformationStage.PREDICATE_CONFIG.equals(entry.getKey())
                                    || TransformationStage.NEGATE_CONFIG.equals(entry.getKey())) {
                                log.warn("Transformer config {} is masked by implicit config of that name",
                                        entry.getKey());
                                return false;
                            } else {
                                return true;
                            }
                        });
            }

            @Override
            protected ConfigDef config(Transformation<?> transformation) {
                return transformation.config();
            }

            @Override
            protected void validateProps(String prefix) {
                String prefixedNegate = prefix + TransformationStage.NEGATE_CONFIG;
                String prefixedPredicate = prefix + TransformationStage.PREDICATE_CONFIG;
                if (props.containsKey(prefixedNegate) &&
                        !props.containsKey(prefixedPredicate)) {
                    throw new ConfigException("Config '" + prefixedNegate + "' was provided " +
                            "but there is no config '" + prefixedPredicate + "' defining a predicate to be negated.");
                }
            }
        }.enrich(newDef);

        new EnrichablePlugin<Predicate<?>>("Predicate", PREDICATES_CONFIG, PREDICATES_GROUP,
                (Class) Predicate.class, props, requireFullConfig) {
            @Override
            protected Set<PluginDesc<Predicate<?>>> plugins() {
                return plugins.predicates();
            }

            @Override
            protected ConfigDef config(Predicate<?> predicate) {
                return predicate.config();
            }
        }.enrich(newDef);
        return newDef;
    }

    private static <T> ConverterDefaults converterDefaults(
            Plugins plugins,
            String connectorConverterConfig,
            String workerConverterConfig,
            String workerConverterVersionConfig,
            Map<String, String> connectorProps,
            WorkerConfig workerConfig,
            Class<T> converterType
    ) {
        /*
        if a converter is specified in the connector config it overrides the worker config for the corresponding converter
        otherwise the worker config is used, hence if the converter is not provided in the connector config, the default
        is the one provided in the worker config

        for converters which version is used depends on a several factors with multi-versioning support
        A. If the converter class is provided as part of the connector properties
            1. if the version is not provided,
                - if the converter is packaged with the connector then, the packaged version is used
                - if the converter is not packaged with the connector, the latest version is used
            2. if the version is provided, the provided version is used
        B. If the converter class is not provided as part of the connector properties, but provided as part of the worker properties
            1. if the version is not provided, the latest version is used
            2. if the version is provided, the provided version is used
        C. If the converter class is not provided as part of the connector properties and not provided as part of the worker properties,
        the converter to use is unknown hence no default version can be determined (null)

        Note: Connect when using service loading has an issue outlined in KAFKA-18119. The issue means that the above
        logic does not hold currently for clusters using service loading when converters are defined in the connector.
        However, the logic to determine the default should ideally follow the one outlined above, and the code here
        should still show the correct default version regardless of the bug.
        */
        final String connectorConverter = connectorProps.get(connectorConverterConfig);
        // since header converter defines a default in the worker config we need to handle it separately
        final String workerConverter = workerConverterConfig.equals(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG) ?
                workerConfig.getClass(workerConverterConfig).getName() : workerConfig.originalsStrings().get(workerConverterConfig);
        final String connectorClass = connectorProps.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        final String connectorVersion = connectorProps.get(ConnectorConfig.CONNECTOR_VERSION);
        String type = null;
        if (connectorClass == null || (connectorConverter == null && workerConverter == null)) {
            return new ConverterDefaults(null, null);
        }
        // update the default of connector converter based on if the worker converter is provided
        type = workerConverter;

        String version = null;
        if (connectorConverter != null) {
            version = fetchPluginVersion(plugins, connectorConverter, connectorVersion, connectorConverter);
        } else {
            version = workerConfig.originalsStrings().get(workerConverterVersionConfig);
            if (version == null) {
                version = plugins.latestVersion(workerConverter);
            }
        }
        return new ConverterDefaults(type, version);
    }

    private static void updateKeyDefault(ConfigDef configDef, String versionConfigKey, String versionDefault) {
        ConfigDef.ConfigKey key = configDef.configKeys().get(versionConfigKey);
        if (key == null) {
            return;
        }
        configDef.configKeys().put(versionConfigKey, new ConfigDef.ConfigKey(
                versionConfigKey, key.type, versionDefault, key.validator, key.importance, key.documentation, key.group, key.orderInGroup, key.width, key.displayName, key.dependents, key.recommender, false
        ));
    }

    @SuppressWarnings("unchecked")
    private static <T> String fetchPluginVersion(Plugins plugins, String connectorClass, String connectorVersion, String pluginName) {
        if (pluginName == null) {
            return null;
        }
        try {
            VersionRange range = PluginUtils.connectorVersionRequirement(connectorVersion);
            return plugins.pluginVersion(pluginName, plugins.pluginLoader(connectorClass, range));
        } catch (InvalidVersionSpecificationException | VersionedPluginLoadingException e) {
            // these errors should be captured in other places, so we can ignore them here
            log.warn("Failed to determine default plugin version for {}", connectorClass, e);
        }
        return null;
    }

    /**
     * An abstraction over "enrichable plugins" ({@link Transformation}s and {@link Predicate}s) used for computing the
     * contribution to a Connectors ConfigDef.
     *
     * This is not entirely elegant because
     * although they basically use the same "alias prefix" configuration idiom there are some differences.
     * The abstract method pattern is used to cope with this.
     * @param <T> The type of plugin (either {@code Transformation} or {@code Predicate}).
     */
    abstract static class EnrichablePlugin<T> {

        private final String aliasKind;
        private final String aliasConfig;
        private final String aliasGroup;
        private final Class<T> baseClass;
        private final Map<String, String> props;
        private final boolean requireFullConfig;

        public EnrichablePlugin(
                String aliasKind,
                String aliasConfig, String aliasGroup, Class<T> baseClass,
                Map<String, String> props, boolean requireFullConfig) {
            this.aliasKind = aliasKind;
            this.aliasConfig = aliasConfig;
            this.aliasGroup = aliasGroup;
            this.baseClass = baseClass;
            this.props = props;
            this.requireFullConfig = requireFullConfig;
        }

        /** Add the configs for this alias to the given {@code ConfigDef}. */
        void enrich(ConfigDef newDef) {
            Object aliases = ConfigDef.parseType(aliasConfig, props.get(aliasConfig), Type.LIST);
            if (!(aliases instanceof List)) {
                return;
            }

            LinkedHashSet<?> uniqueAliases = new LinkedHashSet<>((List<?>) aliases);
            for (Object o : uniqueAliases) {
                if (!(o instanceof String)) {
                    throw new ConfigException("Item in " + aliasConfig + " property is not of "
                            + "type String");
                }
                String alias = (String) o;
                final String prefix = aliasConfig + "." + alias + ".";
                final String group = aliasGroup + ": " + alias;
                int orderInGroup = 0;

                final String typeConfig = prefix + "type";
                final ConfigDef.Validator typeValidator = ConfigDef.LambdaValidator.with(
                        (String name, Object value) -> {
                            validateProps(prefix);
                            // The value will be null if the class couldn't be found; no point in performing follow-up validation
                            if (value != null) {
                                getConfigDefFromConfigProvidingClass(typeConfig, (Class<?>) value);
                            }
                        },
                        () -> "valid configs for " + alias + " " + aliasKind.toLowerCase(Locale.ENGLISH));
                newDef.define(typeConfig, Type.CLASS, ConfigDef.NO_DEFAULT_VALUE, typeValidator, Importance.HIGH,
                        "Class for the '" + alias + "' " + aliasKind.toLowerCase(Locale.ENGLISH) + ".", group, orderInGroup++, Width.LONG,
                        baseClass.getSimpleName() + " type for " + alias,
                        Collections.emptyList(), new ClassRecommender());

                final ConfigDef configDef = populateConfigDef(typeConfig);
                if (configDef == null) continue;
                newDef.embed(prefix, group, orderInGroup, configDef);
            }
        }

        /** Subclasses can add extra validation of the {@link #props}. */
        protected void validateProps(String prefix) {
        }

        /**
         * Populates the ConfigDef according to the configs returned from {@code configs()} method of class
         * named in the {@code ...type} parameter of the {@code props}.
         */
        protected ConfigDef populateConfigDef(String typeConfig) {
            final ConfigDef configDef = initialConfigDef();
            try {
                configDefsForClass(typeConfig)
                        .forEach(entry -> configDef.define(entry.getValue()));
            } catch (ConfigException e) {
                if (requireFullConfig) {
                    throw e;
                } else {
                    return null;
                }
            }
            return configDef;
        }

        /**
         * Return a stream of configs provided by the {@code configs()} method of class
         * named in the {@code ...type} parameter of the {@code props}.
         */
        protected Stream<Map.Entry<String, ConfigDef.ConfigKey>> configDefsForClass(String typeConfig) {
            final Class<?> cls = (Class<?>) ConfigDef.parseType(typeConfig, props.get(typeConfig), Type.CLASS);
            return getConfigDefFromConfigProvidingClass(typeConfig, cls)
                    .configKeys().entrySet().stream();
        }

        /** Get an initial ConfigDef */
        protected ConfigDef initialConfigDef() {
            return new ConfigDef();
        }

        /**
         * Return {@link ConfigDef} from {@code cls}, which is expected to be a non-null {@code Class<T>},
         * by instantiating it and invoking {@link #config(T)}.
         * @param key
         * @param cls The subclass of the baseclass.
         */
        ConfigDef getConfigDefFromConfigProvidingClass(String key, Class<?> cls) {
            if (cls == null) {
                throw new ConfigException(key, null, "Not a " + baseClass.getSimpleName());
            }
            Utils.ensureConcreteSubclass(baseClass, cls);

            T pluginInstance;
            try {
                pluginInstance = Utils.newInstance(cls, baseClass);
            } catch (Exception e) {
                throw new ConfigException(key, String.valueOf(cls), "Error getting config definition from " + baseClass.getSimpleName() + ": " + e.getMessage());
            }
            ConfigDef configDef = config(pluginInstance);
            if (null == configDef) {
                throw new ConnectException(
                        String.format(
                                "%s.config() must return a ConfigDef that is not null.",
                                cls.getName()
                        )
                );
            }
            return configDef;
        }

        /**
         * Get the ConfigDef from the given entity.
         * This is necessary because there's no abstraction across {@link Transformation#config()} and
         * {@link Predicate#config()}.
         */
        protected abstract ConfigDef config(T t);

        /**
         * The transformation or predicate plugins (as appropriate for T) to be used
         * for the {@link ClassRecommender}.
         */
        protected abstract Set<PluginDesc<T>> plugins();

        /**
         * Recommend bundled transformations or predicates.
         */
        final class ClassRecommender implements ConfigDef.Recommender {

            @Override
            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                List<Object> result = new ArrayList<>();
                for (PluginDesc<T> plugin : plugins()) {
                    result.add(plugin.pluginClass());
                }
                return Collections.unmodifiableList(result);
            }

            @Override
            public boolean visible(String name, Map<String, Object> parsedConfig) {
                return true;
            }
        }
    }

    private static class ConverterDefaults {
        private final String type;
        private final String version;

        public ConverterDefaults(String type, String version) {
            this.type = type;
            this.version = version;
        }

        public String type() {
            return type;
        }

        public String version() {
            return version;
        }
    }

    public static class PluginVersionValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {

            try {
                PluginUtils.connectorVersionRequirement((String) value);
            } catch (InvalidVersionSpecificationException e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }
    }
}
