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
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

    private static ConfigDef.CompositeValidator aliasValidator(String kind) {
        return ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.Validator() {
            @SuppressWarnings("unchecked")
            @Override
            public void ensureValid(String name, Object value) {
                final List<String> aliases = (List<String>) value;
                if (aliases.size() > new HashSet<>(aliases).size()) {
                    throw new ConfigException(name, value, "Duplicate alias provided.");
                }
            }

            @Override
            public String toString() {
                return "unique " + kind + " aliases";
            }
        });
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
                    transformations.add(new TransformationStage<>(predicate, negate == null ? false : Boolean.parseBoolean(negate.toString()), transformation));
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
            @SuppressWarnings("rawtypes")
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

    /**
     * An abstraction over "enrichable plugins" ({@link Transformation}s and {@link Predicate}s) used for computing the
     * contribution to a Connectors ConfigDef.
     *
     * This is not entirely elegant because
     * although they basically use the same "alias prefix" configuration idiom there are some differences.
     * The abstract method pattern is used to cope with this.
     * @param <T> The type of plugin (either {@code Transformation} or {@code Predicate}).
     */
    static abstract class EnrichablePlugin<T> {

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
        protected void validateProps(String prefix) { }

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
            if (cls == null || !baseClass.isAssignableFrom(cls)) {
                throw new ConfigException(key, String.valueOf(cls), "Not a " + baseClass.getSimpleName());
            }
            if (Modifier.isAbstract(cls.getModifiers())) {
                String childClassNames = Stream.of(cls.getClasses())
                        .filter(cls::isAssignableFrom)
                        .filter(c -> !Modifier.isAbstract(c.getModifiers()))
                        .filter(c -> Modifier.isPublic(c.getModifiers()))
                        .map(Class::getName)
                        .collect(Collectors.joining(", "));
                String message = Utils.isBlank(childClassNames) ?
                        aliasKind + " is abstract and cannot be created." :
                        aliasKind + " is abstract and cannot be created. Did you mean " + childClassNames + "?";
                throw new ConfigException(key, String.valueOf(cls), message);
            }
            T transformation;
            try {
                transformation = Utils.newInstance(cls, baseClass);
            } catch (Exception e) {
                throw new ConfigException(key, String.valueOf(cls), "Error getting config definition from " + baseClass.getSimpleName() + ": " + e.getMessage());
            }
            ConfigDef configDef = config(transformation);
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

}
