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
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

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

    public static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String TASKS_MAX_DOC = "Maximum number of tasks to use for this connector.";
    public static final int TASKS_MAX_DEFAULT = 1;
    private static final int TASKS_MIN_CONFIG = 1;

    private static final String TASK_MAX_DISPLAY = "Tasks max";

    public static final String TRANSFORMS_CONFIG = "transforms";
    private static final String TRANSFORMS_DOC = "Aliases for the transformations to be applied to records.";
    private static final String TRANSFORMS_DISPLAY = "Transforms";

    private final EnrichedConnectorConfig enrichedConfig;
    private static class EnrichedConnectorConfig extends AbstractConfig {
        EnrichedConnectorConfig(ConfigDef configDef, Map<String, String> props) {
            super(configDef, props);
        }

        public Object get(String key) {
            return super.get(key);
        }
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(NAME_CONFIG, Type.STRING, Importance.HIGH, NAME_DOC, COMMON_GROUP, 1, Width.MEDIUM, NAME_DISPLAY)
                .define(CONNECTOR_CLASS_CONFIG, Type.STRING, Importance.HIGH, CONNECTOR_CLASS_DOC, COMMON_GROUP, 2, Width.LONG, CONNECTOR_CLASS_DISPLAY)
                .define(TASKS_MAX_CONFIG, Type.INT, TASKS_MAX_DEFAULT, atLeast(TASKS_MIN_CONFIG), Importance.HIGH, TASKS_MAX_DOC, COMMON_GROUP, 3, Width.SHORT, TASK_MAX_DISPLAY)
                .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, KEY_CONVERTER_CLASS_DOC, COMMON_GROUP, 4, Width.SHORT, KEY_CONVERTER_CLASS_DISPLAY)
                .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, VALUE_CONVERTER_CLASS_DOC, COMMON_GROUP, 5, Width.SHORT, VALUE_CONVERTER_CLASS_DISPLAY)
                .define(TRANSFORMS_CONFIG, Type.LIST, null, new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object value) {
                        if (value == null) return;
                        final List<String> transformAliases = (List<String>) value;
                        if (transformAliases.size() > new HashSet<>(transformAliases).size()) {
                            throw new ConfigException(name, value, "Duplicate alias provided.");
                        }
                    }
                }, Importance.LOW, TRANSFORMS_DOC, TRANSFORMS_GROUP, 6, Width.LONG, TRANSFORMS_DISPLAY);
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

    /**
     * Returns the initialized list of {@link Transformation} which are specified in {@link #TRANSFORMS_CONFIG}.
     */
    public <R extends ConnectRecord<R>> List<Transformation<R>> transformations() {
        final List<String> transformAliases = getList(TRANSFORMS_CONFIG);
        if (transformAliases == null || transformAliases.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Transformation<R>> transformations = new ArrayList<>(transformAliases.size());
        for (String alias : transformAliases) {
            final String prefix = TRANSFORMS_CONFIG + "." + alias + ".";
            final Transformation<R> transformation;
            try {
                transformation = getClass(prefix + "type").asSubclass(Transformation.class).newInstance();
            } catch (Exception e) {
                throw new ConnectException(e);
            }
            transformation.configure(originalsWithPrefix(prefix));
            transformations.add(transformation);
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
        try {
            return (transformationCls.asSubclass(Transformation.class).newInstance()).config();
        } catch (Exception e) {
            throw new ConfigException(key, String.valueOf(transformationCls), "Error getting config definition from Transformation: " + e.getMessage());
        }
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
