/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class is used for specifying the set of expected configurations. For each configuration, you can specify
 * the name, the type, the default value, the documentation, the group information, the order in the group,
 * the width of the configuration value and the name suitable for display in the UI.
 *
 * You can provide special validation logic used for single configuration validation by overriding {@link Validator}.
 *
 * Moreover, you can specify the dependents of a configuration. The valid values and visibility of a configuration
 * may change according to the values of other configurations. You can override {@link Recommender} to get valid
 * values and set visibility of a configuration given the current configuration values.
 *
 * <p/>
 * To use the class:
 * <p/>
 * <pre>
 * ConfigDef defs = new ConfigDef();
 *
 * defs.define(&quot;config_with_default&quot;, Type.STRING, &quot;default string value&quot;, &quot;Configuration with default value.&quot;);
 * defs.define(&quot;config_with_validator&quot;, Type.INT, 42, Range.atLeast(0), &quot;Configuration with user provided validator.&quot;);
 * defs.define(&quot;config_with_dependents&quot;, Type.INT, &quot;Configuration with dependents.&quot;, &quot;group&quot;, 1, &quot;Config With Dependents&quot;, Arrays.asList(&quot;config_with_default;&quot;,&quot;config_with_validator&quot;));
 *
 * Map&lt;String, String&gt; props = new HashMap&lt;&gt();
 * props.put(&quot;config_with_default&quot;, &quot;some value&quot;);
 * props.put(&quot;config_with_dependents&quot;, &quot;some other value&quot;);
 * // will return &quot;some value&quot;
 * Map&lt;String, Object&gt; configs = defs.parse(props);
 * String someConfig = (String) configs.get(&quot;config_with_default&quot;);
 * // will return default value of 42
 * int anotherConfig = (Integer) configs.get(&quot;config_with_validator&quot;);
 *
 * To validate the full configuration, use:
 * List&lt;Config&gt; configs = def.validate(props);
 * The {@link Config} contains updated configuration information given the current configuration values.
 * </pre>
 * <p/>
 * This class can be used standalone or in combination with {@link AbstractConfig} which provides some additional
 * functionality for accessing configs.
 */
public class ConfigDef {

    public static final Object NO_DEFAULT_VALUE = new String("");

    private final Map<String, ConfigKey> configKeys = new HashMap<>();
    private final List<String> groups = new LinkedList<>();
    private Set<String> configsWithNoParent;

    /**
     * Returns unmodifiable set of properties names defined in this {@linkplain ConfigDef}
     *
     * @return new unmodifiable {@link Set} instance containing the keys
     */
    public Set<String> names() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    /**
     * Define a new configuration
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        if (configKeys.containsKey(name)) {
            throw new ConfigException("Configuration " + name + " is defined twice.");
        }
        if (group != null && !groups.contains(group)) {
            groups.add(group);
        }
        Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
        configKeys.put(name, new ConfigKey(name, type, parsedDefault, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender));
        return this;
    }

    /**
     * Define a new configuration with no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no dependents
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no special validation logic
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no special validation logic, not dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic, no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no group, no order in group, no width, no display name, no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Validator validator, Importance importance, String documentation) {
        return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
    }

    /**
     * Define a new configuration with no special validation logic
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param defaultValue  The default value to use if this config isn't present
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, ConfigType type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
    }

    /**
     * Get the configuration keys
     * @return a map containing all configuration keys
     */
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    /**
     * Get the groups for the configuration
     * @return a list of group names
     */
    public List<String> groups() {
        return groups;
    }

    /**
     * Add standard SSL client configuration options.
     * @return this
     */
    public ConfigDef withClientSslSupport() {
        SslConfigs.addClientSslSupport(this);
        return this;
    }

    /**
     * Add standard SASL client configuration options.
     * @return this
     */
    public ConfigDef withClientSaslSupport() {
        SaslConfigs.addClientSaslSupport(this);
        return this;
    }

    /**
     * Parse and validate configs against this configuration definition. The input is a map of configs. It is expected
     * that the keys of the map are strings, but the values can either be strings or they may already be of the
     * appropriate type (int, string, etc). This will work equally well with either java.util.Properties instances or a
     * programmatically constructed map.
     *
     * @param props The configs to parse and validate.
     * @return Parsed and validated configs. The key will be the config name and the value will be the value parsed into
     * the appropriate type (int, string, etc).
     */
    public Map<String, Object> parse(Map<?, ?> props) {
        // Check all configurations are defined
        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        if (!undefinedConfigKeys.isEmpty()) {
            String joined = Utils.join(undefinedConfigKeys, ",");
            throw new ConfigException("Some configurations in are referred in the dependents, but not defined: " + joined);
        }
        // parse all known keys
        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            // props map contains setting - assign ConfigKey value
            if (props.containsKey(key.name)) {
                value = parseType(key.name, props.get(key.name), key.type);
                // props map doesn't contain setting, the key is required because no default value specified - its an error
            } else if (key.defaultValue == NO_DEFAULT_VALUE) {
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            } else {
                // otherwise assign setting its default value
                value = key.defaultValue;
            }
            if (key.validator != null) {
                key.validator.ensureValid(key.name, value);
            }
            values.put(key.name, value);
        }
        return values;
    }

    /**
     * Validate the current configuration values with the configuration definition.
     * @param props the current configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    public List<ConfigValue> validate(Map<String, String> props) {
        return new ArrayList<>(validateAll(props).values());
    }

    public Map<String, ConfigValue> validateAll(Map<String, String> props) {
        Map<String, ConfigValue> configValues = new HashMap<>();
        for (String name: configKeys.keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        for (String undefinedConfigKey: undefinedConfigKeys) {
            ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
            undefinedConfigValue.addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValues.put(undefinedConfigKey, undefinedConfigValue);
        }

        Map<String, Object> parsed = parseForValidate(props, configValues);
        return validate(parsed, configValues);
    }

    // package accessible for testing
    Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
        Map<String, Object> parsed = new HashMap<>();
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            parseForValidate(name, props, parsed, configValues);
        }
        return parsed;
    }


    private Map<String, ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            validate(name, parsed, configValues);
        }
        return configValues;
    }

    private List<String> undefinedDependentConfigs() {
        Set<String> undefinedConfigKeys = new HashSet<>();
        for (String configName: configKeys.keySet()) {
            ConfigKey configKey = configKeys.get(configName);
            List<String> dependents = configKey.dependents;
            for (String dependent: dependents) {
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent);
                }
            }
        }
        return new ArrayList<>(undefinedConfigKeys);
    }

    private Set<String> getConfigsWithNoParent() {
        if (this.configsWithNoParent != null) {
            return this.configsWithNoParent;
        }
        Set<String> configsWithParent = new HashSet<>();

        for (ConfigKey configKey: configKeys.values()) {
            List<String> dependents = configKey.dependents;
            configsWithParent.addAll(dependents);
        }

        Set<String> configs = new HashSet<>(configKeys.keySet());
        configs.removeAll(configsWithParent);
        this.configsWithNoParent = configs;
        return configs;
    }

    private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);

        Object value = null;
        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props.get(key.name), key.type);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        } else if (key.defaultValue == NO_DEFAULT_VALUE) {
            config.addErrorMessage("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            value = key.defaultValue;
        }

        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }
        config.value(value);
        parsed.put(name, value);
        for (String dependent: key.dependents) {
            parseForValidate(dependent, props, parsed, configs);
        }
    }

    private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);
        List<Object> recommendedValues;
        if (key.recommender != null) {
            try {
                recommendedValues = key.recommender.validValues(name, parsed);
                List<Object> originalRecommendedValues = config.recommendedValues();
                if (!originalRecommendedValues.isEmpty()) {
                    Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
                    Iterator<Object> it = recommendedValues.iterator();
                    while (it.hasNext()) {
                        Object o = it.next();
                        if (!originalRecommendedValueSet.contains(o)) {
                            it.remove();
                        }
                    }
                }
                config.recommendedValues(recommendedValues);
                config.visible(key.recommender.visible(name, parsed));
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }

        configs.put(name, config);
        for (String dependent: key.dependents) {
            validate(dependent, parsed, configs);
        }
    }

    /**
     * Parse a value according to its expected type.
     * @param name  The config name
     * @param value The config value
     * @param type  The expected type
     * @return The parsed object
     */
    private Object parseType(String name, Object value, ConfigType type) {
        if (value == null) return null;
        Object trimmed = value;
        if (value instanceof String)
            trimmed = ((String) value).trim();
        return type.parseType(name, trimmed);
    }

    public static String convertToString(Object parsedValue, ConfigType type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        return type.convertToString(parsedValue);
    }

    public interface ConfigType {
        String convertToString(Object parsedValue);

        Object parseType(String name, Object value);
    }

    /**
     * The atomic config types.
     */
    public enum Type implements ConfigType {
        BOOLEAN {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof String) {
                    if ("true".equalsIgnoreCase((String) value))
                        return true;
                    else if ("false".equalsIgnoreCase((String) value))
                        return false;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                } else if (value instanceof Boolean)
                    return value;
                else
                    throw new ConfigException(name, value, "Expected value to be either true or false");
            }
        },
        STRING {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof String)
                    return value;
                else
                    throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
            }
        },
        INT {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof String) {
                    try {
                        return Integer.parseInt((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new ConfigException(name, value, "Not a number of type " + this);
                    }
                } else {
                    throw new ConfigException(name, value, "Expected value to be an number.");
                }
            }
        },
        SHORT {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Short) {
                    return value;
                } else if (value instanceof String) {
                    try {
                        return Short.parseShort((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new ConfigException(name, value, "Not a number of type " + this);
                    }
                } else {
                    throw new ConfigException(name, value, "Expected value to be an number.");
                }
            }
        },
        LONG {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Long) {
                    return value;
                } else if (value instanceof String) {
                    try {
                        return Long.parseLong((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new ConfigException(name, value, "Not a number of type " + this);
                    }
                } else {
                    throw new ConfigException(name, value, "Expected value to be an number.");
                }
            }
        },
        DOUBLE {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else if (value instanceof String) {
                    try {
                        return Double.parseDouble((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new ConfigException(name, value, "Not a number of type " + this);
                    }
                } else {
                    throw new ConfigException(name, value, "Expected value to be an number.");
                }
            }
        },
        /**
         * Equivalent to {@code new ArrayType(ConfigDef.Type.STRING)}.
         *
         * @deprecated use {@link ArrayType}
         */
        @Deprecated
        LIST {
            @Override
            public String convertToString(Object parsedValue) {
                return Utils.join((List<?>) parsedValue, ",");
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof List)
                    return value;
                else if (value instanceof String)
                    if (((String) value).isEmpty())
                        return Collections.emptyList();
                    else
                        return Arrays.asList(((String) value).split("\\s*,\\s*", -1));
                else
                    throw new ConfigException(name, value, "Expected a comma separated list.");
            }
        },
        CLASS {
            @Override
            public String convertToString(Object parsedValue) {
                return ((Class<?>) parsedValue).getCanonicalName();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Class)
                    return value;
                else if (value instanceof String)
                    try {
                        return Class.forName((String) value, true, Utils.getContextOrKafkaClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new ConfigException(name, value, "Class " + value + " could not be found.");
                    }
                else
                    throw new ConfigException(name, value, "Expected a Class instance or class name.");
            }
        },
        PASSWORD {
            @Override
            public String convertToString(Object parsedValue) {
                return parsedValue.toString();
            }

            @Override
            public Object parseType(String name, Object value) {
                if (value instanceof Password)
                    return value;
                else if (value instanceof String)
                    return new Password((String) value);
                else
                    throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
            }
        }
    }

    /**
     * Array of elements of the specified value type. Each element is delimited by a comma.
     *
     * TODO: how should escaping be handled?
     *
     * The runtime representation is {@code List<Object>}.
     */
    public class ArrayType implements ConfigType {
        public final Type valueType;

        public ArrayType(Type valueType) {
            this.valueType = valueType;
        }

        @Override
        public String convertToString(Object parsedValue) {
            // TODO
            return null;
        }

        @Override
        public Object parseType(String name, Object value) {
            // TODO
            return null;
        }

        @Override
        public String toString() {
            return "ARRAY[" + valueType + "]";
        }
    }

    /**
     * Map of String keys and specified value type. The key and value are separated by a colon, and each key-value pair is separated by comma.
     *
     * TODO: how should escaping be handled?
     *
     * The runtime representation is {@code Map<String, Object>}.
     */
    public class MapType implements ConfigType {
        public final Type valueType;

        public MapType(Type valueType) {
            this.valueType = valueType;
        }

        @Override
        public String convertToString(Object parsedValue) {
            // TODO
            return null;
        }

        @Override
        public Object parseType(String name, Object value) {
            // TODO
            return null;
        }

        @Override
        public String toString() {
            return "MAP[" + valueType + "]";
        }
    }

    /**
     * The importance level for a configuration
     */
    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    /**
     * The width of a configuration value
     */
    public enum Width {
        NONE, SHORT, MEDIUM, LONG
    }

    /**
     * This is used by the {@link #validate(Map)} to get valid values for a configuration given the current
     * configuration values in order to perform full configuration validation and visibility modification.
     * In case that there are dependencies between configurations, the valid values and visibility
     * for a configuration may change given the values of other configurations.
     */
    public interface Recommender {

        /**
         * The valid values for the configuration given the current configuration values.
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The list of valid values. To function properly, the returned objects should have the type
         * defined for the configuration using the recommender.
         */
        List<Object> validValues(String name, Map<String, Object> parsedConfig);

        /**
         * Set the visibility of the configuration given the current configuration values.
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The visibility of the configuration
         */
        boolean visible(String name, Map<String, Object> parsedConfig);
    }

    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    public interface Validator {
        /**
         * Perform single configuration validation.
         * @param name The name of the configuration
         * @param value The value of the configuration
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        /**
         * A numeric range that checks both the upper and lower bound
         */
        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new ConfigException(name, o, "Value must be non-null");
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue())
                throw new ConfigException(name, o, "Value must be at least " + min);
            if (max != null && n.doubleValue() > max.doubleValue())
                throw new ConfigException(name, o, "Value must be no more than " + max);
        }

        public String toString() {
            if (min == null)
                return "[...," + max + "]";
            else if (max == null)
                return "[" + min + ",...]";
            else
                return "[" + min + ",...," + max + "]";
        }
    }

    public static class ValidList implements Validator {

        ValidString validString;

        private ValidList(List<String> validStrings) {
            this.validString = new ValidString(validStrings);
        }

        public static ValidList in(String... validStrings) {
            return new ValidList(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            List<String> values = (List<String>) value;
            for (String string : values) {
                validString.ensureValid(name, string);
            }
        }

        public String toString() {
            return validString.toString();
        }
    }

    public static class ValidString implements Validator {
        List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + Utils.join(validStrings, ", "));
            }

        }

        public String toString() {
            return "[" + Utils.join(validStrings, ", ") + "]";
        }
    }

    public static class ConfigKey {
        public final String name;
        public final ConfigType type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;
        public final String group;
        public final int orderInGroup;
        public final Width width;
        public final String displayName;
        public final List<String> dependents;
        public final Recommender recommender;

        public ConfigKey(String name, ConfigType type, Object defaultValue, Validator validator,
                         Importance importance, String documentation, String group,
                         int orderInGroup, Width width, String displayName,
                         List<String> dependents, Recommender recommender) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && this.hasDefault())
                this.validator.ensureValid(name, defaultValue);
            this.documentation = documentation;
            this.dependents = dependents;
            this.group = group;
            this.orderInGroup = orderInGroup;
            this.width = width;
            this.displayName = displayName;
            this.recommender = recommender;
        }

        public boolean hasDefault() {
            return this.defaultValue != NO_DEFAULT_VALUE;
        }
    }

    protected List<String> headers() {
        return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", "Importance");
    }

    protected String getConfigValue(ConfigKey key, String headerName) {
        switch (headerName) {
            case "Name":
                return key.name;
            case "Description":
                return key.documentation;
            case "Type":
                return key.type.toString().toLowerCase(Locale.ROOT);
            case "Default":
                if (key.hasDefault()) {
                    if (key.defaultValue == null)
                        return "null";
                    String defaultValueStr = convertToString(key.defaultValue, key.type);
                    if (defaultValueStr.isEmpty())
                        return "\"\"";
                    else
                        return defaultValueStr;
                } else
                    return "";
            case "Valid Values":
                return key.validator != null ? key.validator.toString() : "";
            case "Importance":
                return key.importance.toString().toLowerCase(Locale.ROOT);
            default:
                throw new RuntimeException("Can't find value for header '" + headerName + "' in " + key.name);
        }
    }

    public String toHtmlTable() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        // print column headers
        for (String headerName : headers()) {
            b.append("<th>");
            b.append(headerName);
            b.append("</th>\n");
        }
        b.append("</tr>\n");
        for (ConfigKey def : configs) {
            b.append("<tr>\n");
            // print column values
            for (String headerName : headers()) {
                b.append("<td>");
                b.append(getConfigValue(def, headerName));
                b.append("</td>");
            }
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    /**
     * Get the configs formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toRst() {
        StringBuilder b = new StringBuilder();
        for (ConfigKey def : sortedConfigs()) {
            getConfigKeyRst(def, b);
            b.append("\n");
        }
        return b.toString();
    }

    /**
     * Configs with new metadata (group, orderInGroup, dependents) formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toEnrichedRst() {
        StringBuilder b = new StringBuilder();

        String lastKeyGroupName = "";
        for (ConfigKey def : sortedConfigsByGroup()) {
            if (def.group != null) {
                if (!lastKeyGroupName.equalsIgnoreCase(def.group)) {
                    b.append(def.group).append("\n");

                    char[] underLine = new char[def.group.length()];
                    Arrays.fill(underLine, '^');
                    b.append(new String(underLine)).append("\n\n");
                }
                lastKeyGroupName = def.group;
            }

            getConfigKeyRst(def, b);

            if (def.dependents != null && def.dependents.size() > 0) {
                int j = 0;
                b.append("  * Dependents: ");
                for (String dependent : def.dependents) {
                    b.append("``");
                    b.append(dependent);
                    if (++j == def.dependents.size())
                        b.append("``");
                    else
                        b.append("``, ");
                }
                b.append("\n");
            }
            b.append("\n");
        }
        return b.toString();
    }

    /**
     * Shared content on Rst and Enriched Rst.
     */
    private void getConfigKeyRst(ConfigKey def, StringBuilder b) {
        b.append("``").append(def.name).append("``").append("\n");
        for (String docLine : def.documentation.split("\n")) {
            if (docLine.length() == 0) {
                continue;
            }
            b.append("  ").append(docLine).append("\n\n");
        }
        b.append("  * Type: ").append(getConfigValue(def, "Type")).append("\n");
        if (def.hasDefault()) {
            b.append("  * Default: ").append(getConfigValue(def, "Default")).append("\n");
        }
        if (def.validator != null) {
            b.append("  * Valid Values: ").append(getConfigValue(def, "Valid Values")).append("\n");
        }
        b.append("  * Importance: ").append(getConfigValue(def, "Importance")).append("\n");
    }

    /**
     * Get a list of configs sorted into "natural" order: listing required fields first, then
     * ordering by importance, and finally by name.
     */
    protected List<ConfigKey> sortedConfigs() {
        // sort first required fields, then by importance, then name
        List<ConfigKey> configs = new ArrayList<>(this.configKeys.values());
        Collections.sort(configs, new Comparator<ConfigKey>() {
            public int compare(ConfigKey k1, ConfigKey k2) {
                // first take anything with no default value
                if (!k1.hasDefault() && k2.hasDefault()) {
                    return -1;
                } else if (!k2.hasDefault() && k1.hasDefault()) {
                    return 1;
                }

                // then sort by importance
                int cmp = k1.importance.compareTo(k2.importance);
                if (cmp == 0) {
                    // then sort in alphabetical order
                    return k1.name.compareTo(k2.name);
                } else {
                    return cmp;
                }
            }
        });
        return configs;
    }

    /**
     * Get a list of configs sorted taking the 'group' and 'orderInGroup' into account.
     */
    protected List<ConfigKey> sortedConfigsByGroup() {
        final Map<String, Integer> groupOrd = new HashMap<>(groups.size());
        int ord = 0;
        for (String group: groups) {
            groupOrd.put(group, ord++);
        }

        List<ConfigKey> configs = new ArrayList<>(configKeys.values());
        Collections.sort(configs, new Comparator<ConfigKey>() {
            @Override
            public int compare(ConfigKey k1, ConfigKey k2) {
                int cmp = k1.group == null
                        ? (k2.group == null ? 0 : -1)
                        : (k2.group == null ? 1 : Integer.compare(groupOrd.get(k1.group), groupOrd.get(k2.group)));
                if (cmp == 0) {
                    cmp = Integer.compare(k1.orderInGroup, k2.orderInGroup);
                }
                if (cmp == 0) {
                    cmp = k1.name.compareTo(k2.name);
                }
                return cmp;
            }
        });
        return configs;
    }

}