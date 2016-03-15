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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

/**
 * This class is used for specifying the set of expected configurations, their type, their defaults, their
 * documentation, and any special validation logic used for checking the correctness of the values the user provides.
 * <p/>
 * Usage of this class looks something like this:
 * <p/>
 * <pre>
 * ConfigDef defs = new ConfigDef();
 * defs.define(&quot;config_name&quot;, Type.STRING, &quot;default string value&quot;, &quot;This configuration is used for blah blah blah.&quot;);
 * defs.define(&quot;another_config_name&quot;, Type.INT, 42, Range.atLeast(0), &quot;More documentation on this config&quot;);
 *
 * Properties props = new Properties();
 * props.setProperty(&quot;config_name&quot;, &quot;some value&quot;);
 * Map&lt;String, Object&gt; configs = defs.parse(props);
 *
 * String someConfig = (String) configs.get(&quot;config_name&quot;); // will return &quot;some value&quot;
 * int anotherConfig = (Integer) configs.get(&quot;another_config_name&quot;); // will return default value of 42
 * </pre>
 * <p/>
 * This class can be used stand-alone or in combination with {@link AbstractConfig} which provides some additional
 * functionality for accessing configs.
 */
public class ConfigDef {

    public static final Object NO_DEFAULT_VALUE = new String("");

    private final Map<String, ConfigKey> configKeys = new HashMap<String, ConfigKey>();

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
     *
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param defaultValue  The default value to use if this config isn't present
     * @param validator     A validator to use in checking the correctness of the config
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation) {
        if (configKeys.containsKey(name))
            throw new ConfigException("Configuration " + name + " is defined twice.");
        Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
        configKeys.put(name, new ConfigKey(name, type, parsedDefault, validator, importance, documentation));
        return this;
    }

    /**
     * Define a new configuration with no special validation logic
     *
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param defaultValue  The default value to use if this config isn't present
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     *
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
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
     * @param props The configs to parse and validate
     * @return Parsed and validated configs. The key will be the config name and the value will be the value parsed into
     * the appropriate type (int, string, etc)
     */
    public Map<String, Object> parse(Map<?, ?> props) {
        /* parse all known keys */
        Map<String, Object> values = new HashMap<String, Object>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            // props map contains setting - assign ConfigKey value
            if (props.containsKey(key.name))
                value = parseType(key.name, props.get(key.name), key.type);
            // props map doesn't contain setting, the key is required because no default value specified - its an error
            else if (key.defaultValue == NO_DEFAULT_VALUE)
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            // otherwise assign setting its default value
            else
                value = key.defaultValue;
            if (key.validator != null)
                key.validator.ensureValid(key.name, value);
            values.put(key.name, value);
        }
        return values;
    }

    /**
     * Parse a value according to its expected type.
     *
     * @param name  The config name
     * @param value The config value
     * @param type  The expected type
     * @return The parsed object
     */
    private Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) return null;

            String trimmed = null;
            if (value instanceof String)
                trimmed = ((String) value).trim();

            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true"))
                            return true;
                        else if (trimmed.equalsIgnoreCase("false"))
                            return false;
                        else
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                    } else if (value instanceof Boolean)
                        return value;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                case PASSWORD:
                    if (value instanceof Password)
                        return value;
                    else if (value instanceof String)
                        return new Password(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case STRING:
                    if (value instanceof String)
                        return trimmed;
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case INT:
                    if (value instanceof Integer) {
                        return (Integer) value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return (Short) value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case LONG:
                    if (value instanceof Integer)
                        return ((Integer) value).longValue();
                    if (value instanceof Long)
                        return (Long) value;
                    else if (value instanceof String)
                        return Long.parseLong(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case DOUBLE:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof String)
                        return Double.parseDouble(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case LIST:
                    if (value instanceof List)
                        return (List<?>) value;
                    else if (value instanceof String)
                        if (trimmed.isEmpty())
                            return Collections.emptyList();
                        else
                            return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
                    else
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                case CLASS:
                    if (value instanceof Class)
                        return (Class<?>) value;
                    else if (value instanceof String)
                        return Class.forName(trimmed, true, Utils.getContextOrKafkaClassLoader());
                    else
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    /**
     * The config types
     */
    public enum Type {
        BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    }

    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    /**
     * Validation logic the user may provide
     */
    public interface Validator {
        public void ensureValid(String name, Object o);
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

    private static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation) {
            super();
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && this.hasDefault())
                this.validator.ensureValid(name, defaultValue);
            this.documentation = documentation;
        }

        public boolean hasDefault() {
            return this.defaultValue != NO_DEFAULT_VALUE;
        }

    }

    private List<ConfigDef.ConfigKey> getSortedList() {
        List<ConfigDef.ConfigKey> configs = new ArrayList<ConfigDef.ConfigKey>(this.configKeys.values());
        Collections.sort(configs, new Comparator<ConfigDef.ConfigKey>() {
            public int compare(ConfigDef.ConfigKey k1, ConfigDef.ConfigKey k2) {
                // first take anything with no default value (therefore required)
                if (!k1.hasDefault() && k2.hasDefault())
                    return -1;
                else if (!k2.hasDefault() && k1.hasDefault())
                    return 1;

                // then sort by importance
                int cmp = k1.importance.compareTo(k2.importance);
                if (cmp == 0)
                    // then sort in alphabetical order
                    return k1.name.compareTo(k2.name);
                else
                    return cmp;
            }
        });
        return configs;
    }

    public String toMarkdown() {
        StringBuilder b = new StringBuilder();

        List<ConfigDef.ConfigKey> configs = getSortedList();
        String[] headers = new String[]{
            "Name", "Description", "Type", "Default", "Valid Values", "Importance"
        };
        int[] lengths = new int[headers.length];
        for (int i = 0; i < headers.length; i++) {
            lengths[i] = headers[i].length();
        }

        for (ConfigDef.ConfigKey def:configs) {
            for (int i = 0; i < headers.length; i++) {
                int length = 0;
                switch (i) {
                    case 0: //Name
                        length = null == def.name ? 0 : def.name.length();
                        break;
                    case 1:
                        length = null == def.documentation ? 0 : def.documentation.length();
                        break;
                    case 2:
                        length = null == def.type ? 0 : def.type.toString().length();
                        break;
                    case 3:
                        String defaultValue = getDefaultValue(def);
                        length = null == defaultValue ? 0 : defaultValue.length();
                        break;
                    case 4:
                        String validValues = def.validator != null ? def.validator.toString() : "";
                        length = null == validValues ? 0 : validValues.length();
                        break;
                    case 5:
                        length = null == def.importance ? 0 : def.importance.toString().length();
                        break;
                    default:
                        throw new IllegalArgumentException("There are more headers than columns.");
                }
                if (length > lengths[i]) {
                    lengths[i] = length;
                }
            }
        }

        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];
            String format = " %-" + lengths[i] + "s ";
            String value = String.format(format, header);

            if (i == 0) {
                b.append("|");
            }
            b.append(value);
            b.append("|");
            if (i == headers.length - 1) {
                b.append("\n");
            }
        }

        for (int i = 0; i < headers.length; i++) {
            String format = " %-" + lengths[i] + "s ";
            String value = String.format(format, "").replace(" ", "-");

            if (i == 0) {
                b.append("|");
            }
            b.append(value);
            b.append("|");
            if (i == headers.length - 1) {
                b.append("\n");
            }
        }

        for (ConfigDef.ConfigKey def:configs) {
            for (int i = 0; i < headers.length; i++) {
                int length = lengths[i];
                String format = " %-" + lengths[i] + "s ";
                String value;
                switch (i) {
                    case 0: //Name
                        value = def.name;
                        break;
                    case 1:
                        value = null == def.documentation ? "" : def.documentation;
                        break;
                    case 2:
                        value = def.type.toString().toLowerCase();
                        break;
                    case 3:
                        String defaultValue = getDefaultValue(def);
                        value = null == defaultValue ? "" : defaultValue;
                        break;
                    case 4:
                        String validValues = def.validator != null ? def.validator.toString() : "";
                        value = null == validValues ? "" : validValues;
                        break;
                    case 5:
                        value = def.importance.toString().toLowerCase();
                        break;
                    default:
                        throw new IllegalArgumentException("There are more headers than columns.");
                }

                if (i == 0) {
                    b.append("|");
                }
                b.append(String.format(format, value));
                b.append("|");
                if (i == headers.length - 1) {
                    b.append("\n");
                }
            }
        }

        return b.toString();
    }

    static String getDefaultValue(ConfigKey def) {
        if (def.hasDefault()) {
            if (def.defaultValue == null)
                return "null";
            else if (def.type == Type.STRING && def.defaultValue.toString().isEmpty())
                return "\"\"";
            else
                return def.defaultValue.toString();
        } else
            return "";
    }

    public String toHtmlTable() {
        // sort first required fields, then by importance, then name
        List<ConfigDef.ConfigKey> configs = getSortedList();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        b.append("<th>Name</th>\n");
        b.append("<th>Description</th>\n");
        b.append("<th>Type</th>\n");
        b.append("<th>Default</th>\n");
        b.append("<th>Valid Values</th>\n");
        b.append("<th>Importance</th>\n");
        b.append("</tr>\n");
        for (ConfigKey def : configs) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(def.name);
            b.append("</td>");
            b.append("<td>");
            b.append(def.documentation);
            b.append("</td>");
            b.append("<td>");
            b.append(def.type.toString().toLowerCase());
            b.append("</td>");
            b.append("<td>");
            b.append(getDefaultValue(def));
            b.append("</td>");
            b.append("<td>");
            b.append(def.validator != null ? def.validator.toString() : "");
            b.append("</td>");
            b.append("<td>");
            b.append(def.importance.toString().toLowerCase());
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }
}
