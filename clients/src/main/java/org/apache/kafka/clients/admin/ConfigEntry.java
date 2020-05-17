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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A class representing a configuration entry containing name, value and additional metadata.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ConfigEntry {

    private final String name;
    private final Optional<String> value;
    private final ConfigSource source;
    private final boolean isSensitive;
    private final boolean isReadOnly;
    private final List<ConfigSynonym> synonyms;

    /**
     * Create a configuration entry with the provided values.
     *
     * @param name the non-null config name
     * @param value the config value or null
     */
    public ConfigEntry(String name, String value) {
        this(name, value, false, false, false);
    }

    /**
     * Create a configuration with the provided values.
     *
     * @param name the non-null config name
     * @param value the config value or null
     * @param isDefault whether the config value is the default or if it's been explicitly set
     * @param isSensitive whether the config value is sensitive, the broker never returns the value if it is sensitive
     * @param isReadOnly whether the config is read-only and cannot be updated
     * @deprecated since 1.1.0. This constructor will be removed in a future release.
     */
    @Deprecated
    public ConfigEntry(String name, String value, boolean isDefault, boolean isSensitive, boolean isReadOnly) {
        this(name,
             Optional.ofNullable(value),
             isDefault ? ConfigSource.DEFAULT_CONFIG : ConfigSource.UNKNOWN,
             isSensitive,
             isReadOnly,
             Collections.<ConfigSynonym>emptyList());
    }

    /**
     * Create a configuration with the provided values.
     *
     * @param name the non-null config name
     * @param value the config value or null
     * @param source the source of this config entry
     * @param isSensitive whether the config value is sensitive, the broker never returns the value if it is sensitive
     * @param isReadOnly whether the config is read-only and cannot be updated
     * @param synonyms Synonym configs in order of precedence
     */
    ConfigEntry(String name, Optional<String> value, ConfigSource source, boolean isSensitive, boolean isReadOnly,
                List<ConfigSynonym> synonyms) {
        Objects.requireNonNull(name, "name should not be null");
        this.name = name;
        this.value = value;
        this.source = source;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
        this.synonyms = synonyms;
    }

    /**
     * Return the config name.
     */
    public String name() {
        return name;
    }

    /**
     * Return the value or null. Null is returned if the config is unset or if isSensitive is true.
     */
    public Optional<String> value() {
        return value;
    }

    /**
     * Return the source of this configuration entry.
     */
    public ConfigSource source() {
        return source;
    }

    /**
     * Return whether the config value is the default or if it's been explicitly set.
     */
    public boolean isDefault() {
        return source == ConfigSource.DEFAULT_CONFIG;
    }

    /**
     * Return whether the config value is sensitive. The value is always set to null by the broker if the config value
     * is sensitive.
     */
    public boolean isSensitive() {
        return isSensitive;
    }

    /**
     * Return whether the config is read-only and cannot be updated.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Returns all config values that may be used as the value of this config along with their source,
     * in the order of precedence. The list starts with the value returned in this ConfigEntry.
     * The list is empty if synonyms were not requested using {@link DescribeConfigsOptions#includeSynonyms(boolean)}
     */
    public List<ConfigSynonym> synonyms() {
        return  synonyms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConfigEntry that = (ConfigEntry) o;

        return this.name.equals(that.name) &&
                this.value != null ? this.value.equals(that.value) : that.value == null &&
                this.isSensitive == that.isSensitive &&
                this.isReadOnly == that.isReadOnly &&
                this.source == that.source &&
                Objects.equals(this.synonyms, that.synonyms);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        result = prime * result + value.hashCode();
        result = prime * result + (isSensitive ? 1 : 0);
        result = prime * result + (isReadOnly ? 1 : 0);
        result = prime * result + source.hashCode();
        result = prime * result + synonyms.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConfigEntry(" +
                "name=" + name +
                ", value=" + value +
                ", source=" + source +
                ", isSensitive=" + isSensitive +
                ", isReadOnly=" + isReadOnly +
                ", synonyms=" + synonyms +
                ")";
    }


    /**
     * Source of configuration entries.
     */
    public enum ConfigSource {
        DYNAMIC_TOPIC_CONFIG,           // dynamic topic config that is configured for a specific topic
        DYNAMIC_BROKER_LOGGER_CONFIG,   // dynamic broker logger config that is configured for a specific broker
        DYNAMIC_BROKER_CONFIG,          // dynamic broker config that is configured for a specific broker
        DYNAMIC_DEFAULT_BROKER_CONFIG,  // dynamic broker config that is configured as default for all brokers in the cluster
        STATIC_BROKER_CONFIG,           // static broker config provided as broker properties at start up (e.g. server.properties file)
        DEFAULT_CONFIG,                 // built-in default configuration for configs that have a default value
        UNKNOWN                         // source unknown e.g. in the ConfigEntry used for alter requests where source is not set
    }

    /**
     * Class representing a configuration synonym of a {@link ConfigEntry}.
     */
    public static class ConfigSynonym {

        private final String name;
        private final String value;
        private final ConfigSource source;

        /**
         * Create a configuration synonym with the provided values.
         *
         * @param name Configuration name (this may be different from the name of the associated {@link ConfigEntry}
         * @param value Configuration value
         * @param source {@link ConfigSource} of this configuraton
         */
        ConfigSynonym(String name, String value, ConfigSource source) {
            this.name = name;
            this.value = value;
            this.source = source;
        }

        /**
         * Returns the name of this configuration.
         */
        public String name() {
            return name;
        }

        /**
         * Returns the value of this configuration, which may be null if the configuration is sensitive.
         */
        public String value() {
            return value;
        }

        /**
         * Returns the source of this configuration.
         */
        public ConfigSource source() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConfigSynonym that = (ConfigSynonym) o;
            return Objects.equals(name, that.name) && Objects.equals(value, that.value) && source == that.source;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, source);
        }

        @Override
        public String toString() {
            return "ConfigSynonym(" +
                    "name=" + name +
                    ", value=" + value +
                    ", source=" + source +
                    ")";
        }
    }
}
