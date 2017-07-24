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

import java.util.Objects;

/**
 * A class representing a configuration entry containing name, value and additional metadata.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ConfigEntry {

    private final String name;
    private final String value;
    private final boolean isDefault;
    private final boolean isSensitive;
    private final boolean isReadOnly;

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
     */
    public ConfigEntry(String name, String value, boolean isDefault, boolean isSensitive, boolean isReadOnly) {
        Objects.requireNonNull(name, "name should not be null");
        this.name = name;
        this.value = value;
        this.isDefault = isDefault;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
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
    public String value() {
        return value;
    }

    /**
     * Return whether the config value is the default or if it's been explicitly set.
     */
    public boolean isDefault() {
        return isDefault;
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

    @Override
    public String toString() {
        return "ConfigEntry(" +
                "name=" + name +
                ", value=" + value +
                ", isDefault=" + isDefault +
                ", isSensitive=" + isSensitive +
                ", isReadOnly=" + isReadOnly +
                ")";
    }
}
