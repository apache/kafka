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

package org.apache.kafka.server.util;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.require;

public class VerifiableProperties {
    private final Properties props;

    public VerifiableProperties(Properties props) {
        this.props = props;
    }

    public static VerifiableProperties fromMap(Map<String, Object> map) {
        Properties props = new Properties();
        props.putAll(map);

        return new VerifiableProperties(props);
    }

    public boolean containsKey(String name) {
        return props.containsKey(name);
    }

    /**
     * Read an integer from the properties instance.
     * @param name The property name.
     * @param defaultVal The default value to use if the property is not found.
     * @return the integer value.
     */
    public int getInt(String name, int defaultVal) {
        return containsKey(name) ? Integer.parseInt(Objects.requireNonNull(getProperty(name))) : defaultVal;
    }

    /**
     * Read a boolean value from the properties instance.
     * @param name The property name.
     * @param defaultVal The default value to use if the property is not found.
     * @return the boolean value.
     */
    public boolean getBoolean(String name, boolean defaultVal) {
        if (!containsKey(name))
            return defaultVal;

        String v = getProperty(name);
        require(Objects.equals(v, "true") || Objects.equals(v, "false"),
            "Unacceptable value for property '" + name + "', boolean values must be either 'true' or 'false");

        return Boolean.parseBoolean(v);
    }

    /**
     * Get a string property, or, if no such property is defined, return the given default value.
     */
    public String getString(String name, String defaultVal) {
        return containsKey(name) ? getProperty(name) : defaultVal;
    }

    /**
     * Get a string property or throw and exception if no such property is defined.
     */
    public String getString(String name) {
        require(containsKey(name), "Missing required property '" + name + "'");
        return getProperty(name);
    }

    private String getProperty(String name) {
        String value = props.getProperty(name);
        return value == null ? null : value.trim();
    }

    @Override
    public String toString() {
        return props.toString();
    }
}
