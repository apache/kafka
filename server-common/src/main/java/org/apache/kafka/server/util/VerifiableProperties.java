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
import java.util.Properties;

public class VerifiableProperties {
    private final Properties props;

    public VerifiableProperties(Properties props) {
        this.props = props;
    }

    public VerifiableProperties(Map<String, Object> map) {
        Properties props = new Properties();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue().toString());
        }
        this.props = props;
    }

    public String getProperty(String name) {
        String value = props.getProperty(name);
        return value == null ? null : value.trim();
    }

    /**
     * Read a required integer property value or throw an exception if no such property is found
     */
    public int getInt(String name) {
        return Integer.parseInt(getString(name));
    }

    /**
     * Read an integer from the properties instance
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @return the integer value
     */
    public int getInt(String name, int defaultValue) {
        return getIntInRange(name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Read an integer from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @param minValue The minimum value (inclusive)
     * @param maxValue The maximum value (inclusive)
     * @throws IllegalArgumentException If the value is not in the given range
     * @return the integer value
     */
    private int getIntInRange(String name, int defaultValue, int minValue, int maxValue) {
        int value;
        if (props.containsKey(name)) {
            value = Integer.parseInt(getProperty(name));
        } else {
            value = defaultValue;
        }

        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(name + " has value " + value + " which is not in the range [" + minValue + ", " + maxValue + "].");
        }
        return value;
    }

    /**
     * Read a required long property value or throw an exception if no such property is found
     */
    public long getLong(String name) {
        return Long.parseLong(getString(name));
    }

    /**
     * Read a long from the properties instance
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @return the long value
     */
    public long getLong(String name, long defaultValue) {
        return getLongInRange(name, defaultValue, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Read a long from the properties instance. Throw an exception
     * if the value is not in the given range (inclusive)
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @param minValue The minimum value (inclusive)
     * @param maxValue The maximum value (inclusive)
     * @throws IllegalArgumentException If the value is not in the given range
     * @return the long value
     */
    private long getLongInRange(String name, long defaultValue, long minValue, long maxValue) {
        long value;
        if (props.containsKey(name)) {
            value = Long.parseLong(getProperty(name));
        } else {
            value = defaultValue;
        }

        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(name + " has value " + value +
                    " which is not in the range [" + minValue + ", " + maxValue + "].");
        }
        return value;
    }

    /**
     * Read a boolean value from the properties instance
     * @param name The property name
     * @param defaultValue The default value to use if the property is not found
     * @return the boolean value
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        if (!props.containsKey(name)) {
            return defaultValue;
        } else {
            String value = getProperty(name);
            if (!"true".equals(value) && !"false".equals(value)) {
                throw new IllegalArgumentException("Unacceptable value for property '" + name +
                        "', boolean values must be either 'true' or 'false'");
            }
            return Boolean.parseBoolean(value);
        }
    }

    public boolean getBoolean(String name) {
        return Boolean.parseBoolean(getString(name));
    }

    /**
     * Get a string property, or, if no such property is defined, return the given default value
     */
    public String getString(String name, String defaultValue) {
        if (props.containsKey(name)) {
            return getProperty(name);
        } else {
            return defaultValue;
        }
    }

    /**
     * Get a string property or throw an exception if no such property is defined.
     */
    public String getString(String name) {
        if (!props.containsKey(name)) {
            throw new IllegalArgumentException("Missing required property '" + name + "'");
        }
        return getProperty(name);
    }

    @Override
    public String toString() {
        return props.toString();
    }
}
