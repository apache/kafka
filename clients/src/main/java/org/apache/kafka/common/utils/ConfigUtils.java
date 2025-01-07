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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    public static String configMapToRedactedString(Map<String, Object> map, ConfigDef configDef) {
        StringBuilder bld = new StringBuilder("{");
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        String prefix = "";
        for (String key : keys) {
            bld.append(prefix).append(key).append("=");
            ConfigKey configKey = configDef.configKeys().get(key);
            if (configKey == null || configKey.type().isSensitive()) {
                bld.append("(redacted)");
            } else {
                Object value = map.get(key);
                if (value == null) {
                    bld.append("null");
                } else if (configKey.type() == Type.STRING) {
                    bld.append("\"").append(value).append("\"");
                } else {
                    bld.append(value);
                }
            }
            prefix = ", ";
        }
        bld.append("}");
        return bld.toString();
    }

    /**
     * Finds and returns a boolean configuration option from the configuration map or the default value if the option is
     * not set.
     *
     * @param configs Map with the configuration options
     * @param key Configuration option for which the boolean value will be returned
     * @param defaultValue The default value that will be used when the key is not present
     * @return A boolean value of the configuration option of the default value
     */
    public static boolean getBoolean(final Map<String, Object> configs, final String key, final boolean defaultValue) {
        final Object value = configs.getOrDefault(key, defaultValue);
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else {
            log.error("Invalid value (" + value + ") on configuration '" + key + "'. The default value '" + defaultValue + "' will be used instead. Please specify a true/false value.");
            return defaultValue;
        }
    }
}
