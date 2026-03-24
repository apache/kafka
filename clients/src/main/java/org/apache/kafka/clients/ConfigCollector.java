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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.message.PushConfigRequestData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Utility class for collecting and filtering client configuration for transmission to brokers.
 * This class handles filtering out sensitive configuration (passwords, security settings, etc.)
 * and converting configuration to the format required by the PushConfig RPC.
 */
public class ConfigCollector {
    private static final Logger log = LoggerFactory.getLogger(ConfigCollector.class);

    /**
     * Collect non-sensitive configuration values for transmission to broker.
     *
     * @param config Client configuration (ConsumerConfig, ProducerConfig, etc.)
     * @param requestedKeys Keys requested by broker ("*" for all non-sensitive)
     * @param maxBytes Maximum payload size in bytes
     * @return List of config entries ready for PushConfigRequest
     */
    public static List<PushConfigRequestData.ClientConfig> collectConfigs(
            AbstractConfig config,
            List<String> requestedKeys,
            int maxBytes) {

        List<PushConfigRequestData.ClientConfig> result = new ArrayList<>();

        // Expand wildcard "*" to all keys
        Set<String> keysToInclude = expandKeys(config, requestedKeys);

        // Filter and convert
        int currentBytes = 0;
        for (String key : keysToInclude) {
            if (shouldExclude(key, config)) {
                continue;  // Skip sensitive configs
            }

            Object value = config.values().get(key);
            if (value == null) {
                continue;
            }

            ConfigDef.Type type = config.typeOf(key);
            if (type == null) {
                continue;  // Unknown config
            }

            PushConfigRequestData.ClientConfig entry =
                convertToClientConfig(key, value, type);

            // Check size limit
            int entrySize = estimateSize(entry);
            if (currentBytes + entrySize > maxBytes) {
                log.warn("Config payload would exceed {} bytes, truncating at {} entries",
                    maxBytes, result.size());
                break;
            }

            result.add(entry);
            currentBytes += entrySize;
        }

        log.debug("Collected {} config entries ({} bytes)", result.size(), currentBytes);
        return result;
    }

    /**
     * Expand wildcard or specific key list to actual keys to include.
     */
    private static Set<String> expandKeys(AbstractConfig config, List<String> requestedKeys) {
        Set<String> keysToInclude = new HashSet<>();

        for (String requestedKey : requestedKeys) {
            if ("*".equals(requestedKey)) {
                // Wildcard - include all keys from config
                keysToInclude.addAll(config.values().keySet());
            } else {
                // Specific key requested
                keysToInclude.add(requestedKey);
            }
        }

        return keysToInclude;
    }

    /**
     * Determine if a config key should be excluded from transmission.
     * Excludes passwords, security settings, class names, and other sensitive data.
     */
    private static boolean shouldExclude(String key, AbstractConfig config) {
        ConfigDef.Type type = config.typeOf(key);

        // 1. Exclude PASSWORD type
        if (type == ConfigDef.Type.PASSWORD) {
            return true;
        }

        // 2. Exclude CLASS type (per KIP requirement)
        if (type == ConfigDef.Type.CLASS) {
            return true;
        }

        // 3. Exclude bootstrap.servers
        if ("bootstrap.servers".equals(key)) {
            return true;
        }

        // 4. Exclude security/auth related keys
        String lowerKey = key.toLowerCase(Locale.ROOT);
        if (lowerKey.contains("sasl.") ||
            lowerKey.contains("ssl.") ||
            lowerKey.contains("security.")) {
            return true;
        }

        // 5. Exclude keys ending with sensitive suffixes
        if (lowerKey.endsWith(".password") ||
            lowerKey.endsWith(".secret") ||
            lowerKey.endsWith(".key") ||
            lowerKey.endsWith(".token")) {
            return true;
        }

        return false;
    }

    /**
     * Convert a config entry to the protocol format.
     */
    private static PushConfigRequestData.ClientConfig convertToClientConfig(
            String key,
            Object value,
            ConfigDef.Type type) {

        PushConfigRequestData.ClientConfig config = new PushConfigRequestData.ClientConfig();
        config.setName(key);
        config.setValue(String.valueOf(value));
        config.setType(mapConfigType(type));
        return config;
    }

    /**
     * Convert ConfigDef.Type to protocol byte value.
     */
    private static byte mapConfigType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN:
                return 0;
            case STRING:
                return 1;
            case INT:
                return 2;
            case SHORT:
                return 3;
            case LONG:
                return 4;
            case DOUBLE:
                return 5;
            case LIST:
                return 6;
            case CLASS:
                return 7;
            case PASSWORD:
                return 8;  // Should never reach here due to filtering
            default:
                return 1;  // Default to STRING
        }
    }

    /**
     * Estimate the size of a config entry in bytes.
     * This is a rough estimate for checking against maxBytes limit.
     */
    private static int estimateSize(PushConfigRequestData.ClientConfig config) {
        // Rough estimate: key length + value length + overhead for type and framing
        return config.name().length() + config.value().length() + 10;
    }
}
