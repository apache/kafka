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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Client metric configuration related parameters and the supporting methods like validation, etc. are
 * defined in this class.
 * <p>
 * {
 * <ul>
 *   <li> name: Name supplied by CLI during the creation of the client metric subscription.
 *   <li> metrics: List of metric prefixes
 *   <li> intervalMs: A positive integer value >=0  tells the client that how often a client can push the metrics
 *   <li> match: List of client matching patterns, that are used by broker to match the client instance
 *   with the subscription.
 * </ul>
 * }
 * <p>
 * At present, CLI can pass the following parameters in request to add/delete/update the client metrics
 * subscription:
 * <ul>
 *    <li> "name" is a unique name for the subscription. This is used to identify the subscription in
 *          the broker. Ex: "METRICS-SUB"
 *    <li> "metrics" value should be comma separated metrics list. A prefix match on the requested metrics
 *          is performed in clients to determine subscribed metrics. An empty list means no metrics subscribed.
 *          A list containing just an empty string means all metrics subscribed.
 *          Ex: "org.apache.kafka.producer.partition.queue.,org.apache.kafka.producer.partition.latency"
 *
 *    <li> "interval.ms" should be between 100 and 3600000 (1 hour). This is the interval at which the client
 *          should push the metrics to the broker.
 *
 *    <li> "match" is a comma separated list of client match patterns, in case if there is no matching
 *          pattern specified then broker considers that as all match which means the associated metrics
 *          applies to all the clients. Ex: "client_software_name = Java, client_software_version = 11.1.*"
 *          which means all Java clients with any sub versions of 11.1 will be matched i.e. 11.1.1, 11.1.2 etc.
 * </ul>
 * For more information please look at kip-714:
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientmetricsconfiguration
 */
public class ClientMetricsConfigs extends AbstractConfig {

    public static final String SUBSCRIPTION_METRICS = "metrics";
    public static final String PUSH_INTERVAL_MS = "interval.ms";
    public static final String CLIENT_MATCH_PATTERN = "match";

    public static final String CLIENT_INSTANCE_ID = "client_instance_id";
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SOFTWARE_NAME = "client_software_name";
    public static final String CLIENT_SOFTWARE_VERSION = "client_software_version";
    public static final String CLIENT_SOURCE_ADDRESS = "client_source_address";
    public static final String CLIENT_SOURCE_PORT = "client_source_port";

    // '*' in client-metrics resource configs indicates that all the metrics are subscribed.
    public static final String ALL_SUBSCRIBED_METRICS_CONFIG = "*";

    public static final int DEFAULT_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
    private static final int MIN_INTERVAL_MS = 100; // 100ms
    private static final int MAX_INTERVAL_MS = 3600000; // 1 hour

    private static final Set<String> ALLOWED_MATCH_PARAMS = new HashSet<>(Arrays.asList(
        CLIENT_INSTANCE_ID,
        CLIENT_ID,
        CLIENT_SOFTWARE_NAME,
        CLIENT_SOFTWARE_VERSION,
        CLIENT_SOURCE_ADDRESS,
        CLIENT_SOURCE_PORT
    ));

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(SUBSCRIPTION_METRICS, Type.LIST, Collections.emptyList(), Importance.MEDIUM, "Subscription metrics list")
        .define(PUSH_INTERVAL_MS, Type.INT, DEFAULT_INTERVAL_MS, Importance.MEDIUM, "Push interval in milliseconds")
        .define(CLIENT_MATCH_PATTERN, Type.LIST, Collections.emptyList(), Importance.MEDIUM, "Client match pattern list");

    public ClientMetricsConfigs(Properties props) {
        super(CONFIG, props);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public static Set<String> names() {
        return CONFIG.names();
    }

    public static void validate(String subscriptionName, Properties properties) {
        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new InvalidRequestException("Subscription name can't be empty");
        }

        validateProperties(properties);
    }

    private static void validateProperties(Properties properties) {
        // Make sure that all the properties are valid
        properties.forEach((key, value) -> {
            if (!names().contains(key)) {
                throw new InvalidRequestException("Unknown client metrics configuration: " + key);
            }
        });

        // Make sure that push interval is between 100ms and 1 hour.
        if (properties.containsKey(PUSH_INTERVAL_MS)) {
            int pushIntervalMs = Integer.parseInt(properties.getProperty(PUSH_INTERVAL_MS));
            if (pushIntervalMs < MIN_INTERVAL_MS || pushIntervalMs > MAX_INTERVAL_MS) {
                String msg = String.format("Invalid value %s for %s, interval must be between 100 and 3600000 (1 hour)",
                    pushIntervalMs, PUSH_INTERVAL_MS);
                throw new InvalidRequestException(msg);
            }
        }

        // Make sure that client match patterns are valid by parsing them.
        if (properties.containsKey(CLIENT_MATCH_PATTERN)) {
            List<String> patterns = Arrays.asList(properties.getProperty(CLIENT_MATCH_PATTERN).split(","));
            // Parse the client matching patterns to validate if the patterns are valid.
            parseMatchingPatterns(patterns);
        }
    }

    /**
     * Parses the client matching patterns and builds a map with entries that has
     * (PatternName, PatternValue) as the entries.
     * Ex: "VERSION=1.2.3" would be converted to a map entry of (Version, 1.2.3)
     * <p>
     * NOTES:
     * Client match pattern splits the input into two parts separated by first occurrence of the character '='
     *
     * @param patterns List of client matching pattern strings
     * @return map of client matching pattern entries
     */
    public static Map<String, Pattern> parseMatchingPatterns(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Pattern> patternsMap = new HashMap<>();
        patterns.forEach(pattern -> {
            String[] nameValuePair = pattern.split("=");
            if (nameValuePair.length != 2) {
                throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
            }

            String param = nameValuePair[0].trim();
            if (!isValidParam(param)) {
                throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
            }

            try {
                Pattern patternValue = Pattern.compile(nameValuePair[1].trim());
                patternsMap.put(param, patternValue);
            } catch (PatternSyntaxException e) {
                throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
            }
        });

        return patternsMap;
    }

    private static boolean isValidParam(String paramName) {
        return ALLOWED_MATCH_PARAMS.contains(paramName);
    }
}
