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

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

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
 *
 *    <li> "metrics" value should be comma-separated metrics list. A prefix match on the requested metrics
 *          is performed in clients to determine subscribed metrics. An empty list means no metrics subscribed.
 *          A list containing just an empty string means all metrics subscribed.
 *          Ex: "org.apache.kafka.producer.partition.queue.,org.apache.kafka.producer.partition.latency"
 *
 *    <li> "interval.ms" should be between 100 and 3600000 (1 hour). This is the interval at which the client
 *          should push the metrics to the broker.
 *
 *    <li> "match" is a comma-separated list of client match patterns, in case if there is no matching
 *          pattern specified then broker considers that as all match which means the associated metrics
 *          applies to all the clients. Ex: "client_software_name = Java, client_software_version = 11.1.*"
 *          which means all Java clients with any sub versions of 11.1 will be matched i.e. 11.1.1, 11.1.2 etc.
 * </ul>
 * For more information please look at
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientmetricsconfiguration">KIP-714</a>
 */
public class ClientMetricsConfigs extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsConfigs.class);

    public static final String METRICS_CONFIG = "metrics";
    public static final String INTERVAL_MS_CONFIG = "interval.ms";
    public static final String MATCH_CONFIG = "match";

    public static final String CLIENT_INSTANCE_ID = "client_instance_id";
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SOFTWARE_NAME = "client_software_name";
    public static final String CLIENT_SOFTWARE_VERSION = "client_software_version";
    public static final String CLIENT_SOURCE_ADDRESS = "client_source_address";
    public static final String CLIENT_SOURCE_PORT = "client_source_port";

    // '*' in client-metrics resource configs indicates that all the metrics are subscribed.
    public static final String ALL_SUBSCRIBED_METRICS = "*";

    public static final List<String> METRICS_DEFAULT = List.of();

    public static final int INTERVAL_MS_DEFAULT = 5 * 60 * 1000; // 5 minutes
    private static final int MIN_INTERVAL_MS = 100; // 100ms
    private static final int MAX_INTERVAL_MS = 3600000; // 1 hour

    public static final List<String> MATCH_DEFAULT = List.of();

    private static final Set<String> ALLOWED_MATCH_PARAMS = Set.of(
        CLIENT_INSTANCE_ID,
        CLIENT_ID,
        CLIENT_SOFTWARE_NAME,
        CLIENT_SOFTWARE_VERSION,
        CLIENT_SOURCE_ADDRESS,
        CLIENT_SOURCE_PORT
    );

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(METRICS_CONFIG, 
                Type.LIST, 
                METRICS_DEFAULT, 
                ConfigDef.ValidList.anyNonDuplicateValues(true, false), 
                Importance.MEDIUM, 
                "Telemetry metric name prefix list")
        .define(INTERVAL_MS_CONFIG, Type.INT, INTERVAL_MS_DEFAULT, Importance.MEDIUM, "Metrics push interval in milliseconds")
        .define(MATCH_CONFIG, 
                Type.LIST, 
                MATCH_DEFAULT,
                ConfigDef.ValidList.anyNonDuplicateValues(true, false),
                Importance.MEDIUM, 
                "Client match criteria");

    public ClientMetricsConfigs(Properties props) {
        super(CONFIG, props, false);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static Map<String, Object> defaultConfigsMap() {
        Map<String, Object> clientMetricsProps = new HashMap<>();
        clientMetricsProps.put(METRICS_CONFIG, METRICS_DEFAULT);
        clientMetricsProps.put(INTERVAL_MS_CONFIG, INTERVAL_MS_DEFAULT);
        clientMetricsProps.put(MATCH_CONFIG, MATCH_DEFAULT);
        return clientMetricsProps;
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * Validates a subscription's configs. Unknown keys and interval bounds are always enforced.
     * Match patterns are only strictly validated if {@code match} is new or changed relative to
     * {@code oldProps}.
     *
     * @param subscriptionName Name of the client metrics subscription being validated
     * @param newProps The full set of configs this subscription would have after this call
     * @param oldProps The subscription's previously persisted configs, or an empty map if new.
     */
    public static void validate(String subscriptionName, Map<?, ?> newProps, Map<?, ?> oldProps) {
        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new InvalidRequestException("Subscription name can't be empty");
        }

        validateConfigs(newProps, oldProps);
    }

    @SuppressWarnings("unchecked")
    private static void validateConfigs(Map<?, ?> configs, Map<?, ?> oldProps) {
        // Make sure that all the configs are valid
        configs.forEach((key, value) -> {
            if (!configNames().contains(key)) {
                throw new InvalidRequestException("Unknown client metrics configuration: " + key);
            }
        });

        Map<String, Object> parsed = CONFIG.parse(configs);

        // Make sure that push interval is between 100ms and 1 hour.
        if (configs.containsKey(INTERVAL_MS_CONFIG)) {
            int pushIntervalMs = (Integer) parsed.get(INTERVAL_MS_CONFIG);
            if (pushIntervalMs < MIN_INTERVAL_MS || pushIntervalMs > MAX_INTERVAL_MS) {
                String msg = String.format("Invalid value %s for %s, interval must be between 100 and 3600000 (1 hour)",
                    pushIntervalMs, INTERVAL_MS_CONFIG);
                throw new InvalidRequestException(msg);
            }
        }

        // Make sure that new or changed match patterns are valid RE2/J syntax.
        if (configs.containsKey(MATCH_CONFIG) && matchPatternsChanged(configs, oldProps)) {
            List<String> patterns = (List<String>) parsed.get(MATCH_CONFIG);
            patterns.forEach(pattern -> {
                String[] nameValuePair = splitMatchPattern(pattern);
                String param = nameValuePair[0];
                String patternValue = nameValuePair[1];

                compileStrict(param, patternValue);
            });
        }
    }

    private static boolean matchPatternsChanged(Map<?, ?> newProps, Map<?, ?> oldProps) {
        Object newMatch = newProps.get(MATCH_CONFIG);
        Object oldMatch = oldProps == null ? null : oldProps.get(MATCH_CONFIG);
        return !Objects.equals(newMatch, oldMatch);
    }

    /**
     * Compiles a single match pattern with RE2/J only.
     *
     * See also {@link #compileLenient(String, String, String)}.
     *
     * @throws InvalidConfigurationException if the pattern is not valid RE2/J syntax
     */
    private static ClientMatchPattern compileStrict(String param, String patternValue) {
        try {
            return ClientMatchPattern.ofRe2(Pattern.compile(patternValue));
        } catch (PatternSyntaxException e) {
            throw new InvalidConfigurationException(
                String.format("Client match pattern `%s=%s` is not a valid regular expression: %s.",
                    param, patternValue, e.getDescription()));
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
     * @param subscriptionName Name of the client metrics subscription these patterns belong to
     * @param patterns List of client matching pattern strings
     * @return map of client matching pattern entries
     */
    public static Map<String, ClientMatchPattern> parseMatchingPatterns(String subscriptionName, List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return Map.of();
        }

        Map<String, ClientMatchPattern> patternsMap = new HashMap<>();
        patterns.forEach(pattern -> {
            String[] nameValuePair = splitMatchPattern(pattern);
            String param = nameValuePair[0];
            String patternValue = nameValuePair[1];

            patternsMap.put(param, compileLenient(subscriptionName, param, patternValue));
        });

        return patternsMap;
    }

    /**
     * Compiles a single match pattern with RE2/J, falling back to java.util.regex (and logging a
     * warning) if that fails.
     *
     * See also {@link #compileStrict(String, String)}.
     *
     * @throws InvalidConfigurationException if the pattern is not valid under either engine
     */
    @SuppressWarnings("removal")
    private static ClientMatchPattern compileLenient(String subscriptionName, String param, String patternValue) {
        try {
            return ClientMatchPattern.ofRe2(Pattern.compile(patternValue));
        } catch (PatternSyntaxException re2Exception) {
            try {
                java.util.regex.Pattern legacyPattern = java.util.regex.Pattern.compile(patternValue);
                log.warn("Client metrics subscription '{}' match pattern '{}={}' relies on deprecated " +
                        "java.util.regex syntax ({}) and will stop being supported in Apache Kafka 5.0. " +
                        "Please update it to valid RE2/J syntax.",
                    subscriptionName, param, patternValue, re2Exception.getDescription());
                return ClientMatchPattern.ofLegacy(legacyPattern);
            } catch (java.util.regex.PatternSyntaxException legacyException) {
                throw new InvalidConfigurationException("Illegal client matching pattern: " + param + "=" + patternValue);
            }
        }
    }

    /**
     * Splits a client match pattern of the form {@code param=patternValue} into its two parts.
     * The pattern value may itself contain '=' characters, so the split only occurs at the first
     * occurrence.
     *
     * @param pattern a single client matching pattern string
     * @return a two-element array of {@code [param, patternValue]}, both trimmed
     * @throws InvalidConfigurationException if the pattern is malformed or the param name is unknown
     */
    private static String[] splitMatchPattern(String pattern) {
        String[] nameValuePair = pattern.split("=", 2);
        if (nameValuePair.length != 2) {
            throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
        }

        String param = nameValuePair[0].trim();
        if (!isValidParam(param)) {
            throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
        }

        return new String[] {param, nameValuePair[1].trim()};
    }

    private static boolean isValidParam(String paramName) {
        return ALLOWED_MATCH_PARAMS.contains(paramName);
    }

    /**
     * Create a client metrics config instance using the given properties and defaults.
     */
    public static ClientMetricsConfigs fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new ClientMetricsConfigs(props);
    }
}
