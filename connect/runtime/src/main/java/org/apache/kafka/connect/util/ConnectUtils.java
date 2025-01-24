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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

public final class ConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(ConnectUtils.class);

    public static Long checkAndConvertTimestamp(Long timestamp) {
        if (timestamp == null || timestamp >= 0)
            return timestamp;
        else if (timestamp == RecordBatch.NO_TIMESTAMP)
            return null;
        else
            throw new InvalidRecordException(String.format("Invalid record timestamp %d", timestamp));
    }

    /**
     * Ensure that the {@link Map properties} contain an expected value for the given key, inserting the
     * expected value into the properties if necessary.
     *
     * <p>If there is a pre-existing value for the key in the properties, log a warning to the user
     * that this value will be ignored, and the expected value will be used instead.
     *
     * @param props the configuration properties provided by the user; may not be null
     * @param key the name of the property to check on; may not be null
     * @param expectedValue the expected value for the property; may not be null
     * @param justification the reason the property cannot be overridden.
     *                      Will follow the phrase "The value... for the... property will be ignored as it cannot be overridden ".
     *                      For example, one might supply the message "in connectors with the DLQ feature enabled" for this parameter.
     *                      May be null (in which case, no justification is given to the user in the logged warning message)
     * @param caseSensitive whether the value should match case-insensitively
     */
    public static void ensureProperty(
            Map<String, ? super String> props,
            String key,
            String expectedValue,
            String justification,
            boolean caseSensitive
    ) {
        ensurePropertyAndGetWarning(props, key, expectedValue, justification, caseSensitive).ifPresent(log::warn);
    }

    // Visible for testing
    /**
     * Ensure that a given key has an expected value in the properties, inserting the expected value into the
     * properties if necessary. If a user-supplied value is overridden, return a warning message that can
     * be logged to the user notifying them of this fact.
     *
     * @return an {@link Optional} containing a warning that should be logged to the user if a value they
     * supplied in the properties is being overridden, or {@link Optional#empty()} if no such override has
     * taken place
     */
    static Optional<String> ensurePropertyAndGetWarning(
            Map<String, ? super String> props,
            String key,
            String expectedValue,
            String justification,
            boolean caseSensitive) {
        if (!props.containsKey(key)) {
            // Insert the expected value
            props.put(key, expectedValue);
            // But don't issue a warning to the user
            return Optional.empty();
        }

        String value = Objects.toString(props.get(key));
        boolean matchesExpectedValue = caseSensitive ? expectedValue.equals(value) : expectedValue.equalsIgnoreCase(value);
        if (matchesExpectedValue) {
            return Optional.empty();
        }

        // Insert the expected value
        props.put(key, expectedValue);

        justification = justification != null ? " " + justification : "";
        // And issue a warning to the user
        return Optional.of(String.format(
                "The value '%s' for the '%s' property will be ignored as it cannot be overridden%s. "
                        + "The value '%s' will be used instead.",
                value, key, justification, expectedValue
        ));
    }

    /**
     * Adds Connect metrics context properties.
     * @param prop the properties map to which the metrics context properties are to be added
     * @param config the worker config
     * @param clusterId the Connect cluster's backing Kafka cluster ID
     *
     * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-606%3A+Add+Metadata+Context+to+MetricsReporter">KIP-606</a>
     */
    public static void addMetricsContextProperties(Map<String, Object> prop, WorkerConfig config, String clusterId) {
        //add all properties predefined with "metrics.context."
        prop.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX, false));
        //add connect properties
        prop.put(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_KAFKA_CLUSTER_ID, clusterId);
        Object groupId = config.originals().get(DistributedConfig.GROUP_ID_CONFIG);
        if (groupId != null) {
            prop.put(CommonClientConfigs.METRICS_CONTEXT_PREFIX + WorkerConfig.CONNECT_GROUP_ID, groupId);
        }
    }

    public static boolean isSinkConnector(Connector connector) {
        return SinkConnector.class.isAssignableFrom(connector.getClass());
    }

    public static boolean isSourceConnector(Connector connector) {
        return SourceConnector.class.isAssignableFrom(connector.getClass());
    }

    /**
     * Apply a specified transformation {@link Function} to every value in a Map.
     * @param map the Map to be transformed
     * @param transformation the transformation function
     * @return the transformed Map
     * @param <K> the key type
     * @param <I> the pre-transform value type
     * @param <O> the post-transform value type
     */
    public static <K, I, O> Map<K, O> transformValues(Map<K, I> map, Function<I, O> transformation) {
        return map.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                transformation.compose(Map.Entry::getValue)
        ));
    }

    public static <I> List<I> combineCollections(Collection<Collection<I>> collections) {
        return combineCollections(collections, Function.identity());
    }

    public static <I, T> List<T> combineCollections(Collection<I> collection, Function<I, Collection<T>> extractCollection) {
        return combineCollections(collection, extractCollection, Collectors.toList());
    }

    public static <I, T, C> C combineCollections(
            Collection<I> collection,
            Function<I, Collection<T>> extractCollection,
            Collector<T, ?, C> collector
    ) {
        return collection.stream()
                .map(extractCollection)
                .flatMap(Collection::stream)
                .collect(collector);
    }

    public static ConnectException maybeWrap(Throwable t, String message) {
        if (t == null) {
            return null;
        }
        if (t instanceof ConnectException) {
            return (ConnectException) t;
        }
        return new ConnectException(message, t);
    }

    /**
     * Create the base of a {@link CommonClientConfigs#CLIENT_ID_DOC client ID} that can be
     * used for Kafka clients instantiated by this worker. Workers should append an extra identifier
     * to the end of this base ID to include extra information on what they are using it for; for example,
     * {@code clientIdBase(config) + "configs"} could be used as the client ID for a consumer, producer,
     * or admin client used to interact with a worker's config topic.
     * @param config the worker config; may not be null
     * @return the base client ID for this worker; never null, never empty, and will always end in a
     * hyphen ('-')
     */
    public static String clientIdBase(WorkerConfig config) {
        String result = Optional.ofNullable(config.groupId())
                .orElse("connect");
        String userSpecifiedClientId = config.getString(CLIENT_ID_CONFIG);
        if (userSpecifiedClientId != null && !userSpecifiedClientId.trim().isEmpty()) {
            result += "-" + userSpecifiedClientId;
        }
        return result + "-";
    }

    /**
     * Get the class name for an object in a null-safe manner.
     * @param o the object whose class name is to be returned
     * @return "null" if the object is null; or else the object's class name
     */
    public static String className(Object o) {
        return o != null ? o.getClass().getName() : "null";
    }

    /**
     * Apply a patch on a connector config.
     *
     * <p>In the output, the values from the patch will override the values from the config.
     * {@code null} values will cause the corresponding key to be removed completely.
     * @param config the config to be patched.
     * @param patch the patch.
     * @return the output config map.
     */
    public static Map<String, String> patchConfig(
            Map<String, String> config,
            Map<String, String> patch
    ) {
        Map<String, String> result = new HashMap<>(config);
        patch.forEach((k, v) -> {
            if (v != null) {
                result.put(k, v);
            } else {
                result.remove(k);
            }
        });
        return result;
    }
}
