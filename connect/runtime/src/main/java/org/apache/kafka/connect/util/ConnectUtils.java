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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

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

    public static String lookupKafkaClusterId(WorkerConfig config) {
        log.info("Creating Kafka admin client");
        try (Admin adminClient = Admin.create(config.originals())) {
            return lookupKafkaClusterId(adminClient);
        }
    }

    static String lookupKafkaClusterId(Admin adminClient) {
        log.debug("Looking up Kafka cluster ID");
        try {
            KafkaFuture<String> clusterIdFuture = adminClient.describeCluster().clusterId();
            if (clusterIdFuture == null) {
                log.info("Kafka cluster version is too old to return cluster ID");
                return null;
            }
            log.debug("Fetching Kafka cluster ID");
            String kafkaClusterId = clusterIdFuture.get();
            log.info("Kafka cluster ID: {}", kafkaClusterId);
            return kafkaClusterId;
        } catch (InterruptedException e) {
            throw new ConnectException("Unexpectedly interrupted when looking up Kafka cluster info", e);
        } catch (ExecutionException e) {
            throw new ConnectException("Failed to connect to and describe Kafka cluster. "
                                       + "Check worker's broker connection and security properties.", e);
        }
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

}
