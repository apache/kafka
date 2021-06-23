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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

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
     * Log a warning when the user attempts to override a property that cannot be overridden.
     * @param props the configuration properties provided by the user
     * @param key the name of the property to check on
     * @param expectedValue the expected value for the property
     * @param justification the reason the property cannot be overridden.
     *                      Will follow the phrase "The value... for the... property will be ignored as it cannot be overridden ".
     *                      For example, one might supply the message "in connectors with the DLQ feature enabled" for this parameter.
     * @param caseSensitive whether the value should match case-insensitively
     */
    public static void warnOnOverriddenProperty(
            Map<String, ?> props,
            String key,
            String expectedValue,
            String justification,
            boolean caseSensitive) {
        overriddenPropertyWarning(props, key, expectedValue, justification, caseSensitive).ifPresent(log::warn);
    }

    // Visible for testing
    static Optional<String> overriddenPropertyWarning(
            Map<String, ?> props,
            String key,
            String expectedValue,
            String justification,
            boolean caseSensitive) {
        Predicate<String> matchesExpectedValue = caseSensitive ? expectedValue::equals : expectedValue::equalsIgnoreCase;
        String value = Optional.ofNullable(props.get(key)).map(Object::toString).orElse(null);
        if (value != null && !matchesExpectedValue.test(value)) {
            return Optional.of(String.format(
                    "The value '%s' for the '%s' property will be ignored as it cannot be overridden %s. "
                            + "The value '%s' will be used instead.",
                    value, key, justification, expectedValue
            ));
        } else {
            return Optional.empty();
        }
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
