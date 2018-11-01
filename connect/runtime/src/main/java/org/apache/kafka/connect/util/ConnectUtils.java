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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

public final class ConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(ConnectUtils.class);

    protected static final String[] PRODUCER_CONFIG_PREFIXES = {
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
            ProducerConfig.PARTITIONER_CLASS_CONFIG
    };

    protected static final String[] CONSUMER_CONFIG_PREFIXES = {
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
            };

    protected static final String[] ADMIN_CONFIG_PREFIXES = {
            AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG
    };

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
        try (AdminClient adminClient = AdminClient.create(config.originals())) {
            return lookupKafkaClusterId(adminClient);
        }
    }

    static String lookupKafkaClusterId(AdminClient adminClient) {
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
     * Modify the supplied map of configurations to retain only those configuration name-value pairs that satisfy the supplied predicate.
     *
     * @param configs the map of configurations to be modified; may not be null
     * @param isValid a function that is used to determine which configuration properties to retain; may not be null
     * @return the supplied {@code configs} parameter, returned for convenience
     * @see ProducerConfig#isKnownConfig(String)
     * @see ConsumerConfig#isKnownConfig(String)
     * @see AdminClientConfig#isKnownConfig(String)
     */
    public static Map<String, Object> retainConfigs(Map<String, Object> configs, Predicate<String> isValid) {
        Iterator<Entry<String, Object>> entryIter = configs.entrySet().iterator();
        while (entryIter.hasNext()) {
            Map.Entry<String, Object> entry = entryIter.next();
            if (!isValid.test(entry.getKey())) {
                log.debug("Not retaining the '{}' config property when passing to a subcomponent", entry.getKey());
                entryIter.remove();
            }
        }
        return configs;
    }
}
