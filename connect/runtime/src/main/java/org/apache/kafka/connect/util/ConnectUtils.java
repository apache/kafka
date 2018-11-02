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
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

public final class ConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(ConnectUtils.class);

    private static final List<String> WORKER_CONFIG_PREFIXES = Collections.unmodifiableList(Arrays.asList(
            "producer.",
            "consumer.",
            WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
            WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
            WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
            WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG,
            WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG,
            WorkerConfig.REST_EXTENSION_CLASSES_CONFIG,
            WorkerConfig.CONFIG_PROVIDERS_CONFIG
    ));

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
     * Modify the supplied map of configurations to retain only the configuration that may apply to the
     * {@link org.apache.kafka.clients.producer.Producer}, including extension components.
     * This will remove all properties that are known to be for consumers, admin client, and Connect workers.
     *
     * @param configs the map of configurations to be modified; may not be null
     * @return the supplied {@code configs} parameter, returned for convenience
     */
    public static Map<String, Object> retainProducerConfigs(Map<String, Object> configs) {
        return retainConfigs(configs, ConnectUtils::isProducerConfig);
    }

    /**
     * Modify the supplied map of configurations to retain only the configuration that may apply to the
     * {@link org.apache.kafka.clients.consumer.Consumer}, including extension components.
     * This will remove all properties that are known to be for producers, admin client, and Connect workers.
     *
     * @param configs the map of configurations to be modified; may not be null
     * @return the supplied {@code configs} parameter, returned for convenience
     */
    public static Map<String, Object> retainConsumerConfigs(Map<String, Object> configs) {
        return retainConfigs(configs, ConnectUtils::isConsumerConfig);
    }

    /**
     * Modify the supplied map of configurations to retain only the configuration that may apply to the {@link AdminClient},
     * including metric reporter configuration properties.
     * This will remove all properties that are known to be for producers, consumers, and Connect workers.
     *
     * @param configs the map of configurations to be modified; may not be null
     * @return the supplied {@code configs} parameter, returned for convenience
     */
    public static Map<String, Object> retainAdminClientConfigs(Map<String, Object> configs) {
        return retainConfigs(configs, ConnectUtils::isAdminClientConfig);
    }

    /**
     * Modify the supplied map of configurations to retain only those configuration name-value pairs that satisfy the supplied predicate.
     *
     * @param configs the map of configurations to be modified; may not be null
     * @param isValid a function that is used to determine which configuration properties to retain; may not be null
     * @return the supplied {@code configs} parameter, returned for convenience
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

    /**
     * Determine if the configuration with the supplied name is a known Connect {@link WorkerConfig} property, including properties
     * that are prefixed and passed to various extension. Note that the {@link org.apache.kafka.connect.rest.ConnectRestExtension
     * REST Extension} uses <em>unprefixed</em> properties, so this method treats those as unknown.
     *
     * @param key the name of the configuration property
     * @return true if the named configuration property is known to be a property used by Connect workers, or false otherwise
     */
    protected static boolean isKnownWorkerConfig(String key) {
        if (key == null) {
            return false;
        }
        if (DistributedConfig.configNames().contains(key) || StandaloneConfig.configNames().contains(key)) {
            return true;
        }
        // Include any property that begins with a known prefix
        for (String prefix : WORKER_CONFIG_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        // REST Extension properties are not prefixed, so we don't know what they are
        return false;
    }

    /**
     * Determine if the configuration with the supplied name is a known {@link ProducerConfig} property. Producers use metric reporters,
     * interceptors, partitioners, and key and value serializers, to which the producer passes all of its (unprefixed) properties.
     * Therefore, this method will consider all property names unknown to the {@link ProducerConfig}, {@link ConsumerConfig},
     * {@link AdminClientConfig}, and {@link #isKnownWorkerConfig(String) known worker configs} to be valid producer properties.
     *
     * @param key the name of the configuration property
     * @return true if the named configuration property is known to be a producer property or known to not be a consumer, admin client,
     *         or worker configuration; or false otherwise
     */
    protected static boolean isProducerConfig(String key) {
        if (key == null) {
            return false;
        }
        if (ProducerConfig.configNames().contains(key)) {
            return true;
        }
        if (ConsumerConfig.configNames().contains(key)) {
            return false;
        }
        if (AdminClientConfig.configNames().contains(key)) {
            return false;
        }
        if (isKnownWorkerConfig(key)) {
            return false;
        }
        // Producers can use metrics reporters, interceptors, and other extensions that take non-prefixed properties,
        // so we have to include all non-prefixed properties
        return true;
    }

    /**
     * Determine if the configuration with the supplied name is a known {@link ConsumerConfig} property. Consumers use metric reporters,
     * interceptors, partitioners, and key and value deserializers, to which the consumer passes all of its (unprefixed) properties.
     * Therefore, this method will consider all property names unknown to the {@link ProducerConfig}, {@link ConsumerConfig},
     * {@link AdminClientConfig}, and {@link #isKnownWorkerConfig(String) known worker configs} to be valid consumer properties.
     *
     * @param key the name of the configuration property
     * @return true if the named configuration property is known to be a consumer property or known to not be a producer, admin client,
     *         or worker configuration; or false otherwise
     */
    protected static boolean isConsumerConfig(String key) {
        if (key == null) {
            return false;
        }
        if (ConsumerConfig.configNames().contains(key)) {
            return true;
        }
        if (ProducerConfig.configNames().contains(key)) {
            return false;
        }
        if (AdminClientConfig.configNames().contains(key)) {
            return false;
        }
        if (isKnownWorkerConfig(key)) {
            return false;
        }
        // Consumers can use metrics reporters, interceptors, and other extensions that take non-prefixed properties,
        // so we have to include all non-prefixed properties
        return true;
    }

    /**
     * Determine if the configuration with the supplied name is a known {@link AdminClientConfig} property. Admin clients use metric
     * reporters to which the admin client passes all of its (unprefixed) properties.
     * Therefore, this method will consider all property names unknown to the {@link AdminClientConfig}, {@link ConsumerConfig},
     * {@link ProducerConfig}, and {@link #isKnownWorkerConfig(String) known worker configs} to be valid admin client properties.
     *
     * @param key the name of the configuration property
     * @return true if the named configuration property is known to be a admin client property or known to not be a producer, consumer,
     *         or worker configuration; or false otherwise
     */
    protected static boolean isAdminClientConfig(String key) {
        if (key == null) {
            return false;
        }
        if (AdminClientConfig.configNames().contains(key)) {
            return true;
        }
        if (ProducerConfig.configNames().contains(key)) {
            return false;
        }
        if (ConsumerConfig.configNames().contains(key)) {
            return false;
        }
        if (isKnownWorkerConfig(key)) {
            return false;
        }
        // AdminClient can use metrics reporters that take non-prefixed properties, so we have to include all non-prefixed properties
        return true;
    }
}
