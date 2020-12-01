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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The result of {@link Admin#createTopics(Collection)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class CreateTopicsResult {
    final static int UNKNOWN = -1;

    private final Map<String, KafkaFuture<TopicMetadataAndConfig>> futures;

    protected CreateTopicsResult(Map<String, KafkaFuture<TopicMetadataAndConfig>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from topic names to futures, which can be used to check the status of individual
     * topic creations.
     */
    public Map<String, KafkaFuture<Void>> values() {
        return futures.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().thenApply(v -> (Void) null)));
    }

    /**
     * Return a future which succeeds if all the topic creations succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

    /**
     * Returns a future that provides topic configs for the topic when the request completes.
     * <p>
     * If broker version doesn't support replication factor in the response, throw
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * {@link org.apache.kafka.common.errors.TopicAuthorizationException} is thrown if user does not
     * have permission to describe topic configs.
     */
    public KafkaFuture<Config> config(String topic) {
        return futures.get(topic).thenApply(TopicMetadataAndConfig::config);
    }

    /**
     * Returns a future that provides topic ID for the topic when the request completes.
     * <p>
     * If broker version doesn't support replication factor in the response, throw
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * {@link org.apache.kafka.common.errors.TopicAuthorizationException} is thrown if user does not
     * have permission to describe topic configs.
     */
    public KafkaFuture<Uuid> topicId(String topic) {
        return futures.get(topic).thenApply(TopicMetadataAndConfig::topicId);
    }
    
    /**
     * Returns a future that provides number of partitions in the topic when the request completes.
     * <p>
     * If broker version doesn't support replication factor in the response, throw
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * {@link org.apache.kafka.common.errors.TopicAuthorizationException} is thrown if user does not
     * have permission to describe topic configs.
     */
    public KafkaFuture<Integer> numPartitions(String topic) {
        return futures.get(topic).thenApply(TopicMetadataAndConfig::numPartitions);
    }

    /**
     * Returns a future that provides replication factor for the topic when the request completes.
     * <p>
     * If broker version doesn't support replication factor in the response, throw
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
     * If broker returned an error for topic configs, throw appropriate exception. For example,
     * {@link org.apache.kafka.common.errors.TopicAuthorizationException} is thrown if user does not
     * have permission to describe topic configs.
     */
    public KafkaFuture<Integer> replicationFactor(String topic) {
        return futures.get(topic).thenApply(TopicMetadataAndConfig::replicationFactor);
    }

    public static class TopicMetadataAndConfig {
        private final ApiException exception;
        private final Uuid topicId;
        private final int numPartitions;
        private final int replicationFactor;
        private final Config config;

        TopicMetadataAndConfig(Uuid topicId, int numPartitions, int replicationFactor, Config config) {
            this.exception = null;
            this.topicId = topicId;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.config = config;
        }

        TopicMetadataAndConfig(ApiException exception) {
            this.exception = exception;
            this.topicId = Uuid.ZERO_UUID;
            this.numPartitions = UNKNOWN;
            this.replicationFactor = UNKNOWN;
            this.config = null;
        }
        
        public Uuid topicId() {
            ensureSuccess();
            return topicId;
        }

        public int numPartitions() {
            ensureSuccess();
            return numPartitions;
        }

        public int replicationFactor() {
            ensureSuccess();
            return replicationFactor;
        }

        public Config config() {
            ensureSuccess();
            return config;
        }

        private void ensureSuccess() {
            if (exception != null)
                throw exception;
        }
    }
}
