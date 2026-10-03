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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaYammerMetricsIntegrationTest {
    

    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:ANONYMOUS"),
            @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
            @ClusterConfigProperty(key = REPLICA_SELECTOR_CLASS_CONFIG, value = "org.apache.kafka.common.replica.RackAwareReplicaSelector"),
            @ClusterConfigProperty(key = REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, value = "true"),
            @ClusterConfigProperty(key = REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager"),
            @ClusterConfigProperty(key = REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
        }
    )
    public void testAllKafkaYammerMetricsNamingIsValid(ClusterInstance cluster) throws InterruptedException {
        // do some work to ensure that the metrics are registered
        produceAndConsumeData(cluster);
        transactionProduceAndConsumeData(cluster);
        
        var metricsNamePrefix = Pattern.compile("^kafka\\.");
        var metrics = KafkaYammerMetrics.defaultRegistry().allMetrics();
        for (var metricName : metrics.keySet()) {
            // These metrics are not prefixed with `kafka.` more information can be found in KIP-1100
            if (metricName.getMBeanName().contains("org.apache.kafka.server:type=AssignmentsManager") ||
                    metricName.getMBeanName().contains("org.apache.kafka.storage.internals.log:type=RemoteStorageThreadPool")) {
                continue;
            }
            if (!metricsNamePrefix.matcher(metricName.getMBeanName()).find()) {
                fail("this metric name " + metricName + " is not prefixed with kafka.");
            }
        }
    }

    private static void transactionProduceAndConsumeData(ClusterInstance cluster) throws InterruptedException {
        Map<String, Object> transactionalProducerConfig = Map.of(
            ENABLE_IDEMPOTENCE_CONFIG, "true",
            ACKS_CONFIG, "all",
            TRANSACTIONAL_ID_CONFIG, "transactionId"
        );

        Map<String, Object> transactionalConsumerConfig = Map.of(
            ENABLE_IDEMPOTENCE_CONFIG, "true",
            ACKS_CONFIG, "all",
            TRANSACTIONAL_ID_CONFIG, "transactionId"
        );
        var topicName = "test-topic2";
        cluster.createTopic(topicName, 3, (short) 2);
        try (var producer = cluster.producer(transactionalProducerConfig);
             var consumer = cluster.consumer(transactionalConsumerConfig);
             var admin = cluster.admin()
        ) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topicName, new byte[1_000 * 100]));
            producer.commitTransaction();
            
            consumer.subscribe(List.of(topicName));
            consumer.poll(Duration.ofMillis(100L));
            admin.deleteTopics(List.of(topicName));
        }
    }

    private static void produceAndConsumeData(ClusterInstance cluster) throws InterruptedException {
        var topicName = "test-topic";
        cluster.createTopic(topicName, 1, (short) 1);
        try (var producer = cluster.producer();
             var consumer = cluster.consumer();
             var admin = cluster.admin()
        ) {
            producer.send(new ProducerRecord<>(topicName, 0, "key".getBytes(), "value".getBytes()));

            consumer.subscribe(List.of(topicName));
            consumer.poll(Duration.ofMillis(100L));
            admin.deleteTopics(List.of(topicName));
        }
    }
}
