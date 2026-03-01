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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, value = "org.apache.kafka.clients.CreateTopicsRequestWithPolicyTest$Policy"),
    }
)
public class CreateTopicsRequestWithPolicyTest {

    public static class Policy implements CreateTopicPolicy {
        private Map<String, ?> configs;
        private boolean closed = false;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }

        @Override
        public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
            if (Topic.isInternal(requestMetadata.topic())) {
                // Do not verify internal topics
                return;
            }

            if (closed) {
                throw new IllegalStateException("Policy should not be closed");
            }
            if (configs == null || configs.isEmpty()) {
                throw new IllegalStateException("Configure should have been called with non empty configs");
            }

            if (requestMetadata.numPartitions() != null || requestMetadata.replicationFactor() != null) {
                assertNotNull(requestMetadata.numPartitions(), "numPartitions should not be null, but it is " + requestMetadata.numPartitions());
                assertNotNull(requestMetadata.replicationFactor(), "replicationFactor should not be null, but it is " + requestMetadata.replicationFactor());
                assertNull(requestMetadata.replicasAssignments(), "replicaAssignments should be null, but it is " + requestMetadata.replicasAssignments());

                if (requestMetadata.numPartitions() < 5) {
                    throw new PolicyViolationException("Topics should have at least 5 partitions, received " +
                            requestMetadata.numPartitions());
                }

                if (requestMetadata.numPartitions() > 10) {
                    String retentionMs = requestMetadata.configs().get(TopicConfig.RETENTION_MS_CONFIG);
                    if (retentionMs == null || Integer.parseInt(retentionMs) > 5000) {
                        throw new PolicyViolationException("RetentionMs should be less than 5000ms if partitions > 10");
                    }
                } else {
                    assertTrue(requestMetadata.configs().isEmpty(), "Topic configs should be empty, but it is " + requestMetadata.configs());
                }
            } else {
                assertNull(requestMetadata.numPartitions(), "numPartitions should be null, but it is " + requestMetadata.numPartitions());
                assertNull(requestMetadata.replicationFactor(), "replicationFactor should be null, but it is " + requestMetadata.replicationFactor());
                assertNotNull(requestMetadata.replicasAssignments(), "replicasAssignments should not be null, but it is " + requestMetadata.replicasAssignments());

                requestMetadata.replicasAssignments().forEach((partitionId, replicas) -> {
                    if (replicas.size() < 2) {
                        throw new PolicyViolationException("Topic partitions should have at least 2 replicas, received " +
                                replicas.size() + " for partition " + partitionId);
                    }
                });
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private void validateValidCreateTopicsRequests(NewTopic topic, Admin admin, boolean validateOnly) throws Exception {
        admin.createTopics(
            List.of(topic),
            new CreateTopicsOptions().validateOnly(validateOnly)
        ).all().get();
    }

    @ClusterTest
    public void testValidCreateTopicsRequests(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            cluster.createTopic("topic1", 5, (short) 1);

            validateValidCreateTopicsRequests(
                new NewTopic("topic2", 5, (short) 3),
                admin,
                true
            );

            validateValidCreateTopicsRequests(
                new NewTopic("topic3", 11, (short) 2)
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "4999")),
                admin,
                true
            );

            validateValidCreateTopicsRequests(
                new NewTopic("topic4", Map.of(
                    0, List.of(1, 0),
                    1, List.of(0, 1)
                )),
                admin,
                false
            );
        }
    }

    private void validateErrorCreateTopicsRequests(NewTopic topic, Admin admin, boolean validateOnly, Class<? extends Throwable> expectedExceptionClass, String expectedErrorMessage) {
        ExecutionException exception = assertThrows(ExecutionException.class, () ->
                admin.createTopics(List.of(topic), new CreateTopicsOptions().validateOnly(validateOnly)).all().get());
        assertEquals(
                expectedExceptionClass,
                exception.getCause().getClass(),
                "Expected " + expectedExceptionClass.getSimpleName() + ", but got " + exception.getCause().getClass().getSimpleName()
        );
        assertTrue(exception.getMessage().contains(expectedErrorMessage));
    }

    @ClusterTest
    public void testErrorCreateTopicsRequests(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            String existingTopic = "existing-topic";
            cluster.createTopic(existingTopic, 5, (short) 1);

            // Policy violations
            validateErrorCreateTopicsRequests(
                new NewTopic("policy-topic1", 4, (short) 1),
                admin,
                false,
                PolicyViolationException.class,
                "Topics should have at least 5 partitions, received 4"
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("policy-topic2", 4, (short) 3),
                admin,
                true,
                PolicyViolationException.class,
                "Topics should have at least 5 partitions, received 4"
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("policy-topic3", 11, (short) 2)
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "5001")),
                admin,
                true,
                PolicyViolationException.class,
                "RetentionMs should be less than 5000ms if partitions > 10"
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("policy-topic4", Map.of(
                    0, List.of(1),
                    1, List.of(0)
                )).configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "5001")),
                admin,
                true,
                PolicyViolationException.class,
                "Topic partitions should have at least 2 replicas, received 1 for partition 0"
            );

            // Check that basic errors still work
            validateErrorCreateTopicsRequests(
                new NewTopic(existingTopic, 5, (short) 1),
                admin,
                false,
                TopicExistsException.class,
                "Topic 'existing-topic' already exists."
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("error-replication", 10, (short) 4),
                admin,
                true,
                InvalidReplicationFactorException.class,
                "Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be "
                    + "reached because only 3 broker(s) are registered or some brokers have all their log directories cordoned."
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("error-replication2", 10, (short) -2),
                admin,
                true,
                InvalidReplicationFactorException.class,
                "Replication factor must be larger than 0, or -1 to use the default value."
            );

            validateErrorCreateTopicsRequests(
                new NewTopic("error-partitions", -2, (short) 1),
                admin,
                true,
                InvalidPartitionsException.class,
                "Number of partitions was set to an invalid non-positive value."
            );
        }
    }
}
