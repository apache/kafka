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

package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.fail;

@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = "create.topic.policy.class.name", value = "kafka.server.TooManyPartitionsIntegrationTest$TopicPolicy"),
    }
)
public class TooManyPartitionsIntegrationTest {
    @ClusterTest
    public void testCreateTooMany(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.waitForReadyBrokers();
        try (Admin admin = clusterInstance.admin()) {
            // policy kicks in
            try {
                admin.createTopics(Collections.singleton(new NewTopic("topic1K", 1_000, (short) 3))).all().get();
                fail("ExecutionException expected");
            } catch (ExecutionException e) {
                Assertions.assertInstanceOf(PolicyViolationException.class, e.getCause());
                Assertions.assertTrue(e.getCause().getMessage().contains("> 999"), e.getCause().getMessage());
            }

            // protection kicks in
            try {
                admin.createTopics(Collections.singleton(new NewTopic("topic1G", 1_000_000_000, (short) 3))).all().get();
                fail("ExecutionException expected");
            } catch (ExecutionException e) {
                Assertions.assertInstanceOf(PolicyViolationException.class, e.getCause());
                Assertions.assertTrue(e.getCause().getMessage().contains("Excessively large"), e.getCause().getMessage());
            }
        }
    }

    @ClusterTest
    public void testExtendTooMany(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.waitForReadyBrokers();
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(Collections.singleton(new NewTopic("topic", 1, (short) 3))).all().get();

            // protection kicks in
            try {
                Map<String, NewPartitions> newPartitions = new HashMap<>();
                newPartitions.put("topic", NewPartitions.increaseTo(1_000_000_000));
                admin.createPartitions(newPartitions).all().get();
                fail("ExecutionException expected");
            } catch (ExecutionException e) {
                Assertions.assertInstanceOf(PolicyViolationException.class, e.getCause());
                Assertions.assertTrue(e.getCause().getMessage().contains("Excessively large"), e.getCause().getMessage());
            }
        }
    }

    public static class TopicPolicy implements CreateTopicPolicy {
        @Override
        public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
            if (requestMetadata.numPartitions() > 999) {
                throw new PolicyViolationException("Too many partitions: " + requestMetadata.numPartitions() + " > 999");
            }
        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public void configure(Map<String, ?> map) {
        }
    }
}

