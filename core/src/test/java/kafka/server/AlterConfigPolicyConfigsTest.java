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

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.server.config.ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(serverProperties = {
        @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, value = "kafka.server.AlterConfigPolicyConfigsTest$Policy"),
})
@ExtendWith(value = ClusterTestExtensions.class)
public class AlterConfigPolicyConfigsTest {

    @BeforeEach
    public void setUp() {
        Policy.lastConfig = null;
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigSubtract(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            clusterInstance.waitForReadyBrokers();

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "foo"),
                    AlterConfigOp.OpType.SUBTRACT);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigAppend(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            clusterInstance.waitForReadyBrokers();

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "foo,bar"),
                    AlterConfigOp.OpType.APPEND);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("foo,bar", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigSet(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            clusterInstance.waitForReadyBrokers();

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "foo"),
                    AlterConfigOp.OpType.SET);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("foo", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigDelete(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            clusterInstance.waitForReadyBrokers();

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "unused"),
                    AlterConfigOp.OpType.DELETE);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertTrue(Policy.lastConfig.containsKey(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
            assertNull(Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigSubtract(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            admin.createTopics(Collections.singleton(new NewTopic("topic1", 1, (short) 1))).all().get();
            clusterInstance.waitForTopic("topic1", 1);

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "foo"),
                    AlterConfigOp.OpType.SUBTRACT);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("delete", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigAppend(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            admin.createTopics(Collections.singleton(new NewTopic("topic1", 1, (short) 1))).all().get();
            clusterInstance.waitForTopic("topic1", 1);

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"),
                    AlterConfigOp.OpType.APPEND);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("delete,compact", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigSet(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            admin.createTopics(Collections.singleton(new NewTopic("topic1", 1, (short) 1))).all().get();
            clusterInstance.waitForTopic("topic1", 1);

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"),
                    AlterConfigOp.OpType.SET);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertEquals("compact", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        }
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigDelete(ClusterInstance clusterInstance) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            admin.createTopics(Collections.singleton(new NewTopic("topic1", 1, (short) 1))).all().get();
            clusterInstance.waitForTopic("topic1", 1);

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "unused"),
                    AlterConfigOp.OpType.DELETE);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            assertFalse(Policy.lastConfig.containsKey(TopicConfig.CLEANUP_POLICY_CONFIG));
        }
    }

    public static class Policy implements AlterConfigPolicy {
        public static Map<String, String> lastConfig;

        @Override
        public void validate(AlterConfigPolicy.RequestMetadata requestMetadata) throws PolicyViolationException {
            assertNull(lastConfig);
            lastConfig = requestMetadata.configs();
        }

        @Override
        public void close() throws Exception {}
        @Override
        public void configure(Map<String, ?> configs) {}
    }

}
