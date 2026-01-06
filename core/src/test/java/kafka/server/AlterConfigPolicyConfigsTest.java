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
import kafka.test.annotation.Type;
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
import static org.apache.kafka.server.config.ServerLogConfigs.ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG;
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

    @ClusterTest(
            types = {Type.ZK},
            serverProperties = {
                    @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG, value = "true"),
            })
    public void testPolicyAlterBrokerConfigSubtractCompatibityEnabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterBrokerConfigSubtract(clusterInstance, true);
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigSubtractCompatibityDisabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterBrokerConfigSubtract(clusterInstance, false);
    }

    public void testPolicyAlterBrokerConfigSubtract(ClusterInstance clusterInstance, boolean compatibilityMode) throws Exception {
        try (Admin admin = clusterInstance.createAdminClient()) {
            clusterInstance.waitForReadyBrokers();

            AlterConfigOp alterConfigOp = new AlterConfigOp(
                    new ConfigEntry(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "foo"),
                    AlterConfigOp.OpType.SUBTRACT);
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    Collections.singletonList(alterConfigOp));
            admin.incrementalAlterConfigs(alterConfigs).all().get();

            if (clusterInstance.isKRaftTest() || compatibilityMode) {
                assertEquals("", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
            } else {
                assertEquals("foo", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
            }
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

    @ClusterTest(
            types = {Type.ZK},
            serverProperties = {
                    @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG, value = "true"),
            })
    public void testPolicyAlterBrokerConfigDeleteCompatibityEnabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterBrokerConfigDelete(clusterInstance, true);
    }

    @ClusterTest
    public void testPolicyAlterBrokerConfigDeleteCompatibityDisabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterBrokerConfigDelete(clusterInstance, false);
    }

    public void testPolicyAlterBrokerConfigDelete(ClusterInstance clusterInstance, boolean compatibilityMode) throws Exception {
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

            if (clusterInstance.isKRaftTest() || compatibilityMode) {
                assertNull(Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
            } else {
                assertEquals("unused", Policy.lastConfig.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
            }
        }
    }

    @ClusterTest(
            types = {Type.ZK},
            serverProperties = {
                    @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG, value = "true"),
            })
    public void testPolicyAlterTopicConfigSubtractCompatibityEnabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigSubtract(clusterInstance, true);
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigSubtractCompatibityDisabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigSubtract(clusterInstance, false);
    }

    public void testPolicyAlterTopicConfigSubtract(ClusterInstance clusterInstance, boolean compatibilityMode) throws Exception {
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
            if (clusterInstance.isKRaftTest() || compatibilityMode) {
                assertEquals("delete", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
            } else {
                assertEquals("foo", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
            }
        }
    }

    @ClusterTest(
            types = {Type.ZK},
            serverProperties = {
                    @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG, value = "true"),
            })
    public void testPolicyAlterTopicConfigAppendCompatibityEnabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigAppend(clusterInstance, true);
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigAppendCompatibityDisabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigAppend(clusterInstance, false);
    }

    public void testPolicyAlterTopicConfigAppend(ClusterInstance clusterInstance, boolean compatibilityMode) throws Exception {
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
            if (clusterInstance.isKRaftTest() || compatibilityMode) {
                assertEquals("delete,compact", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
            } else {
                assertEquals("compact", Policy.lastConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG));
            }
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

    @ClusterTest(
            types = {Type.ZK},
            serverProperties = {
                    @ClusterConfigProperty(key = ALTER_CONFIG_POLICY_KRAFT_COMPATIBILITY_ENABLE_CONFIG, value = "true"),
            })
    public void testPolicyAlterTopicConfigDeleteCompatibityEnabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigDelete(clusterInstance, true);
    }

    @ClusterTest
    public void testPolicyAlterTopicConfigDeleteCompatibityDisabled(ClusterInstance clusterInstance) throws Exception {
        testPolicyAlterTopicConfigDelete(clusterInstance, false);
    }

    public void testPolicyAlterTopicConfigDelete(ClusterInstance clusterInstance, boolean compatibilityMode) throws Exception {
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
            if (clusterInstance.isKRaftTest() || compatibilityMode) {
                assertFalse(Policy.lastConfig.containsKey(TopicConfig.CLEANUP_POLICY_CONFIG));
            } else {
                assertTrue(Policy.lastConfig.containsKey(TopicConfig.CLEANUP_POLICY_CONFIG));
            }
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
