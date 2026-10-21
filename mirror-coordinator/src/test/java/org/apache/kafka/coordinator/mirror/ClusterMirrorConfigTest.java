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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterMirrorConfigTest {
    @Test
    public void testFromPropertiesWithDefaults() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ClusterMirrorConfig.SOURCE_CLUSTER_ID_CONFIG, "source-cluster");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals("localhost:9092", config.originals().get(ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("source-cluster", config.originals().get(ClusterMirrorConfig.SOURCE_CLUSTER_ID_CONFIG));
        assertEquals(1000, config.fetchWaitMaxMs());
    }

    @Test
    public void testFromPropertiesWithCustomValues() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
        props.setProperty(ClusterMirrorConfig.FETCH_WAIT_MAX_MS_CONFIG, "5000");
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "topic1,topic2");
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "__.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(5000, config.fetchWaitMaxMs());
        assertNotNull(config.topicsIncludePattern());
        assertNotNull(config.topicsExcludePattern());
    }

    @Test
    public void testTopicPropertiesExcludePattern() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPIC_PROPERTIES_EXCLUDE_CONFIG,
                "leader.replication.throttled.replicas,follower.replication.throttled.replicas");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNotNull(config.topicPropertiesExcludePattern());
        assertTrue(config.topicPropertiesExcludePattern()
                .matcher("leader.replication.throttled.replicas").matches());
        assertFalse(config.topicPropertiesExcludePattern()
                .matcher("some.other.property").matches());
    }

    @Test
    public void testTopicIncludePatternMatching() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test.*,prod.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsIncludePattern().matcher("test-topic").matches());
        assertTrue(config.topicsIncludePattern().matcher("prod-topic").matches());
        assertFalse(config.topicsIncludePattern().matcher("staging").matches());
    }

    @Test
    public void testTopicExcludePatternMatching() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "__.*,internal-.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsExcludePattern().matcher("__consumer_offsets").matches());
        assertTrue(config.topicsExcludePattern().matcher("internal-topic").matches());
        assertFalse(config.topicsExcludePattern().matcher("user-topic").matches());
    }

    @Test
    public void testGroupIncludePatternMatching() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, "app.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsIncludePattern().matcher("app-group-1").matches());
        assertFalse(config.groupsIncludePattern().matcher("test-group").matches());
    }

    @Test
    public void testGroupExcludePatternMatching() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, "console-.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsExcludePattern().matcher("console-consumer").matches());
        assertFalse(config.groupsExcludePattern().matcher("my-group").matches());
    }

    @Test
    public void testParseAclRulesWildcard() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertNull(rule.resourceType());
        assertNull(rule.operation());
    }

    @Test
    public void testParseAclRulesWithResourceType() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
    }

    @Test
    public void testParseAclRulesWithMultipleParts() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;READ;ALLOW");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.READ, rule.operation());
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
    }

    @Test
    public void testParseAclRulesWithPrincipal() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;READ;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals("User:alice", rule.principal());
    }

    @Test
    public void testParseAclExcludeRules() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, "TOPIC;.*;WRITE");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclExcludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclExcludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.WRITE, rule.operation());
    }

    @Test
    public void testAclRuleMatching() {
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "test-topic", PatternType.LITERAL),
                new AccessControlEntry(
                        "User:alice",
                        "*",
                        AclOperation.READ,
                        AclPermissionType.ALLOW
                )
        );

        ClusterMirrorConfig.MirrorAclRule rule = ClusterMirrorConfig.MirrorAclRule.parse("TOPIC;.*");
        assertTrue(rule.matches(binding));
    }

    @Test
    public void testAclRuleNotMatching() {
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
                new AccessControlEntry(
                        "User:alice",
                        "*",
                        AclOperation.READ,
                        AclPermissionType.ALLOW
                )
        );

        ClusterMirrorConfig.MirrorAclRule rule = ClusterMirrorConfig.MirrorAclRule.parse("TOPIC;.*");
        assertFalse(rule.matches(binding));
    }

    @Test
    public void testIsValidConfigName() {
        assertTrue(ClusterMirrorConfig.isValidConfigName(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG));
        assertTrue(ClusterMirrorConfig.isValidConfigName(ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertFalse(ClusterMirrorConfig.isValidConfigName("invalid.config.name"));
    }

    @Test
    public void testConfigDefIsNotNull() {
        assertNotNull(ClusterMirrorConfig.configDef());
    }

    @Test
    public void testFetchConfiguration() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.FETCH_MAX_BYTES_CONFIG, "5242880");
        props.setProperty(ClusterMirrorConfig.FETCH_MIN_BYTES_CONFIG, "100");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(5242880, config.fetchMaxBytes());
        assertEquals(100, config.fetchMinBytes());
    }

    @Test
    public void testSecurityConfiguration() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore");
        props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals("SSL", config.securityProtocol());
    }

    @Test
    public void testSaslMechanismConfiguration() {
        Properties props = new Properties();
        props.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals("SCRAM-SHA-512", config.saslMechanism());
    }

    @Test
    public void testValidRegexPatterns() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test.*,prod.*");
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, "console-.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNotNull(config.topicsIncludePattern());
        assertNotNull(config.groupsExcludePattern());
    }

    @Test
    public void testInvalidRegexPattern() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test[invalid");

        assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
    }

    @Test
    public void testValidAclRules() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;READ;ALLOW");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
    }

    @Test
    public void testInvalidAclRuleOperation() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;INVALID_OP;ALLOW");

        assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
    }

    @Test
    public void testInvalidAclRuleResourceType() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, "INVALID_TYPE;.*;READ");

        assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
    }

    @Test
    public void testMultipleValidRegexPatterns() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test.*,prod.*,staging.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsIncludePattern().matcher("test-topic").matches());
        assertTrue(config.topicsIncludePattern().matcher("prod-topic").matches());
        assertTrue(config.topicsIncludePattern().matcher("staging-topic").matches());
        assertFalse(config.topicsIncludePattern().matcher("other-topic").matches());
    }

    @Test
    public void testMultipleValidAclRules() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;READ;ALLOW,CLUSTER;.*;ALL;DENY");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(2, config.aclIncludeRules().size());
    }

    @Test
    public void testMixedValidAndEmptyAclRules() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;READ;ALLOW, , CLUSTER;.*;ALL;ALLOW");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(2, config.aclIncludeRules().size());
    }
}
