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

import java.util.List;
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
    public void testTopicPropertiesExclude() {
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
    public void testTopicPropertiesExcludeWithRegex() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPIC_PROPERTIES_EXCLUDE_CONFIG, "min.*,max.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicPropertiesExcludePattern().matcher("min.insync.replicas").matches());
        assertTrue(config.topicPropertiesExcludePattern().matcher("max.message.bytes").matches());
        assertFalse(config.topicPropertiesExcludePattern().matcher("cleanup.policy").matches());
    }

    @Test
    public void testTopicsInclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test.*,prod.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsIncludePattern().matcher("test-topic").matches());
        assertTrue(config.topicsIncludePattern().matcher("prod-topic").matches());
        assertFalse(config.topicsIncludePattern().matcher("staging").matches());
    }

    @Test
    public void testTopicsIncludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsIncludePattern().matcher("any-topic").matches());
        assertTrue(config.topicsIncludePattern().matcher("test").matches());
    }

    @Test
    public void testTopicsIncludeWithEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.topicsIncludePattern());
    }

    @Test
    public void testTopicsIncludePatternWithWhitespaces() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "   ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.topicsIncludePattern());
    }

    @Test
    public void testTopicsIncludeWihInvalidRegex() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "test[invalid");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid regex pattern"));
    }

    @Test
    public void testTopicsExclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "__.*,internal-.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsExcludePattern().matcher("__consumer_offsets").matches());
        assertTrue(config.topicsExcludePattern().matcher("internal-topic").matches());
        assertFalse(config.topicsExcludePattern().matcher("user-topic").matches());
    }

    @Test
    public void testTopicsExcludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.topicsExcludePattern().matcher("any-topic").matches());
        assertTrue(config.topicsExcludePattern().matcher("test").matches());
    }

    @Test
    public void testTopicsExcludeWithEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.topicsExcludePattern());
    }

    @Test
    public void testTopicsExcludeWithWhitespaces() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "   ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.topicsExcludePattern());
    }

    @Test
    public void testGroupsInclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, "app.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsIncludePattern().matcher("app-group-1").matches());
        assertFalse(config.groupsIncludePattern().matcher("test-group").matches());
    }

    @Test
    public void testGroupsIncludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsIncludePattern().matcher("any-group").matches());
        assertTrue(config.groupsIncludePattern().matcher("test").matches());
    }

    @Test
    public void testGroupsIncludeWithEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, "");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.groupsIncludePattern());
    }

    @Test
    public void testGroupsIncludeWithWhitespaces() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, "   ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.groupsIncludePattern());
    }

    @Test
    public void testGroupsExclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, "console-.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsExcludePattern().matcher("console-consumer").matches());
        assertFalse(config.groupsExcludePattern().matcher("my-group").matches());
    }

    @Test
    public void testGroupsExcludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertTrue(config.groupsExcludePattern().matcher("any-group").matches());
        assertTrue(config.groupsExcludePattern().matcher("test").matches());
    }

    @Test
    public void testGroupsExcludeWithEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, "");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.groupsExcludePattern());
    }

    @Test
    public void testGroupsExcludeWithWhitespaces() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, "   ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertNull(config.groupsExcludePattern());
    }

    @Test
    public void testAclsInclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
    }

    @Test
    public void testAclsIncludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ClusterMirrorConfig.MirrorAclRule.MATCH_ALL, rule);
    }

    @Test
    public void testAclsIncludeWithMultipleFields() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;ALLOW");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.READ, rule.operation());
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
    }

    @Test
    public void testAclsIncludeWithExtraFields() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG,
                "TOPIC;.*;.*;READ;ALLOW;User:alice;extrafield;moredata");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals("User:alice;extrafield;moredata", rule.principal());
        // Extra semicolons after the principal are preserved as part of the principal (split limit=6)
        assertTrue(rule.principal().contains(";extrafield;moredata"));
    }

    @Test
    public void testAclsIncludeWithPrincipal() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals("User:alice", rule.principal());
    }

    @Test
    public void testAclsIncludeWithWildcardPrincipal() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;ALLOW;User:*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        // User:* is a wildcard principal string
        assertEquals("User:*", rule.principal());
    }

    @Test
    public void testAclsIncludeWithPrincipalDN() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG,
                "TOPIC;.*;.*;READ;ALLOW;User:CN=billing-service,OU=Finance,O=Enterprise,L=London,C=GB");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.READ, rule.operation());
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
        assertEquals("User:CN=billing-service,OU=Finance,O=Enterprise,L=London,C=GB", rule.principal());
    }

    @Test
    public void testAclsIncludeWithCommaContainingPrincipal() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG,
                "TOPIC;.*;.*;READ;ALLOW;User:CN=billing-service,OU=Finance,O=Enterprise,L=London,C=GB||TOPIC;.*;.*;WRITE;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(2, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule1 = config.aclIncludeRules().get(0);
        assertEquals("User:CN=billing-service,OU=Finance,O=Enterprise,L=London,C=GB", rule1.principal());
        assertEquals(AclOperation.READ, rule1.operation());
        ClusterMirrorConfig.MirrorAclRule rule2 = config.aclIncludeRules().get(1);
        assertEquals("User:alice", rule2.principal());
        assertEquals(AclOperation.WRITE, rule2.operation());
    }

    @Test
    public void testAclsIncludeWithPipeContainingPrincipal() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;ALLOW;User:alice|bob");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals("User:alice|bob", rule.principal());
    }

    @Test
    public void testAclsIncludeIncludingEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, "TOPIC;.*;.*;WRITE||||CLUSTER;.*;.*;ALL;DENY");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(2, config.aclExcludeRules().size());
    }

    @Test
    public void testAclsIncludeWithEmpty() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(0, config.aclIncludeRules().size());
    }

    @Test
    public void testAclsIncludeWithWhitespaces() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "   ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(0, config.aclIncludeRules().size());
    }

    @Test
    public void testAclsIncludeWithInvalidOperation() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;INVALID_OP;ALLOW");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithInvalidResource() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "INVALID_TYPE;.*;.*;READ");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithInvalidPermission() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;INVALID_PERMISSION");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithWhitespaceInFields() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "  TOPIC  ;  .*  ;  .*  ;  READ  ;  ALLOW  ;  User:alice  ");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.READ, rule.operation());
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
        assertEquals("User:alice", rule.principal());
    }

    @Test
    public void testAclsIncludeWithAnyResource() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "ANY;.*;.*;READ;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertNull(rule.resourceType());
    }

    @Test
    public void testAclsIncludeWithAnyOperation() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;ANY;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertNull(rule.operation());
    }

    @Test
    public void testAclsIncludeWithAnyPermission() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;ANY;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertNull(rule.permissionType());
    }

    @Test
    public void testAclsIncludeWithUnknownResource() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "UNKNOWN;.*;.*;READ;ALLOW");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithUnknownOperation() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;UNKNOWN;ALLOW");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithUnknownPermission() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;UNKNOWN");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsIncludeWithLowercaseResourceType() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "topic;.*;.*;read;allow");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
    }

    @Test
    public void testAclsIncludeWithLowercaseOperation() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;read;allow");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(AclOperation.READ, rule.operation());
    }

    @Test
    public void testAclsIncludeWithLowercasePermissionType() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;READ;allow");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
    }

    @Test
    public void testAclsIncludeWithLowercaseAny() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;any;allow");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertNull(rule.operation());
    }

    @Test
    public void testAclsIncludeWithLowercaseUnknown() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;.*;unknown;allow");

        ConfigException ex = assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains("Invalid ACL rule"));
    }

    @Test
    public void testAclsExclude() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, "TOPIC;.*;.*;WRITE");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclExcludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclExcludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertEquals(AclOperation.WRITE, rule.operation());
    }

    @Test
    public void testAclsExcludeAll() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG, ".*");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclExcludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclExcludeRules().get(0);
        assertEquals(ClusterMirrorConfig.MirrorAclRule.MATCH_ALL, rule);
    }

    @Test
    public void testAclRuleWithHost() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;test-.*;192\\.168\\..*;READ;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertNotNull(rule.hostPattern());
        assertTrue(rule.hostPattern().matcher("192.168.1.1").matches());
        assertFalse(rule.hostPattern().matcher("10.0.0.1").matches());
    }

    @Test
    public void testAclRuleWithEmptyHost() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, "TOPIC;.*;;READ;ALLOW;User:alice");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(1, config.aclIncludeRules().size());
        ClusterMirrorConfig.MirrorAclRule rule = config.aclIncludeRules().get(0);
        assertEquals(ResourceType.TOPIC, rule.resourceType());
        assertNotNull(rule.resourceNamePattern());
        assertTrue(rule.resourceNamePattern().matcher("any-topic").matches());
        assertNull(rule.hostPattern());
        assertEquals(AclOperation.READ, rule.operation());
        assertEquals(AclPermissionType.ALLOW, rule.permissionType());
        assertEquals("User:alice", rule.principal());
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
    public void testAclRuleHostPatternMatching() {
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "test-topic", PatternType.LITERAL),
                new AccessControlEntry(
                        "User:alice",
                        "192.168.1.100",
                        AclOperation.READ,
                        AclPermissionType.ALLOW
                )
        );

        ClusterMirrorConfig.MirrorAclRule ruleMatch = ClusterMirrorConfig.MirrorAclRule.parse("TOPIC;.*;192\\.168\\..*;READ;ALLOW;User:alice");
        assertTrue(ruleMatch.matches(binding));

        ClusterMirrorConfig.MirrorAclRule ruleNoMatch = ClusterMirrorConfig.MirrorAclRule.parse("TOPIC;.*;10\\..*;READ;ALLOW;User:alice");
        assertFalse(ruleNoMatch.matches(binding));
    }

    @Test
    public void testAclsDocumentationExamples() {
        // match all ACLs
        ClusterMirrorConfig.MirrorAclRule matchAllRule = ClusterMirrorConfig.MirrorAclRule.parse(".*");
        assertEquals(ClusterMirrorConfig.MirrorAclRule.MATCH_ALL, matchAllRule);

        // match all ACLs for topics matching orders.*
        ClusterMirrorConfig.MirrorAclRule topicOrdersRule = ClusterMirrorConfig.MirrorAclRule.parse("TOPIC;orders.*");
        assertEquals(ResourceType.TOPIC, topicOrdersRule.resourceType());
        assertNotNull(topicOrdersRule.resourceNamePattern());
        assertTrue(topicOrdersRule.resourceNamePattern().matcher("orders-1").matches());
        assertFalse(topicOrdersRule.resourceNamePattern().matcher("payments-1").matches());

        // match all topic READ and WRITE ALLOW ACLs
        List<ClusterMirrorConfig.MirrorAclRule> topicAllowRules = ClusterMirrorConfig
                .MirrorAclRule.parseList("TOPIC;.*;.*;READ;ALLOW||TOPIC;.*;.*;WRITE;ALLOW");
        assertEquals(2, topicAllowRules.size());
        ClusterMirrorConfig.MirrorAclRule readAllowRule = topicAllowRules.get(0);
        assertEquals(ResourceType.TOPIC, readAllowRule.resourceType());
        assertEquals(AclOperation.READ, readAllowRule.operation());
        assertEquals(AclPermissionType.ALLOW, readAllowRule.permissionType());
        ClusterMirrorConfig.MirrorAclRule writeAllowRule = topicAllowRules.get(1);
        assertEquals(ResourceType.TOPIC, writeAllowRule.resourceType());
        assertEquals(AclOperation.WRITE, writeAllowRule.operation());
        assertEquals(AclPermissionType.ALLOW, writeAllowRule.permissionType());

        // match all ACLs for principal User:alice
        ClusterMirrorConfig.MirrorAclRule aliceRule = ClusterMirrorConfig.MirrorAclRule.parse("ANY;.*;.*;ANY;ANY;User:alice");
        assertNull(aliceRule.resourceType());
        assertNull(aliceRule.operation());
        assertNull(aliceRule.permissionType());
        assertEquals("User:alice", aliceRule.principal());

        // match all WRITE ACLs
        ClusterMirrorConfig.MirrorAclRule writeRule = ClusterMirrorConfig.MirrorAclRule.parse("ANY;.*;.*;WRITE;ANY;.*");
        assertNull(writeRule.resourceType());
        assertEquals(AclOperation.WRITE, writeRule.operation());
        assertNull(writeRule.permissionType());
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
    public void testSocketReceiveBufferBytesValidPositive() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, "131072");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(131072, config.socketReceiveBufferBytes());
    }

    @Test
    public void testSocketReceiveBufferBytesValidNegativeOne() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, "-1");

        ClusterMirrorConfig config = ClusterMirrorConfig.fromProperties(props);

        assertEquals(-1, config.socketReceiveBufferBytes());
    }

    @Test
    public void testSocketReceiveBufferBytesInvalidZero() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, "0");

        assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
    }

    @Test
    public void testSocketReceiveBufferBytesInvalidNegative() {
        Properties props = new Properties();
        props.setProperty(ClusterMirrorConfig.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, "-100");

        assertThrows(ConfigException.class, () -> ClusterMirrorConfig.fromProperties(props));
    }
}
