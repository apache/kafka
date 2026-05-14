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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.controller.MockAclMutator;
import org.apache.kafka.metadata.authorizer.AuthorizerTestServerInfo;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.util.ServerTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.CREATE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_TOKENS;
import static org.apache.kafka.common.acl.AclOperation.IDEMPOTENT_WRITE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.TWO_PHASE_COMMIT;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.MATCH;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST;
import static org.apache.kafka.security.authorizer.AclEntry.WILDCARD_PRINCIPAL_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizerTest {

    private final Endpoint plaintext = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9020);

    private final AccessControlEntry allowReadAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, READ, ALLOW);
    private final AccessControlEntry allowWriteAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, WRITE, ALLOW);
    private final AccessControlEntry denyReadAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, READ, DENY);

    private final ResourcePattern wildCardResource = new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL);
    private final ResourcePattern prefixedResource = new ResourcePattern(TOPIC, "foo", PREFIXED);
    private final ResourcePattern clusterResource = new ResourcePattern(CLUSTER, CLUSTER_NAME, LITERAL);
    private final KafkaPrincipal wildcardPrincipal = SecurityUtils.parseKafkaPrincipal(WILDCARD_PRINCIPAL_STRING);

    private final String username = "alice";
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
    private final List<Metrics> metricsInstances = new ArrayList<>();
    private final List<PluginMetricsImpl> pluginMetricsInstances = new ArrayList<>();

    private Authorizer authorizer;
    private RequestContext requestContext;
    private ResourcePattern resource;

    static class CustomPrincipal extends KafkaPrincipal {

        public CustomPrincipal(String principalType, String name) {
            super(principalType, name);
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        requestContext = newRequestContext(principal, InetAddress.getByName("192.168.0.1"));
        authorizer = createAuthorizer(configs());
        resource = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL);
    }

    @AfterEach
    public void tearDown() throws Exception {
        authorizer.close();
        for (PluginMetricsImpl pluginMetric : pluginMetricsInstances) {
            pluginMetric.close();
        }
        for (Metrics metrics : metricsInstances) {
            metrics.close();
        }
    }

    private Map<String, Object> configs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:superuser1; User:superuser2");
        configs.put(KRaftConfigs.NODE_ID_CONFIG, "0");
        return configs;
    }

    @Test
    public void testAuthorizeByResourceTypeMultipleAddAndRemove() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);
        AccessControlEntry denyRead = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry allowRead = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);
        RequestContext u1h1Context = newRequestContext(user1, host1);

        for (int i = 0; i < 10; i++) {
            assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                    "User1 from host1 should not have READ access to any topic when no ACL exists");

            addAcls(authorizer, Set.of(allowRead), resource1);
            assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                    "User1 from host1 now should have READ access to at least one topic");

            for (int j = 0; j < 10; j++) {
                addAcls(authorizer, Set.of(denyRead), resource1);
                assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                        "User1 from host1 now should not have READ access to any topic");

                removeAcls(authorizer, Set.of(denyRead), resource1);
                addAcls(authorizer, Set.of(allowRead), resource1);
                assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                        "User1 from host1 now should have READ access to at least one topic");
            }

            removeAcls(authorizer, Set.of(allowRead), resource1);
            assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                    "User1 from host1 now should not have READ access to any topic");
        }
    }

    @Test
    public void testAuthorizeByResourceTypeIsolationUnrelatedDenyWontDominateAllow() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);
        ResourcePattern resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL);
        ResourcePattern resource3 = new ResourcePattern(GROUP, "s", PREFIXED);

        AccessControlEntry acl1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry acl2 = new AccessControlEntry(user2.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry acl3 = new AccessControlEntry(user1.toString(), host2.getHostAddress(), WRITE, DENY);
        AccessControlEntry acl4 = new AccessControlEntry(user1.toString(), host2.getHostAddress(), READ, DENY);
        AccessControlEntry acl5 = new AccessControlEntry(user1.toString(), host2.getHostAddress(), READ, DENY);
        AccessControlEntry acl6 = new AccessControlEntry(user2.toString(), host2.getHostAddress(), READ, DENY);
        AccessControlEntry acl7 = new AccessControlEntry(user1.toString(), host2.getHostAddress(), READ, ALLOW);

        addAcls(authorizer, Set.of(acl1, acl2, acl3, acl6, acl7), resource1);
        addAcls(authorizer, Set.of(acl4), resource2);
        addAcls(authorizer, Set.of(acl5), resource3);

        RequestContext u1h1Context = newRequestContext(user1, host1);
        RequestContext u1h2Context = newRequestContext(user1, host2);

        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should not have READ access to any topic");
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 should not have READ access to any consumer group");
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TRANSACTIONAL_ID),
                "User1 from host1 should not have READ access to any transactional id");
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, CLUSTER),
                "User1 from host1 should not have READ access to the cluster");
        assertTrue(authorizeByResourceType(authorizer, u1h2Context, READ, TOPIC),
                "User1 from host2 should have READ access to at least one topic");
    }

    @Test
    public void testAuthorizeByResourceTypeDenyTakesPrecedence() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);

        RequestContext u1h1Context = newRequestContext(user1, host1);
        AccessControlEntry acl1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, ALLOW);
        AccessControlEntry acl2 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, DENY);

        addAcls(authorizer, Set.of(acl1), resource1);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, WRITE, TOPIC),
                "User1 from host1 should have WRITE access to at least one topic");

        addAcls(authorizer, Set.of(acl2), resource1);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, WRITE, TOPIC),
                "User1 from host1 should not have WRITE access to any topic");
    }

    @Test
    public void testAuthorizeByResourceTypePrefixedResourceDenyDominate() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern a = new ResourcePattern(GROUP, "a", PREFIXED);
        ResourcePattern ab = new ResourcePattern(GROUP, "ab", PREFIXED);
        ResourcePattern abc = new ResourcePattern(GROUP, "abc", PREFIXED);
        ResourcePattern abcd = new ResourcePattern(GROUP, "abcd", PREFIXED);
        ResourcePattern abcde = new ResourcePattern(GROUP, "abcde", PREFIXED);

        RequestContext u1h1Context = newRequestContext(user1, host1);
        AccessControlEntry allowAce = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);
        AccessControlEntry denyAce = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);

        addAcls(authorizer, Set.of(allowAce), abcde);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 should have READ access to at least one group");

        addAcls(authorizer, Set.of(denyAce), abcd);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 now should not have READ access to any group");

        addAcls(authorizer, Set.of(allowAce), abc);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 now should have READ access to any group");

        addAcls(authorizer, Set.of(denyAce), a);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 now should not have READ access to any group");

        addAcls(authorizer, Set.of(allowAce), ab);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, GROUP),
                "User1 from host1 still should not have READ access to any group");
    }

    @Test
    public void testAuthorizeByResourceTypeWildcardResourceDenyDominate() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern wildcard = new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL);
        ResourcePattern prefixed = new ResourcePattern(GROUP, "hello", PREFIXED);
        ResourcePattern literal = new ResourcePattern(GROUP, "aloha", LITERAL);

        RequestContext u1h1Context = newRequestContext(user1, host1);
        AccessControlEntry allowAce = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, ALLOW);
        AccessControlEntry denyAce = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, DENY);

        addAcls(authorizer, Set.of(allowAce), prefixed);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, WRITE, GROUP),
                "User1 from host1 should have WRITE access to at least one group");

        addAcls(authorizer, Set.of(denyAce), wildcard);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, WRITE, GROUP),
                "User1 from host1 now should not have WRITE access to any group");

        addAcls(authorizer, Set.of(allowAce), wildcard);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, WRITE, GROUP),
                "User1 from host1 still should not have WRITE access to any group");

        addAcls(authorizer, Set.of(allowAce), literal);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, WRITE, GROUP),
                "User1 from host1 still should not have WRITE access to any group");
    }

    @Test
    public void testAuthorizeByResourceTypeWithAllOperationAce() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);
        AccessControlEntry denyAll = new AccessControlEntry(user1.toString(), host1.getHostAddress(), ALL, DENY);
        AccessControlEntry allowAll = new AccessControlEntry(user1.toString(), host1.getHostAddress(), ALL, ALLOW);
        AccessControlEntry denyWrite = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, DENY);
        RequestContext u1h1Context = newRequestContext(user1, host1);

        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should not have READ access to any topic when no ACL exists");

        addAcls(authorizer, Set.of(denyWrite, allowAll), resource1);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 now should have READ access to at least one topic");

        addAcls(authorizer, Set.of(denyAll), resource1);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 now should not have READ access to any topic");
    }

    @Test
    public void testAuthorizeByResourceTypeWithAllHostAce() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");
        String allHost = WILDCARD_HOST;
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);
        ResourcePattern resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL);
        AccessControlEntry allowHost1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);
        AccessControlEntry denyHost1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry denyAllHost = new AccessControlEntry(user1.toString(), allHost, READ, DENY);
        AccessControlEntry allowAllHost = new AccessControlEntry(user1.toString(), allHost, READ, ALLOW);
        RequestContext u1h1Context = newRequestContext(user1, host1);
        RequestContext u1h2Context = newRequestContext(user1, host2);

        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should not have READ access to any topic when no ACL exists");

        addAcls(authorizer, Set.of(allowHost1), resource1);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should now have READ access to at least one topic");

        addAcls(authorizer, Set.of(denyAllHost), resource1);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 now shouldn't have READ access to any topic");

        addAcls(authorizer, Set.of(denyHost1), resource2);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 still should not have READ access to any topic");
        assertFalse(authorizeByResourceType(authorizer, u1h2Context, READ, TOPIC),
                "User1 from host2 should not have READ access to any topic");

        addAcls(authorizer, Set.of(allowAllHost), resource2);
        assertTrue(authorizeByResourceType(authorizer, u1h2Context, READ, TOPIC),
                "User1 from host2 should now have READ access to at least one topic");

        addAcls(authorizer, Set.of(denyAllHost), resource2);
        assertFalse(authorizeByResourceType(authorizer, u1h2Context, READ, TOPIC),
                "User1 from host2 now shouldn't have READ access to any topic");
    }

    @Test
    public void testAuthorizeByResourceTypeWithAllPrincipalAce() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2");
        String allUser = WILDCARD_PRINCIPAL_STRING;
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "sb1" + UUID.randomUUID(), LITERAL);
        ResourcePattern resource2 = new ResourcePattern(TOPIC, "sb2" + UUID.randomUUID(), LITERAL);
        AccessControlEntry allowUser1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);
        AccessControlEntry denyUser1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry denyAllUser = new AccessControlEntry(allUser, host1.getHostAddress(), READ, DENY);
        AccessControlEntry allowAllUser = new AccessControlEntry(allUser, host1.getHostAddress(), READ, ALLOW);
        RequestContext u1h1Context = newRequestContext(user1, host1);
        RequestContext u2h1Context = newRequestContext(user2, host1);

        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should not have READ access to any topic when no ACL exists");

        addAcls(authorizer, Set.of(allowUser1), resource1);
        assertTrue(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 should now have READ access to at least one topic");

        addAcls(authorizer, Set.of(denyAllUser), resource1);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 now shouldn't have READ access to any topic");

        addAcls(authorizer, Set.of(denyUser1), resource2);
        assertFalse(authorizeByResourceType(authorizer, u1h1Context, READ, TOPIC),
                "User1 from host1 still should not have READ access to any topic");
        assertFalse(authorizeByResourceType(authorizer, u2h1Context, READ, TOPIC),
                "User2 from host1 should not have READ access to any topic");

        addAcls(authorizer, Set.of(allowAllUser), resource2);
        assertTrue(authorizeByResourceType(authorizer, u2h1Context, READ, TOPIC),
                "User2 from host1 should now have READ access to at least one topic");

        addAcls(authorizer, Set.of(denyAllUser), resource2);
        assertFalse(authorizeByResourceType(authorizer, u2h1Context, READ, TOPIC),
                "User2 from host1 now shouldn't have READ access to any topic");
    }

    @Test
    public void testAuthorizeByResourceTypeSuperUserHasAccess() throws Exception {
        AccessControlEntry denyAllAce = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, DENY);
        String superUserName = "superuser1";
        KafkaPrincipal superUser1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, superUserName);
        InetAddress host1 = InetAddress.getByName("192.0.4.4");
        ResourcePattern allTopicsResource = new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL);
        ResourcePattern clusterResource = new ResourcePattern(CLUSTER, WILDCARD_RESOURCE, LITERAL);
        ResourcePattern groupResource = new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL);
        ResourcePattern transactionIdResource = new ResourcePattern(TRANSACTIONAL_ID, WILDCARD_RESOURCE, LITERAL);

        addAcls(authorizer, Set.of(denyAllAce), allTopicsResource);
        addAcls(authorizer, Set.of(denyAllAce), clusterResource);
        addAcls(authorizer, Set.of(denyAllAce), groupResource);
        addAcls(authorizer, Set.of(denyAllAce), transactionIdResource);

        RequestContext superUserContext = newRequestContext(superUser1, host1);

        assertTrue(authorizeByResourceType(authorizer, superUserContext, READ, TOPIC),
                "superuser always has access, no matter what acls.");
        assertTrue(authorizeByResourceType(authorizer, superUserContext, READ, CLUSTER),
                "superuser always has access, no matter what acls.");
        assertTrue(authorizeByResourceType(authorizer, superUserContext, READ, GROUP),
                "superuser always has access, no matter what acls.");
        assertTrue(authorizeByResourceType(authorizer, superUserContext, READ, TRANSACTIONAL_ID),
                "superuser always has access, no matter what acls.");
    }

    @Test
    public void testAuthorizeThrowsOnNonLiteralResource() {
        assertThrows(IllegalArgumentException.class, () -> authorize(authorizer, requestContext, READ,
                new ResourcePattern(TOPIC, "something", PREFIXED)));
    }

    @Test
    public void testAuthorizeWithEmptyResourceName() throws Exception {
        assertFalse(authorize(authorizer, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)));
        addAcls(authorizer, Set.of(allowReadAcl), new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL));
        assertTrue(authorize(authorizer, requestContext, READ, new ResourcePattern(GROUP, "", LITERAL)));
    }

    // Authorizing the empty resource is not supported because empty resource name is invalid.
    @Test
    public void testEmptyAclThrowsException() {
        assertThrows(ApiException.class,
                () -> addAcls(authorizer, Set.of(allowReadAcl), new ResourcePattern(GROUP, "", LITERAL)));
    }

    @Test
    public void testTopicAcl() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "rob");
        KafkaPrincipal user3 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "batman");
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");

        //user1 has READ access from host1 and host2.
        AccessControlEntry acl1 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);
        AccessControlEntry acl2 = new AccessControlEntry(user1.toString(), host2.getHostAddress(), READ, ALLOW);

        //user1 does not have  READ access from host1.
        AccessControlEntry acl3 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, DENY);

        //user1 has WRITE access from host1 only.
        AccessControlEntry acl4 = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, ALLOW);

        //user1 has DESCRIBE access from all hosts.
        AccessControlEntry acl5 = new AccessControlEntry(user1.toString(), WILDCARD_HOST, DESCRIBE, ALLOW);

        //user2 has READ access from all hosts.
        AccessControlEntry acl6 = new AccessControlEntry(user2.toString(), WILDCARD_HOST, READ, ALLOW);

        //user3 has WRITE access from all hosts.
        AccessControlEntry acl7 = new AccessControlEntry(user3.toString(), WILDCARD_HOST, WRITE, ALLOW);

        Set<AccessControlEntry> acls = Set.of(acl1, acl2, acl3, acl4, acl5, acl6, acl7);

        changeAclAndVerify(Set.of(), acls, Set.of());

        RequestContext host1Context = newRequestContext(user1, host1);
        RequestContext host2Context = newRequestContext(user1, host2);

        assertTrue(authorize(authorizer, host2Context, READ, resource), "User1 should have READ access from host2");
        assertFalse(authorize(authorizer, host1Context, READ, resource), "User1 should not have READ access from host1 due to denyAcl");
        assertTrue(authorize(authorizer, host1Context, WRITE, resource), "User1 should have WRITE access from host1");
        assertFalse(authorize(authorizer, host2Context, WRITE, resource), "User1 should not have WRITE access from host2 as no allow acl is defined");
        assertTrue(authorize(authorizer, host1Context, DESCRIBE, resource), "User1 should have DESCRIBE access from host1");
        assertTrue(authorize(authorizer, host2Context, DESCRIBE, resource), "User1 should have DESCRIBE access from host2");
        assertFalse(authorize(authorizer, host1Context, ALTER, resource), "User1 should not have edit access from host1");
        assertFalse(authorize(authorizer, host2Context, ALTER, resource), "User1 should not have edit access from host2");

        //test if user has READ or WRITE access they also get DESCRIBE access
        RequestContext user2Context = newRequestContext(user2, host1);
        RequestContext user3Context = newRequestContext(user3, host1);
        assertTrue(authorize(authorizer, user2Context, DESCRIBE, resource), "User2 should have DESCRIBE access from host1");
        assertTrue(authorize(authorizer, user3Context, DESCRIBE, resource), "User3 should have DESCRIBE access from host1");
        assertTrue(authorize(authorizer, user2Context, READ, resource), "User2 should have READ access from host1");
        assertTrue(authorize(authorizer, user3Context, WRITE, resource), "User3 should have WRITE access from host1");
    }

    /**
     * CustomPrincipals should be compared with their principal type and name
     */
    @Test
    public void testAllowAccessWithCustomPrincipal() throws Exception {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        CustomPrincipal customUserPrincipal = new CustomPrincipal(KafkaPrincipal.USER_TYPE, username);
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");

        // user has READ access from host2 but not from host1
        AccessControlEntry acl1 = new AccessControlEntry(user.toString(), host1.getHostAddress(), READ, DENY);
        AccessControlEntry acl2 = new AccessControlEntry(user.toString(), host2.getHostAddress(), READ, ALLOW);
        Set<AccessControlEntry> acls = Set.of(acl1, acl2);
        changeAclAndVerify(Set.of(), acls, Set.of());

        RequestContext host1Context = newRequestContext(customUserPrincipal, host1);
        RequestContext host2Context = newRequestContext(customUserPrincipal, host2);

        assertTrue(authorize(authorizer, host2Context, READ, resource), "User1 should have READ access from host2");
        assertFalse(authorize(authorizer, host1Context, READ, resource), "User1 should not have READ access from host1 due to denyAcl");
    }

    @Test
    public void testDenyTakesPrecedence() throws Exception {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        InetAddress host = InetAddress.getByName("192.168.2.1");
        RequestContext session = newRequestContext(user, host);

        AccessControlEntry allowAll = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, ALLOW);
        AccessControlEntry denyAcl = new AccessControlEntry(user.toString(), host.getHostAddress(), AclOperation.ALL, DENY);
        Set<AccessControlEntry> acls = Set.of(allowAll, denyAcl);

        changeAclAndVerify(Set.of(), acls, Set.of());

        assertFalse(authorize(authorizer, session, READ, resource), "deny should take precedence over allow.");
    }

    @Test
    public void testAllowAllAccess() throws Exception {
        AccessControlEntry allowAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, ALLOW);

        changeAclAndVerify(Set.of(), Set.of(allowAllAcl), Set.of());

        RequestContext context = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "random"), InetAddress.getByName("192.0.4.4"));
        assertTrue(authorize(authorizer, context, READ, resource), "allow all acl should allow access to all.");
    }

    @Test
    public void testSuperUserHasAccess() throws Exception {
        AccessControlEntry denyAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, DENY);

        changeAclAndVerify(Set.of(), Set.of(denyAllAcl), Set.of());

        RequestContext session1 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"));
        RequestContext session2 = newRequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superuser2"), InetAddress.getByName("192.0.4.4"));

        assertTrue(authorize(authorizer, session1, READ, resource), "superuser always has access, no matter what acls.");
        assertTrue(authorize(authorizer, session2, READ, resource), "superuser always has access, no matter what acls.");
    }

    /**
     * CustomPrincipals should be compared with their principal type and name
     */
    @Test
    public void testSuperUserWithCustomPrincipalHasAccess() throws Exception {
        AccessControlEntry denyAllAcl = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, AclOperation.ALL, DENY);
        changeAclAndVerify(Set.of(), Set.of(denyAllAcl), Set.of());

        RequestContext session = newRequestContext(new CustomPrincipal(KafkaPrincipal.USER_TYPE, "superuser1"), InetAddress.getByName("192.0.4.4"));

        assertTrue(authorize(authorizer, session, READ, resource), "superuser with custom principal always has access, no matter what acls.");
    }

    @Test
    public void testWildCardAcls() throws Exception {
        assertFalse(authorize(authorizer, requestContext, READ, resource), "when acls = [], authorizer should fail close.");

        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        InetAddress host1 = InetAddress.getByName("192.168.3.1");
        AccessControlEntry readAcl = new AccessControlEntry(user1.toString(), host1.getHostAddress(), READ, ALLOW);

        Set<AccessControlEntry> acls = changeAclAndVerify(Set.of(), Set.of(readAcl), Set.of(), wildCardResource);

        RequestContext host1Context = newRequestContext(user1, host1);
        assertTrue(authorize(authorizer, host1Context, READ, resource), "User1 should have READ access from host1");

        //allow WRITE to specific topic.
        AccessControlEntry writeAcl = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, ALLOW);
        changeAclAndVerify(Set.of(), Set.of(writeAcl), Set.of());

        //deny WRITE to wild card topic.
        AccessControlEntry denyWriteOnWildCardResourceAcl = new AccessControlEntry(user1.toString(), host1.getHostAddress(), WRITE, DENY);
        changeAclAndVerify(acls, Set.of(denyWriteOnWildCardResourceAcl), Set.of(), wildCardResource);

        assertFalse(authorize(authorizer, host1Context, WRITE, resource), "User1 should not have WRITE access from host1");
    }

    @Test
    public void testNoAclFound() {
        assertFalse(authorize(authorizer, requestContext, READ, resource), "when acls = [], authorizer should deny op.");
    }

    @Test
    public void testNoAclFoundOverride() throws IOException {
        Map<String, Object> cfg = configs();
        cfg.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        try (Authorizer testAuthorizer = createAuthorizer(cfg)) {
            assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
                    "when acls = null or [],  authorizer should allow op with allow.everyone = true.");
        }
    }

    @Test
    public void testAclConfigWithWhitespace() throws IOException {
        Map<String, Object> cfg = configs();
        cfg.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, " true");
        // replace all property values with leading & trailing whitespaces
        cfg.replaceAll((k, v) -> " " + v + " ");

        try (Authorizer testAuthorizer = createAuthorizer(cfg)) {
            assertTrue(authorize(testAuthorizer, requestContext, READ, resource),
                    "when acls = null or [],  authorizer should allow op with allow.everyone = true.");
        }
    }

    @Test
    public void testAclManagementAPIs() throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        String host1 = "host1";
        String host2 = "host2";

        AccessControlEntry acl1 = new AccessControlEntry(user1.toString(), host1, READ, ALLOW);
        AccessControlEntry acl2 = new AccessControlEntry(user1.toString(), host1, WRITE, ALLOW);
        AccessControlEntry acl3 = new AccessControlEntry(user2.toString(), host2, READ, ALLOW);
        AccessControlEntry acl4 = new AccessControlEntry(user2.toString(), host2, WRITE, ALLOW);

        Set<AccessControlEntry> acls = changeAclAndVerify(Set.of(), Set.of(acl1, acl2, acl3, acl4), Set.of());

        //test addAcl is additive
        AccessControlEntry acl5 = new AccessControlEntry(user2.toString(), WILDCARD_HOST, READ, ALLOW);
        acls = changeAclAndVerify(acls, Set.of(acl5), Set.of());

        //test get by principal name.
        TestUtils.waitForCondition(() -> Set.of(acl1, acl2).stream().map(acl -> new AclBinding(resource, acl)).collect(Collectors.toSet()).equals(getAcls(authorizer, user1)),
                "changes not propagated in timeout period");
        TestUtils.waitForCondition(() -> Set.of(acl3, acl4, acl5).stream().map(acl -> new AclBinding(resource, acl)).collect(Collectors.toSet()).equals(getAcls(authorizer, user2)),
                "changes not propagated in timeout period");

        Map<ResourcePattern, Set<AccessControlEntry>> resourceToAcls = Map.of(
                new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL), Set.of(new AccessControlEntry(user2.toString(), WILDCARD_HOST, READ, ALLOW)),
                new ResourcePattern(CLUSTER, WILDCARD_RESOURCE, LITERAL), Set.of(new AccessControlEntry(user2.toString(), host1, READ, ALLOW)),
                new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL), acls,
                new ResourcePattern(GROUP, "test-ConsumerGroup", LITERAL), acls
        );

        for (Map.Entry<ResourcePattern, Set<AccessControlEntry>> entry : resourceToAcls.entrySet()) {
            ResourcePattern key = entry.getKey();
            Set<AccessControlEntry> value = entry.getValue();
            changeAclAndVerify(Set.of(), value, Set.of(), key);
        }

        Set<AclBinding> expectedAcls = new HashSet<>();
        resourceToAcls.forEach((res, aces) ->
            aces.forEach(ace -> expectedAcls.add(new AclBinding(res, ace)))
        );
        acls.forEach(acl -> expectedAcls.add(new AclBinding(resource, acl)));
        TestUtils.waitForCondition(() -> expectedAcls.equals(getAcls(authorizer)), "changes not propagated in timeout period.");

        //test remove acl from existing acls.
        changeAclAndVerify(acls, Set.of(), Set.of(acl1, acl5));

        //test remove all acls for resource
        removeAcls(authorizer, Set.of(), resource);
        ServerTestUtils.waitAndVerifyAcls(Set.of(), authorizer, resource, AccessControlEntryFilter.ANY);

        acls = changeAclAndVerify(Set.of(), Set.of(acl1), Set.of());
        changeAclAndVerify(acls, Set.of(), acls);
    }

    @Test
    public void testLocalConcurrentModificationOfResourceAcls() throws Exception {
        ResourcePattern commonResource = new ResourcePattern(TOPIC, "test", LITERAL);

        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        AccessControlEntry acl1 = new AccessControlEntry(user1.toString(), WILDCARD_HOST, READ, ALLOW);

        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        AccessControlEntry acl2 = new AccessControlEntry(user2.toString(), WILDCARD_HOST, READ, DENY);

        addAcls(authorizer, Set.of(acl1), commonResource);
        addAcls(authorizer, Set.of(acl2), commonResource);

        ServerTestUtils.waitAndVerifyAcls(Set.of(acl1, acl2), authorizer, commonResource, AccessControlEntryFilter.ANY);
    }

    /**
     * Test ACL inheritance, as described in {@link org.apache.kafka.common.acl.AclOperation}
     */
    @Test
    public void testAclInheritance() throws Exception {
        testImplicationsOfAllow(AclOperation.ALL, Set.of(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
                CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS, TWO_PHASE_COMMIT));
        testImplicationsOfDeny(AclOperation.ALL, Set.of(READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
                CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, CREATE_TOKENS, DESCRIBE_TOKENS, TWO_PHASE_COMMIT));
        testImplicationsOfAllow(READ, Set.of(DESCRIBE));
        testImplicationsOfAllow(WRITE, Set.of(DESCRIBE));
        testImplicationsOfAllow(DELETE, Set.of(DESCRIBE));
        testImplicationsOfAllow(ALTER, Set.of(DESCRIBE));
        testImplicationsOfDeny(DESCRIBE, Set.of());
        testImplicationsOfAllow(ALTER_CONFIGS, Set.of(DESCRIBE_CONFIGS));
        testImplicationsOfDeny(DESCRIBE_CONFIGS, Set.of());
    }

    private void testImplicationsOfAllow(AclOperation parentOp, Set<AclOperation> allowedOps) throws Exception {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        InetAddress host = InetAddress.getByName("192.168.3.1");
        RequestContext hostContext = newRequestContext(user, host);
        AccessControlEntry acl = new AccessControlEntry(user.toString(), WILDCARD_HOST, parentOp, ALLOW);
        addAcls(authorizer, Set.of(acl), clusterResource);
        for (AclOperation op : AclOperation.values()) {
            if (invalidOp(op)) continue;
            boolean authorized = authorize(authorizer, hostContext, op, clusterResource);
            if (allowedOps.contains(op) || op == parentOp) {
                assertTrue(authorized, "ALLOW " + parentOp + " should imply ALLOW " + op);
            } else {
                assertFalse(authorized, "ALLOW " + parentOp + " should not imply ALLOW " + op);
            }
        }
        removeAcls(authorizer, Set.of(acl), clusterResource);
    }

    private void testImplicationsOfDeny(AclOperation parentOp, Set<AclOperation> deniedOps) throws Exception {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, username);
        InetAddress host1 = InetAddress.getByName("192.168.3.1");
        RequestContext host1Context = newRequestContext(user1, host1);
        Set<AccessControlEntry> acls = Set.of(new AccessControlEntry(user1.toString(), WILDCARD_HOST, parentOp, DENY),
                new AccessControlEntry(user1.toString(), WILDCARD_HOST, AclOperation.ALL, ALLOW));
        addAcls(authorizer, acls, clusterResource);
        for (AclOperation op : AclOperation.values()) {
            if (invalidOp(op)) continue;
            boolean authorized = authorize(authorizer, host1Context, op, clusterResource);
            if (deniedOps.contains(op) || op == parentOp) {
                assertFalse(authorized, "DENY " + parentOp + " should imply DENY " + op);
            } else {
                assertTrue(authorized, "DENY " + parentOp + " should not imply DENY " + op);
            }
        }
        removeAcls(authorizer, acls, clusterResource);
    }

    @Test
    public void testAccessAllowedIfAllowAclExistsOnWildcardResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl), wildCardResource);

        assertTrue(authorize(authorizer, requestContext, READ, resource));
    }

    @Test
    public void testDeleteAclOnWildcardResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), wildCardResource);

        removeAcls(authorizer, Set.of(allowReadAcl), wildCardResource);

        assertEquals(Set.of(allowWriteAcl), getAcls(authorizer, wildCardResource));
    }

    @Test
    public void testDeleteAllAclOnWildcardResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl), wildCardResource);

        removeAcls(authorizer, Set.of(), wildCardResource);

        assertEquals(Set.of(), getAcls(authorizer));
    }

    @Test
    public void testAccessAllowedIfAllowAclExistsOnPrefixedResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl), prefixedResource);

        assertTrue(authorize(authorizer, requestContext, READ, resource));
    }

    @Test
    public void testDeleteAclOnPrefixedResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), prefixedResource);

        removeAcls(authorizer, Set.of(allowReadAcl), prefixedResource);

        assertEquals(Set.of(allowWriteAcl), getAcls(authorizer, prefixedResource));
    }

    @Test
    public void testDeleteAllAclOnPrefixedResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), prefixedResource);

        removeAcls(authorizer, Set.of(), prefixedResource);

        assertEquals(Set.of(), getAcls(authorizer));
    }

    @Test
    public void testAddAclsOnLiteralResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), resource);
        addAcls(authorizer, Set.of(allowWriteAcl, denyReadAcl), resource);

        assertEquals(Set.of(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer, resource));
        assertEquals(Set.of(), getAcls(authorizer, wildCardResource));
        assertEquals(Set.of(), getAcls(authorizer, prefixedResource));
    }

    @Test
    public void testAddAclsOnWildcardResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), wildCardResource);
        addAcls(authorizer, Set.of(allowWriteAcl, denyReadAcl), wildCardResource);

        assertEquals(Set.of(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer, wildCardResource));
        assertEquals(Set.of(), getAcls(authorizer, resource));
        assertEquals(Set.of(), getAcls(authorizer, prefixedResource));
    }

    @Test
    public void testAddAclsOnPrefixedResource() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl, allowWriteAcl), prefixedResource);
        addAcls(authorizer, Set.of(allowWriteAcl, denyReadAcl), prefixedResource);

        assertEquals(Set.of(allowReadAcl, allowWriteAcl, denyReadAcl), getAcls(authorizer, prefixedResource));
        assertEquals(Set.of(), getAcls(authorizer, wildCardResource));
        assertEquals(Set.of(), getAcls(authorizer, resource));
    }

    @Test
    public void testAuthorizeWithPrefixedResource() throws Exception {
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "a_other", LITERAL));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "a_other", PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID() + "-zzz", PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "fooo-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "fo-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "fop-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "fon-" + UUID.randomUUID(), PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "fon-", PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "z_other", PREFIXED));
        addAcls(authorizer, Set.of(denyReadAcl), new ResourcePattern(TOPIC, "z_other", LITERAL));

        addAcls(authorizer, Set.of(allowReadAcl), prefixedResource);

        assertTrue(authorize(authorizer, requestContext, READ, resource));
    }

    @Test
    public void testSingleCharacterResourceAcls() throws Exception {
        addAcls(authorizer, Set.of(allowReadAcl), new ResourcePattern(TOPIC, "f", LITERAL));
        assertTrue(authorize(authorizer, requestContext, READ, new ResourcePattern(TOPIC, "f", LITERAL)));
        assertFalse(authorize(authorizer, requestContext, READ, new ResourcePattern(TOPIC, "foo", LITERAL)));

        addAcls(authorizer, Set.of(allowReadAcl), new ResourcePattern(TOPIC, "_", PREFIXED));
        assertTrue(authorize(authorizer, requestContext, READ, new ResourcePattern(TOPIC, "_foo", LITERAL)));
        assertTrue(authorize(authorizer, requestContext, READ, new ResourcePattern(TOPIC, "_", LITERAL)));
        assertFalse(authorize(authorizer, requestContext, READ, new ResourcePattern(TOPIC, "foo_", LITERAL)));
    }

    @Test
    public void testGetAclsPrincipal() throws Exception {
        AccessControlEntry aclOnSpecificPrincipal = new AccessControlEntry(principal.toString(), WILDCARD_HOST, WRITE, ALLOW);
        addAcls(authorizer, Set.of(aclOnSpecificPrincipal), resource);

        assertEquals(0,
                getAcls(authorizer, wildcardPrincipal).size(), "acl on specific should not be returned for wildcard request");
        assertEquals(1,
                getAcls(authorizer, principal).size(), "acl on specific should be returned for specific request");
        assertEquals(1,
                getAcls(authorizer, new KafkaPrincipal(principal.getPrincipalType(), principal.getName())).size(), "acl on specific should be returned for different principal instance");

        removeAcls(authorizer, Set.of(), resource);
        AccessControlEntry aclOnWildcardPrincipal = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, WRITE, ALLOW);
        addAcls(authorizer, Set.of(aclOnWildcardPrincipal), resource);

        assertEquals(1, getAcls(authorizer, wildcardPrincipal).size(), "acl on wildcard should be returned for wildcard request");
        assertEquals(0, getAcls(authorizer, principal).size(), "acl on wildcard should not be returned for specific request");
    }

    @Test
    public void testAclsFilter() throws Exception {
        ResourcePattern resource1 = new ResourcePattern(TOPIC, "foo-" + UUID.randomUUID(), LITERAL);
        ResourcePattern resource2 = new ResourcePattern(TOPIC, "bar-" + UUID.randomUUID(), LITERAL);
        ResourcePattern prefixedResource = new ResourcePattern(TOPIC, "bar-", PREFIXED);

        AclBinding acl1 = new AclBinding(resource1, new AccessControlEntry(principal.toString(), WILDCARD_HOST, READ, ALLOW));
        AclBinding acl2 = new AclBinding(resource1, new AccessControlEntry(principal.toString(), "192.168.0.1", WRITE, ALLOW));
        AclBinding acl3 = new AclBinding(resource2, new AccessControlEntry(principal.toString(), WILDCARD_HOST, DESCRIBE, ALLOW));
        AclBinding acl4 = new AclBinding(prefixedResource, new AccessControlEntry(wildcardPrincipal.toString(), WILDCARD_HOST, READ, ALLOW));

        authorizer.createAcls(requestContext, List.of(acl1, acl2, acl3, acl4));
        assertEquals(Set.of(acl1, acl2, acl3, acl4), toSet(authorizer.acls(AclBindingFilter.ANY)));
        assertEquals(Set.of(acl1, acl2), toSet(authorizer.acls(new AclBindingFilter(resource1.toFilter(), AccessControlEntryFilter.ANY))));
        assertEquals(Set.of(acl4), toSet(authorizer.acls(new AclBindingFilter(prefixedResource.toFilter(), AccessControlEntryFilter.ANY))));
        AclBindingFilter matchingFilter = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, resource2.name(), MATCH), AccessControlEntryFilter.ANY);
        assertEquals(Set.of(acl3, acl4), toSet(authorizer.acls(matchingFilter)));

        List<AclBindingFilter> filters = List.of(matchingFilter,
                acl1.toFilter(),
                new AclBindingFilter(resource2.toFilter(), AccessControlEntryFilter.ANY),
                new AclBindingFilter(new ResourcePatternFilter(TOPIC, "baz", PatternType.ANY), AccessControlEntryFilter.ANY));
        List<AclDeleteResult> deleteResults = new ArrayList<>();
        for (CompletionStage<AclDeleteResult> stage : authorizer.deleteAcls(requestContext, filters)) {
            deleteResults.add(stage.toCompletableFuture().get());
        }
        assertTrue(deleteResults.stream().noneMatch(r -> r.exception().isPresent()));
        for (int i = 0; i < filters.size(); i++) {
            assertTrue(deleteResults.get(i).aclBindingDeleteResults().stream().noneMatch(r -> r.exception().isPresent()));
        }
        assertEquals(Set.of(acl3, acl4), deleteResults.get(0).aclBindingDeleteResults().stream().map(AclDeleteResult.AclBindingDeleteResult::aclBinding).collect(Collectors.toSet()));
        assertEquals(Set.of(acl1), deleteResults.get(1).aclBindingDeleteResults().stream().map(AclDeleteResult.AclBindingDeleteResult::aclBinding).collect(Collectors.toSet()));
        // standard authorizer first finds the acls that match filters and then delete them.
        // So filters[2] will match acl3 even though it is also matching filters[0] and will be deleted by it
        assertEquals(Set.of(acl3), deleteResults.get(2).aclBindingDeleteResults().stream().map(AclDeleteResult.AclBindingDeleteResult::aclBinding).collect(Collectors.toSet()));
        assertEquals(Set.of(), deleteResults.get(3).aclBindingDeleteResults().stream().map(AclDeleteResult.AclBindingDeleteResult::aclBinding).collect(Collectors.toSet()));
    }

    @Test
    public void testAuthorizeByResourceTypeNoAclFoundOverride() throws IOException {
        Map<String, Object> cfg = configs();
        cfg.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        try (Authorizer authorizer = createAuthorizer(cfg)) {
            assertTrue(authorizeByResourceType(authorizer, requestContext, READ, resource.resourceType()),
                    "If allow.everyone.if.no.acl.found = true, caller should have read access to at least one topic");
            assertTrue(authorizeByResourceType(authorizer, requestContext, WRITE, resource.resourceType()),
                    "If allow.everyone.if.no.acl.found = true, caller should have write access to at least one topic");
        }
    }

    private <T> Set<T> toSet(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toSet());
    }

    private Set<AccessControlEntry> changeAclAndVerify(Set<AccessControlEntry> originalAcls,
                                                       Set<AccessControlEntry> addedAcls,
                                                       Set<AccessControlEntry> removedAcls) throws Exception {
        return changeAclAndVerify(originalAcls, addedAcls, removedAcls, resource);
    }

    private Set<AccessControlEntry> changeAclAndVerify(Set<AccessControlEntry> originalAcls,
                                                       Set<AccessControlEntry> addedAcls,
                                                       Set<AccessControlEntry> removedAcls,
                                                       ResourcePattern resource) throws Exception {
        Set<AccessControlEntry> acls = new HashSet<>(originalAcls);

        if (!addedAcls.isEmpty()) {
            addAcls(authorizer, addedAcls, resource);
            acls.addAll(addedAcls);
        }

        if (!removedAcls.isEmpty()) {
            removeAcls(authorizer, removedAcls, resource);
            acls.removeAll(removedAcls);
        }

        ServerTestUtils.waitAndVerifyAcls(acls, authorizer, resource, AccessControlEntryFilter.ANY);

        return acls;
    }

    private boolean authorize(Authorizer authorizer, RequestContext requestContext, AclOperation operation, ResourcePattern resource) {
        Action action = new Action(operation, resource, 1, true, true);
        return authorizer.authorize(requestContext, List.of(action)).get(0) == AuthorizationResult.ALLOWED;
    }

    private Set<AccessControlEntry> getAcls(Authorizer authorizer, ResourcePattern resourcePattern) {
        return toSet(authorizer.acls(new AclBindingFilter(resourcePattern.toFilter(), AccessControlEntryFilter.ANY))).stream()
            .map(AclBinding::entry).collect(Collectors.toSet());
    }

    private Set<AclBinding> getAcls(Authorizer authorizer, KafkaPrincipal principal) {
        AclBindingFilter filter = new AclBindingFilter(ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(principal.toString(), null, AclOperation.ANY, AclPermissionType.ANY));
        return toSet(authorizer.acls(filter));
    }

    private Set<AclBinding> getAcls(Authorizer authorizer) {
        return toSet(authorizer.acls(AclBindingFilter.ANY));
    }

    private boolean invalidOp(AclOperation op) {
        return op == AclOperation.ANY || op == AclOperation.UNKNOWN;
    }

    private Authorizer createAuthorizer(Map<String, ?> configs) {
        Metrics metrics = new Metrics();
        metricsInstances.add(metrics);
        PluginMetricsImpl pluginMetrics = new PluginMetricsImpl(metrics, Map.of());
        pluginMetricsInstances.add(pluginMetrics);
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(configs);
        authorizer.withPluginMetrics(pluginMetrics);
        authorizer.start(new AuthorizerTestServerInfo(List.of(plaintext)));
        authorizer.setAclMutator(new MockAclMutator(authorizer));
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private RequestContext newRequestContext(KafkaPrincipal principal, InetAddress clientAddress) {
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 2, "", 1);
        return new RequestContext(header, "", clientAddress, principal, ListenerName.forSecurityProtocol(securityProtocol),
                securityProtocol, ClientInformation.EMPTY, false);
    }

    private boolean authorizeByResourceType(Authorizer authorizer, RequestContext requestContext, AclOperation operation, ResourceType resourceType) {
        return authorizer.authorizeByResourceType(requestContext, operation, resourceType) == AuthorizationResult.ALLOWED;
    }

    private void addAcls(Authorizer authorizer, Set<AccessControlEntry> aces, ResourcePattern resourcePattern) throws Exception {
        List<AclBinding> bindings = aces.stream().map(ace -> new AclBinding(resourcePattern, ace)).toList();
        List<? extends CompletionStage<AclCreateResult>> results = authorizer.createAcls(requestContext, bindings);
        for (CompletionStage<AclCreateResult> ac : results) {
            AclCreateResult result = ac.toCompletableFuture().get();
            result.exception().ifPresent(e -> {
                throw e;
            });
        }
    }

    private void removeAcls(Authorizer authorizer, Set<AccessControlEntry> aces, ResourcePattern resourcePattern) throws Exception {
        List<AclBindingFilter> filters = aces.isEmpty()
            ? List.of(new AclBindingFilter(resourcePattern.toFilter(), AccessControlEntryFilter.ANY))
            : aces.stream().map(ace -> new AclBinding(resourcePattern, ace).toFilter()).toList();

        for (CompletionStage<AclDeleteResult> stage : authorizer.deleteAcls(requestContext, filters)) {
            AclDeleteResult result = stage.toCompletableFuture().get();
            result.exception().ifPresent(e -> {
                throw e;
            });
            for (AclDeleteResult.AclBindingDeleteResult r : result.aclBindingDeleteResults()) {
                r.exception().ifPresent(e -> {
                    throw e;
                });
            }
        }
    }

}
