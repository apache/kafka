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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.SUPER_USERS_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.getConfiguredSuperUsers;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.getDefaultResult;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD_PRINCIPAL;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.findResult;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class StandardAuthorizerTest {
    @Test
    public void testGetConfiguredSuperUsers() {
        assertEquals(Collections.emptySet(),
            getConfiguredSuperUsers(Collections.emptyMap()));
        assertEquals(Collections.emptySet(),
            getConfiguredSuperUsers(Collections.singletonMap(SUPER_USERS_CONFIG, " ")));
        assertEquals(new HashSet<>(asList("User:bob", "User:alice")),
            getConfiguredSuperUsers(Collections.singletonMap(SUPER_USERS_CONFIG, "User:bob;User:alice ")));
        assertEquals(new HashSet<>(asList("User:bob", "User:alice")),
            getConfiguredSuperUsers(Collections.singletonMap(SUPER_USERS_CONFIG, ";  User:bob  ;  User:alice ")));
        assertEquals("expected a string in format principalType:principalName but got bob",
            assertThrows(IllegalArgumentException.class, () -> getConfiguredSuperUsers(
                Collections.singletonMap(SUPER_USERS_CONFIG, "bob;:alice"))).getMessage());
    }

    @Test
    public void testGetDefaultResult() {
        assertEquals(DENIED, getDefaultResult(Collections.emptyMap()));
        assertEquals(ALLOWED, getDefaultResult(Collections.singletonMap(
            ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true")));
        assertEquals(DENIED, getDefaultResult(Collections.singletonMap(
            ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false")));
    }

    @Test
    public void testConfigure() {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(SUPER_USERS_CONFIG, "User:alice;User:chris");
        configs.put(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");
        authorizer.configure(configs);
        assertEquals(new HashSet<>(asList("User:alice", "User:chris")), authorizer.superUsers());
        assertEquals(ALLOWED, authorizer.defaultResult());
    }

    static Action newAction(AclOperation aclOperation,
                            ResourceType resourceType,
                            String resourceName) {
        return new Action(aclOperation,
            new ResourcePattern(resourceType, resourceName, LITERAL), 1, false, false);
    }

    private final static AtomicLong NEXT_ID = new AtomicLong(0);

    static StandardAcl newFooAcl(AclOperation op, AclPermissionType permission) {
        return new StandardAcl(
            TOPIC,
            "foo_",
            PREFIXED,
            "User:bob",
            WILDCARD,
            op,
            permission);
    }

    static StandardAclWithId withId(StandardAcl acl) {
        return new StandardAclWithId(new Uuid(acl.hashCode(), acl.hashCode()), acl);
    }

    @Test
    public void testFindResultImplication() throws Exception {
        // These permissions all imply DESCRIBE.
        for (AclOperation op : asList(DESCRIBE, READ, WRITE, DELETE, ALTER)) {
            assertEquals(ALLOWED, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                newFooAcl(op, ALLOW)));
        }
        // CREATE does not imply DESCRIBE
        assertEquals(null, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
            newFooAcl(CREATE, ALLOW)));
        // Deny ACLs don't do "implication".
        for (AclOperation op : asList(READ, WRITE, DELETE, ALTER)) {
            assertEquals(null, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                newFooAcl(op, DENY)));
        }
        // Exact match
        assertEquals(DENIED, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
            newFooAcl(DESCRIBE, DENY)));
        // These permissions all imply DESCRIBE_CONFIGS.
        for (AclOperation op : asList(DESCRIBE_CONFIGS, ALTER_CONFIGS)) {
            assertEquals(ALLOWED, findResult(newAction(DESCRIBE_CONFIGS, TOPIC, "foo_bar"),
                new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                newFooAcl(op, ALLOW)));
        }
        // Deny ACLs don't do "implication".
        assertEquals(null, findResult(newAction(DESCRIBE_CONFIGS, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
            newFooAcl(ALTER_CONFIGS, DENY)));
        // Exact match
        assertEquals(DENIED, findResult(newAction(ALTER_CONFIGS, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
            newFooAcl(ALTER_CONFIGS, DENY)));
    }

    static StandardAcl newBarAcl(AclOperation op, AclPermissionType permission) {
        return new StandardAcl(
            GROUP,
            "bar",
            LITERAL,
            WILDCARD_PRINCIPAL,
            WILDCARD,
            op,
            permission);
    }

    @Test
    public void testFindResultPrincipalMatching() throws Exception {
        assertEquals(ALLOWED, findResult(newAction(READ, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
            newFooAcl(READ, ALLOW)));
        // Principal does not match.
        assertEquals(null, findResult(newAction(READ, TOPIC, "foo_bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "alice")).build(),
            newFooAcl(READ, ALLOW)));
        // Wildcard principal matches anything.
        assertEquals(DENIED, findResult(newAction(READ, GROUP, "bar"),
            new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "alice")).build(),
            newBarAcl(READ, DENY)));
    }

    private static void assertContains(Iterable<AclBinding> iterable, StandardAcl... acls) {
        Iterator<AclBinding> iterator = iterable.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            AclBinding acl = iterator.next();
            assertTrue(i < acls.length, "Only expected " + i + " element(s)");
            assertEquals(acls[i].toBinding(), acl, "Unexpected element " + i);
        }
        assertFalse(iterator.hasNext(), "Expected only " + acls.length + " element(s)");
    }

    @Test
    public void testListAcls() throws Exception {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAclWithId> fooAcls = asList(
            withId(newFooAcl(READ, ALLOW)),
            withId(newFooAcl(WRITE, ALLOW)));
        List<StandardAclWithId> barAcls = asList(
            withId(newBarAcl(DESCRIBE_CONFIGS, DENY)),
            withId(newBarAcl(ALTER_CONFIGS, DENY)));
        fooAcls.forEach(a -> authorizer.addAcl(a.id(), a.acl()));
        barAcls.forEach(a -> authorizer.addAcl(a.id(), a.acl()));
        assertContains(authorizer.acls(AclBindingFilter.ANY),
            fooAcls.get(0).acl(), fooAcls.get(1).acl(), barAcls.get(0).acl(), barAcls.get(1).acl());
        authorizer.removeAcl(fooAcls.get(1).id());
        assertContains(authorizer.acls(AclBindingFilter.ANY),
            fooAcls.get(0).acl(), barAcls.get(0).acl(), barAcls.get(1).acl());
        assertContains(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
            TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY)),
                fooAcls.get(0).acl());
    }

    @Test
    public void testSimpleAuthorizations() throws Exception {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAclWithId> fooAcls = asList(
            withId(newFooAcl(READ, ALLOW)),
            withId(newFooAcl(WRITE, ALLOW)));
        List<StandardAclWithId> barAcls = asList(
            withId(newBarAcl(DESCRIBE_CONFIGS, ALLOW)),
            withId(newBarAcl(ALTER_CONFIGS, ALLOW)));
        fooAcls.forEach(a -> authorizer.addAcl(a.id(), a.acl()));
        barAcls.forEach(a -> authorizer.addAcl(a.id(), a.acl()));
        assertEquals(singletonList(ALLOWED),
            authorizer.authorize(new MockAuthorizableRequestContext.Builder().
                setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                    singletonList(newAction(READ, TOPIC, "foo_"))));
        assertEquals(singletonList(ALLOWED),
            authorizer.authorize(new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "fred")).build(),
                singletonList(newAction(ALTER_CONFIGS, GROUP, "bar"))));
    }

    @Test
    public void testDenyPrecedenceWithOperationAll() throws Exception {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAcl> acls = Arrays.asList(
            new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", "*", ALL, DENY),
            new StandardAcl(TOPIC, "foo", PREFIXED, "User:alice", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "foo", LITERAL, "User:*", "*", ALL, DENY),
            new StandardAcl(TOPIC, "foo", PREFIXED, "User:*", "*", DESCRIBE, ALLOW)
        );

        acls.forEach(acl -> {
            StandardAclWithId aclWithId = withId(acl);
            authorizer.addAcl(aclWithId.id(), aclWithId.acl());
        });

        assertEquals(Arrays.asList(DENIED, DENIED, DENIED, ALLOWED), authorizer.authorize(
            newRequestContext("alice"),
            Arrays.asList(
                newAction(WRITE, TOPIC, "foo"),
                newAction(READ, TOPIC, "foo"),
                newAction(DESCRIBE, TOPIC, "foo"),
                newAction(READ, TOPIC, "foobar"))));

        assertEquals(Arrays.asList(DENIED, DENIED, DENIED, ALLOWED, DENIED), authorizer.authorize(
            newRequestContext("bob"),
            Arrays.asList(
                newAction(DESCRIBE, TOPIC, "foo"),
                newAction(READ, TOPIC, "foo"),
                newAction(WRITE, TOPIC, "foo"),
                newAction(DESCRIBE, TOPIC, "foobaz"),
                newAction(READ, TOPIC, "foobaz"))));
    }

    @Test
    public void testTopicAclWithOperationAll() throws Exception {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAcl> acls = Arrays.asList(
            new StandardAcl(TOPIC, "foo", LITERAL, "User:*", "*", ALL, ALLOW),
            new StandardAcl(TOPIC, "bar", PREFIXED, "User:alice", "*", ALL, ALLOW),
            new StandardAcl(TOPIC, "baz", LITERAL, "User:bob", "*", ALL, ALLOW)
        );

        acls.forEach(acl -> {
            StandardAclWithId aclWithId = withId(acl);
            authorizer.addAcl(aclWithId.id(), aclWithId.acl());
        });

        assertEquals(Arrays.asList(ALLOWED, ALLOWED, DENIED), authorizer.authorize(
            newRequestContext("alice"),
            Arrays.asList(
                newAction(WRITE, TOPIC, "foo"),
                newAction(DESCRIBE_CONFIGS, TOPIC, "bar"),
                newAction(DESCRIBE, TOPIC, "baz"))));

        assertEquals(Arrays.asList(ALLOWED, DENIED, ALLOWED), authorizer.authorize(
            newRequestContext("bob"),
            Arrays.asList(
                newAction(WRITE, TOPIC, "foo"),
                newAction(READ, TOPIC, "bar"),
                newAction(DESCRIBE, TOPIC, "baz"))));

        assertEquals(Arrays.asList(ALLOWED, DENIED, DENIED), authorizer.authorize(
            newRequestContext("malory"),
            Arrays.asList(
                newAction(DESCRIBE, TOPIC, "foo"),
                newAction(WRITE, TOPIC, "bar"),
                newAction(READ, TOPIC, "baz"))));
    }

    private AuthorizableRequestContext newRequestContext(String principal) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
            .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
            .build();
    }

    @Test
    public void testHostAddressAclValidation() throws Exception {
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");

        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAcl> acls = Arrays.asList(
            new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", host1.getHostAddress(), READ, DENY),
            new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "bar", LITERAL, "User:bob", host2.getHostAddress(), READ, ALLOW),
            new StandardAcl(TOPIC, "bar", LITERAL, "User:*", InetAddress.getLocalHost().getHostAddress(), DESCRIBE, ALLOW)
        );

        acls.forEach(acl -> {
            StandardAclWithId aclWithId = withId(acl);
            authorizer.addAcl(aclWithId.id(), aclWithId.acl());
        });

        List<Action> actions = Arrays.asList(
            newAction(READ, TOPIC, "foo"),
            newAction(READ, TOPIC, "bar"),
            newAction(DESCRIBE, TOPIC, "bar")
        );

        assertEquals(Arrays.asList(ALLOWED, DENIED, ALLOWED), authorizer.authorize(
            newRequestContext("alice", InetAddress.getLocalHost()), actions));

        assertEquals(Arrays.asList(DENIED, DENIED, DENIED), authorizer.authorize(
            newRequestContext("alice", host1), actions));

        assertEquals(Arrays.asList(ALLOWED, DENIED, DENIED), authorizer.authorize(
            newRequestContext("alice", host2), actions));

        assertEquals(Arrays.asList(DENIED, DENIED, ALLOWED), authorizer.authorize(
            newRequestContext("bob", InetAddress.getLocalHost()), actions));

        assertEquals(Arrays.asList(DENIED, DENIED, DENIED), authorizer.authorize(
            newRequestContext("bob", host1), actions));

        assertEquals(Arrays.asList(DENIED, ALLOWED, ALLOWED), authorizer.authorize(
            newRequestContext("bob", host2), actions));
    }

    private AuthorizableRequestContext newRequestContext(String principal, InetAddress clientAddress) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
            .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
            .setClientAddress(clientAddress)
            .build();
    }

    private static StandardAuthorizer createAuthorizerWithManyAcls() {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.configure(Collections.emptyMap());
        List<StandardAcl> acls = Arrays.asList(
            new StandardAcl(TOPIC, "green2", LITERAL, "User:*", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "green", PREFIXED, "User:bob", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "betamax4", LITERAL, "User:bob", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "betamax", LITERAL, "User:bob", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "beta", PREFIXED, "User:*", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "alpha", PREFIXED, "User:*", "*", READ, ALLOW),
            new StandardAcl(TOPIC, "alp", PREFIXED, "User:bob", "*", READ, DENY),
            new StandardAcl(GROUP, "*", LITERAL, "User:bob", "*", WRITE, ALLOW),
            new StandardAcl(GROUP, "wheel", LITERAL, "User:*", "*", WRITE, DENY)
        );
        acls.forEach(acl -> {
            StandardAclWithId aclWithId = withId(acl);
            authorizer.addAcl(aclWithId.id(), aclWithId.acl());
        });
        return authorizer;
    }

    @Test
    public void testAuthorizationWithManyAcls() throws Exception {
        StandardAuthorizer authorizer = createAuthorizerWithManyAcls();
        assertEquals(Arrays.asList(ALLOWED, DENIED),
            authorizer.authorize(new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                Arrays.asList(newAction(READ, TOPIC, "green1"),
                    newAction(WRITE, GROUP, "wheel"))));
        assertEquals(Arrays.asList(DENIED, ALLOWED, DENIED),
            authorizer.authorize(new MockAuthorizableRequestContext.Builder().
                    setPrincipal(new KafkaPrincipal(USER_TYPE, "bob")).build(),
                Arrays.asList(newAction(READ, TOPIC, "alpha"),
                    newAction(WRITE, GROUP, "arbitrary"),
                    newAction(READ, TOPIC, "ala"))));
    }
}