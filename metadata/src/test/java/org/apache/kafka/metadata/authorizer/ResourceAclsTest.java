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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Arrays.asList;
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
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.newAction;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.newBarAcl;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.newFooAcl;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ResourceAclsTest {
    static final KafkaPrincipal ALICE = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");

    static final KafkaPrincipal BOB = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");

    static final StandardAcl ACL1 = new StandardAcl(
            TOPIC,
            "foo",
            LITERAL,
            "User:bob",
            "127.0.0.1",
            AclOperation.READ,
            AclPermissionType.ALLOW);

    static final StandardAcl ACL2 = new StandardAcl(
            TOPIC,
            "foo",
            LITERAL,
            "User:alice",
            "127.0.0.1",
            AclOperation.WRITE,
            AclPermissionType.DENY);

    static final StandardAcl ACL3 = new StandardAcl(
            TOPIC,
            StandardAuthorizerConstants.WILDCARD,
            LITERAL,
            "User:chris",
            "127.0.0.1",
            AclOperation.WRITE,
            AclPermissionType.DENY);

    static final StandardAcl ACL4 = new StandardAcl(
            TOPIC,
            "f",
            PREFIXED,
            "User:chris",
            "127.0.0.1",
            AclOperation.ALL,
            AclPermissionType.DENY);

    static ResourceAclsChanges addAcl1AndAcl2() {
        ResourceAclsChanges changes = new ResourceAclsChanges();
        changes.newAddition(ACL1);
        changes.newAddition(ACL2);
        return changes;
    }

    static ResourceAclsChanges removeAcl1() {
        ResourceAclsChanges changes = new ResourceAclsChanges();
        changes.newRemoval(ACL1);
        return changes;
    }

    @Test
    public void testEmptyResourceAcls() {
        assertTrue(ResourceAcls.EMPTY.isEmpty());
        assertFalse(ResourceAcls.EMPTY.treatedAsPrefixAcls());
        assertNull(ResourceAcls.EMPTY.authorize(null, null, null));
    }

    @Test
    public void tesTreatedAsPrefixAcls() {
        assertFalse(new ResourceAcls(Arrays.asList(ACL1)).treatedAsPrefixAcls());
        assertFalse(new ResourceAcls(Arrays.asList(ACL2)).treatedAsPrefixAcls());
        assertTrue(new ResourceAcls(Arrays.asList(ACL3)).treatedAsPrefixAcls());
        assertTrue(new ResourceAcls(Arrays.asList(ACL4)).treatedAsPrefixAcls());
    }

    @Test
    public void testResourceAclsChanges() {
        ResourceAclsChanges changes = new ResourceAclsChanges();
        assertEquals(0, changes.numRemovals());
        assertEquals(0, changes.netSizeChange());
        assertFalse(changes.isRemoved(ACL1));
        changes.newRemoval(ACL1);
        assertTrue(changes.isRemoved(ACL1));
        assertEquals(1, changes.numRemovals());
        assertEquals(-1, changes.netSizeChange());
        changes.newAddition(ACL2);
        assertEquals(1, changes.numRemovals());
        assertEquals(0, changes.netSizeChange());
    }

    @Test
    public void tesCopyWithChanges() {
        ResourceAcls twoAcls = ResourceAcls.EMPTY.copyWithChanges(addAcl1AndAcl2());
        assertEquals(new ResourceAcls(Arrays.asList(ACL1, ACL2)), twoAcls);
        ResourceAcls oneAcl = twoAcls.copyWithChanges(removeAcl1());
        assertEquals(new ResourceAcls(Arrays.asList(ACL2)), oneAcl);
    }

    @Test
    public void testToString() {
        ResourceAcls twoAcls = ResourceAcls.EMPTY.copyWithChanges(addAcl1AndAcl2());
        assertEquals("ResourceAcls[StandardAcl(resourceType=TOPIC, resourceName=foo, " +
            "patternType=LITERAL, principal=User:bob, host=127.0.0.1, " +
            "operation=READ, permissionType=ALLOW), StandardAcl(resourceType=TOPIC, " +
            "resourceName=foo, patternType=LITERAL, principal=User:alice, host=127.0.0.1, " +
            "operation=WRITE, permissionType=DENY)]", twoAcls.toString());
        ResourceAcls oneAcl = twoAcls.copyWithChanges(removeAcl1());
        assertEquals("ResourceAcls[StandardAcl(resourceType=TOPIC, " +
            "resourceName=foo, patternType=LITERAL, principal=User:alice, host=127.0.0.1, " +
            "operation=WRITE, permissionType=DENY)]", oneAcl.toString());
    }

    @Test
    public void testAuthorize() {
        ResourceAcls resourceAcls = new ResourceAcls(Arrays.asList(ACL1, ACL2));
        assertEquals(new MatchingAclRule(ACL1, AuthorizationResult.ALLOWED),
            resourceAcls.authorize(AclOperation.READ,
                Collections.singleton(BOB),
                    "127.0.0.1"));
        assertEquals(new MatchingAclRule(ACL2, AuthorizationResult.DENIED),
                resourceAcls.authorize(AclOperation.WRITE,
                    Collections.singleton(ALICE),
                        "127.0.0.1"));
        assertEquals(null,
                resourceAcls.authorize(AclOperation.WRITE,
                        Collections.singleton(BOB),
                        "127.0.0.1"));
    }

    private static AuthorizationResult findResult(
        Action action,
        AuthorizableRequestContext requestContext,
        StandardAcl acl
    ) {
        ResourceAcls resourceAcls = new ResourceAcls(Collections.singletonList(acl));
        MatchingAclRule rule = resourceAcls.authorize(action.operation(),
                StandardAuthorizerData.matchingPrincipals(requestContext),
                "127.0.0.1");
        return (rule == null) ? null : rule.result();
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

}
