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
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.metadata.authorizer.AuthorizerData.WILDCARD;
import static org.apache.kafka.metadata.authorizer.AuthorizerData.WILDCARD_PRINCIPAL;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public abstract class AbstractAuthorizerDataTest {

    protected static final Set<String> DEFAULT_SUPERUSERS = new HashSet<>(Arrays.asList("User:superman", "Group:kryptonites"));
    protected static final AclMutator DEFAULT_MUTATOR = mock(AclMutator.class);
    protected static final Map<Uuid, StandardAcl> DEFAULT_ACLS = new HashMap<>();

    protected static final Set<String> INITIAL_SUPERUSERS = Collections.emptySet();
    protected static final AclMutator INITIAL_MUTATOR = null;
    protected static final Map<Uuid, StandardAcl> INITIAL_ACLS = Collections.emptyMap();

    protected AbstractAuthorizerDataTest() {
        StandardAclWithId aclWithId = AuthorizerTestUtils.withId(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW));
        DEFAULT_ACLS.put(aclWithId.id(), aclWithId.acl());
        aclWithId = AuthorizerTestUtils.withId(new StandardAcl(GROUP, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW));
        DEFAULT_ACLS.put(aclWithId.id(), aclWithId.acl());
    }

    protected abstract AuthorizerData getAuthorizerData();

    protected void assertAcceptableAclCount(AuthorizerData data, int value) {
        int i = data.aclCount();
        if (i != value && i != -1) {
            fail(format("aclCount() should return %s or -1.", value));
        }
    }

    protected void assertSameAcls(Iterable<StandardAcl> expected, Iterable<AclBinding> actual) {
        // order of ACLs is not guaranteed so use a hashmap to get them in the same order
        Set<AclBinding> expectedSet = new HashSet<>();
        expected.forEach(acl -> expectedSet.add(acl.toBinding()));

        Set<AclBinding> actualSet = new HashSet<>();
        actual.forEach(actualSet::add);

        List<String> errMsgs = new ArrayList<>();
        if (expectedSet.size() != actualSet.size()) {
            errMsgs.add(format("Wrong number of ACLs returned.  Expected:%s Actual:%s", expectedSet.size(), actualSet.size()));
        } else {
            Iterator<AclBinding> expectedIter = expectedSet.iterator();
            Iterator<AclBinding> actualIter = actualSet.iterator();
            for (int i = 0; i < expectedSet.size(); i++) {
                AclBinding expectedElement = expectedIter.next();
                AclBinding actualElement = actualIter.next();

                if (!expectedElement.equals(actualElement)) {
                    errMsgs.add(format("Expected: %s but got %s", expectedElement, actualElement));
                }
            }
        }
        if (!errMsgs.isEmpty()) {
            errMsgs.add("EXPECTED SET");
            expectedSet.forEach(e -> errMsgs.add(e.toString()));
            errMsgs.add("ACTUAL SET");
            actualSet.forEach(e -> errMsgs.add(e.toString()));
            fail(format("ACLs do not match%n%s", String.join(System.lineSeparator(), errMsgs)));
        }
//        assertArrayEquals(expectedSet.toArray(), actualSet.toArray());
    }

    protected <T> void assertSameSet(Iterable<T> expected, Iterable<T> actual, String msg) {
        Set<T> expectedSet = new HashSet<>();
        expected.forEach(expectedSet::add);

        Set<T> actualSet = new HashSet<>();
        actual.forEach(actualSet::add);

        assertArrayEquals(expectedSet.toArray(), actualSet.toArray(), msg);
    }

    protected void assertSettings(AuthorizerData data, Collection<String> superUsers, AuthorizationResult defaultResult, Collection<StandardAcl> acls, AclMutator mutator) {
        assertSameSet(superUsers, data.superUsers(), "There are missing or extra superusers.");
        assertEquals(defaultResult, data.defaultResult(), () -> format("%s should be the default operation.", defaultResult));
        assertAcceptableAclCount(data, acls.size());
        assertSameAcls(acls, data.acls(AclBindingFilter.ANY));
        assertEquals(mutator, data.aclMutator(), "Wrong mutator present.");
        assertNotNull(data.log(), "There should be a logging implementation.");
    }

    @Test
    public void testInitialConstruction() {
        assertSettings(getAuthorizerData(), INITIAL_SUPERUSERS, DENIED, INITIAL_ACLS.values(), INITIAL_MUTATOR);
    }

    private AuthorizerData dataWithAllValuesSet() {
        return getAuthorizerData().copyWithNewConfig(500, DEFAULT_SUPERUSERS, ALLOWED)
                .copyWithNewAclMutator(DEFAULT_MUTATOR).copyWithNewAcls(DEFAULT_ACLS).copyWithNewLoadingComplete(true);
    }

    @Test
    public void copyWithNewConfigTest() {
        assertSettings(getAuthorizerData().copyWithNewConfig(500, DEFAULT_SUPERUSERS, ALLOWED), DEFAULT_SUPERUSERS, ALLOWED, INITIAL_ACLS.values(), INITIAL_MUTATOR);

        // verify setting config does not change other items.
        Set<String> expectedSuperUsers = new HashSet<>(Arrays.asList("User:clarkKent", "Group:metropolitians"));
        assertSettings(dataWithAllValuesSet().copyWithNewConfig(501, expectedSuperUsers, DENIED), expectedSuperUsers, DENIED, DEFAULT_ACLS.values(), DEFAULT_MUTATOR);
    }


    @Test
    public void copyWithNewAclsTest() {
        Map<Uuid, StandardAcl> acls = new HashMap<>();
        Iterator<Map.Entry<Uuid, StandardAcl>> iter = DEFAULT_ACLS.entrySet().iterator();
        Map.Entry<Uuid, StandardAcl> entry = iter.next();

        // adding an acl yields one result.
        acls.put(entry.getKey(), entry.getValue());
        AuthorizerData data = getAuthorizerData().copyWithNewAcls(acls);
        assertSettings(data, INITIAL_SUPERUSERS, DENIED, acls.values(), INITIAL_MUTATOR);


        // adding the same acl 2x is yields one result.
        data = getAuthorizerData().copyWithNewAcls(acls);
        assertSettings(data, INITIAL_SUPERUSERS, DENIED, acls.values(), INITIAL_MUTATOR);

        // adding a new acl yields two results.
        entry = iter.next();
        acls.put(entry.getKey(), entry.getValue());
        data = getAuthorizerData().copyWithNewAcls(acls);
        assertSettings(data, INITIAL_SUPERUSERS, DENIED, acls.values(), INITIAL_MUTATOR);

        // test that updating a populated AuthorizerData does not change things unexpectedly
        acls.clear();
        StandardAclWithId aclWithId = AuthorizerTestUtils.withId(new StandardAcl(ResourceType.USER, "foo_", PREFIXED, "User:alice", WILDCARD, AclOperation.CREATE, ALLOW));
        acls.put(aclWithId.id(), aclWithId.acl());
        aclWithId = AuthorizerTestUtils.withId(new StandardAcl(ResourceType.TRANSACTIONAL_ID, "foo_", PREFIXED, "User:alice", WILDCARD, AclOperation.CREATE, ALLOW));
        acls.put(aclWithId.id(), aclWithId.acl());
        data = dataWithAllValuesSet().copyWithNewAcls(acls);

        assertSettings(data, DEFAULT_SUPERUSERS, ALLOWED, acls.values(), DEFAULT_MUTATOR);

    }

    @Test
    public void copyWithNewAclMutatorTest() {
        AclMutator mutator = mock(AclMutator.class);
        assertSettings(getAuthorizerData().copyWithNewAclMutator(mutator), INITIAL_SUPERUSERS, DENIED, INITIAL_ACLS.values(), mutator);

        // test that updating a populated AuthorizerData does not change things unexpectedly
        assertSettings(dataWithAllValuesSet().copyWithNewAclMutator(mutator), DEFAULT_SUPERUSERS, ALLOWED, DEFAULT_ACLS.values(), mutator);

    }

    @Test
    public void matchingPrincipalsTest() throws Exception {
        AuthorizableRequestContext ctxt = AuthorizerTestUtils.newRequestContext("bob");
        Set<KafkaPrincipal> result = AuthorizerData.matchingPrincipals(ctxt);
        Set<KafkaPrincipal> expected = new HashSet<>(Arrays.asList(ctxt.principal(), AuthorizerData.WILDCARD_KAFKA_PRINCIPAL));
        assertEquals(expected, result);
    }

    @Test
    public void addAclTest() {
        AuthorizerData data = getAuthorizerData();
        Iterator<AclBinding> iter = data.acls(AclBindingFilter.ANY).iterator();
        assertFalse(iter.hasNext(), "There shoudl be no ACLs");

        StandardAclWithId aclWithId = AuthorizerTestUtils.withId(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW));
        data.addAcl(aclWithId.id(), aclWithId.acl());

        iter = data.acls(AclBindingFilter.ANY).iterator();
        assertTrue(iter.hasNext(), "There should be an ACL");
        assertEquals(aclWithId.toBinding(), iter.next(), "Should have returned original ACL");

        assertFalse(iter.hasNext(), "There should not be a second ACL");
    }

    @Test
    public void removeAclTest() {
        AuthorizerData data = getAuthorizerData();

        // add 2 acls
        StandardAclWithId aclWithId1 = AuthorizerTestUtils.withId(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW));
        data.addAcl(aclWithId1.id(), aclWithId1.acl());
        StandardAclWithId aclWithId2 = AuthorizerTestUtils.withId(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.READ, ALLOW));
        data.addAcl(aclWithId2.id(), aclWithId2.acl());

        Iterator<AclBinding> iter = data.acls(AclBindingFilter.ANY).iterator();
        assertTrue(iter.hasNext(), "There should be an ACL");
        iter.next();
        assertTrue(iter.hasNext(), "There should be a second ACL");
        iter.next();
        assertFalse(iter.hasNext(), "There should not be a third ACL");

        data.removeAcl(aclWithId1.id());

        iter = data.acls(AclBindingFilter.ANY).iterator();
        assertTrue(iter.hasNext(), "There should be an ACL");
        AclBinding binding = iter.next();
        assertEquals(aclWithId2.toBinding(), binding, "Should have matches the second ACL inserted");

        assertFalse(iter.hasNext(), "There should not be a second ACL");
    }

    @Test
    public void superUsersTest() {
        AuthorizerData data = getAuthorizerData();
        assertTrue(data.superUsers().isEmpty(), "Should notbe any super users yet.");

        List<String> expected = Arrays.asList("User:superman", "Group:kryptonians");

        data = data.copyWithNewConfig(500, new HashSet<>(expected), ALLOWED);
        Set<String> superUsers = data.superUsers();
        assertEquals(expected.size(), superUsers.size(), "Should only be 2 superusers.");
        for (String name : expected) {
            assertTrue(superUsers.contains(name), () -> format("Superusers is missing '%s'", name));
        }
    }

    @Test
    public void defaultResult() {
        AuthorizerData data = getAuthorizerData();
        assertEquals(DENIED, data.defaultResult(), "Default authorization result should be DENIED");
        data = data.copyWithNewConfig(500, new HashSet<>(Arrays.asList("User:superman", "Group:kryptonians")), ALLOWED);
        assertEquals(ALLOWED, data.defaultResult(), "Default authorization result should be set to ALLOWED");
    }

    @Test
    public void aclCount() {
        AuthorizerData data = getAuthorizerData();
        int count = data.aclCount();
        if (count == -1) {
            LoggerFactory.getLogger(this.getClass()).info(format("Class %s does not support aclCount -- ignoring tests.", data.getClass()));
            return;
        }
        assertEquals(0, count, "AclCount should be zero at start.");

        StandardAclWithId aclWithId = AuthorizerTestUtils.withId(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW));

        data.addAcl(aclWithId.id(), aclWithId.acl());
        assertEquals(1, data.aclCount(), "AclCount should be one.");

        try {
            data.addAcl(aclWithId.id(), aclWithId.acl());
        } catch (RuntimeException accpetable) {
            // acceptable to throw exception.
        }
        assertEquals(1, data.aclCount(), "Duplicate Acl should not increase count.");
    }

    @Test
    public void aclMutatorTest() {
        AuthorizerData data = getAuthorizerData();
        assertNull(data.aclMutator(), "Default aclMutator should be null.");
        AclMutator mutator = mock(AclMutator.class);
        data = data.copyWithNewAclMutator(mutator);
        assertEquals(mutator, data.aclMutator(), "aclMutator should be set.");
        data = data.copyWithNewAclMutator(null);
        assertNull(data.aclMutator(), "Default aclMutator should be null.");
    }

    @Test
    public void logTest() {
        AuthorizerData data = getAuthorizerData();
        assertNotNull(data.log(), "Log not provided.");
    }

    private void addAcl(AuthorizerData data, StandardAcl acl) {
        StandardAclWithId aclWithId = AuthorizerTestUtils.withId(acl);
        data.addAcl(aclWithId.id(), aclWithId.acl());
    }

    @Test
    public void aclsRetrieveResourceTypeTest() {
        AuthorizerData data = getAuthorizerData();
        Map<ResourceType, Set<AclBinding>> expected = new HashMap<>();


        for (ResourceType type : ResourceType.values()) {
            switch (type) {
                case TOPIC:
                case GROUP:
                case CLUSTER:
                case TRANSACTIONAL_ID:
                case DELEGATION_TOKEN:
                case USER:
                    StandardAcl acl = new StandardAcl(type, "foo", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW);
                    addAcl(data, acl);
                    expected.put(type, new HashSet<>(Collections.singletonList(acl.toBinding())));
                    break;
                case UNKNOWN:
                    expected.put(type, Collections.emptySet());
                    break;
                case ANY:
                    break;
            }
        }

        Set<AclBinding> set = new HashSet<>();
        for (Iterable<AclBinding> iterable : expected.values()) {
            iterable.forEach(set::add);
        }
        expected.put(ResourceType.ANY, set);

        for (ResourceType type : ResourceType.values()) {
            ResourcePatternFilter pattern = new ResourcePatternFilter(type, null, PatternType.ANY);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(pattern, AccessControlEntryFilter.ANY));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(type), actual, () -> format("Failed on type %s", type));
        }
    }

    @Test
    public void aclsRetrievePatternTypeTest() {
        AuthorizerData data = getAuthorizerData();
        Map<PatternType, Set<AclBinding>> expected = new HashMap<>();


        for (PatternType type : PatternType.values()) {
            switch (type) {
                case LITERAL:
                case PREFIXED:
                    StandardAcl acl = new StandardAcl(TOPIC, "foo", type, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW);
                    addAcl(data, acl);
                    expected.put(type, new HashSet<>(Collections.singletonList(acl.toBinding())));
                    break;
                case UNKNOWN:
                    expected.put(type, Collections.emptySet());
                    break;
                case MATCH:
                case ANY:
                    break;
            }
        }

        Set<AclBinding> set = new HashSet<>();
        for (Iterable<AclBinding> iterable : expected.values()) {
            iterable.forEach(set::add);
        }
        expected.put(PatternType.ANY, set);
        expected.put(PatternType.MATCH, set);


        for (PatternType type : PatternType.values()) {
            ResourcePatternFilter pattern = new ResourcePatternFilter(TOPIC, null, type);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(pattern, AccessControlEntryFilter.ANY));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(type), actual, () -> format("Failed on type %s", type));
        }
    }

    @Test
    public void aclsRetrievePrincipalTest() {
        AuthorizerData data = getAuthorizerData();
        Map<String, Set<AclBinding>> expected = new HashMap<>();
        List<String> principals = Arrays.asList("User:bob", "User:alice", "Group:bob", "Group:alice");

        for (String principal : principals) {
            StandardAcl acl = new StandardAcl(TOPIC, "foo", PatternType.LITERAL, principal, WILDCARD, AclOperation.CREATE, ALLOW);
            addAcl(data, acl);
            expected.put(principal, new HashSet<>(Collections.singletonList(acl.toBinding())));
        }

        Set<AclBinding> set = new HashSet<>();
        expected.values().forEach(set::addAll);
        expected.put(WILDCARD_PRINCIPAL, set);

        for (String principal : expected.keySet()) {
            AccessControlEntryFilter pattern = new AccessControlEntryFilter(principal.equals(WILDCARD_PRINCIPAL) ? null : principal, null, AclOperation.ANY, AclPermissionType.ANY);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(ResourcePatternFilter.ANY, pattern));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(principal), actual, () -> format("Failed on type %s", principal));
        }
    }

    @Test
    public void aclsRetrieveHostTest() {
        AuthorizerData data = getAuthorizerData();
        Map<String, Set<AclBinding>> expected = new HashMap<>();
        List<String> hosts = Arrays.asList("localhost", "example.com", "example.net", "example.org");

        for (String host : hosts) {
            StandardAcl acl = new StandardAcl(TOPIC, "foo", PatternType.LITERAL, "User:bob", host, AclOperation.CREATE, ALLOW);
            addAcl(data, acl);
            expected.put(host, new HashSet<>(Collections.singletonList(acl.toBinding())));
        }

        Set<AclBinding> set = new HashSet<>();
        expected.values().forEach(set::addAll);
        expected.put(WILDCARD, set);

        for (String host : expected.keySet()) {
            AccessControlEntryFilter pattern = new AccessControlEntryFilter(null, host.equals(WILDCARD) ? null : host, AclOperation.ANY, AclPermissionType.ANY);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(ResourcePatternFilter.ANY, pattern));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(host), actual, () -> format("Failed on type %s", host));
        }
    }

    @Test
    public void aclsRetrieveOperationTest() {
        AuthorizerData data = getAuthorizerData();
        Map<AclOperation, Set<AclBinding>> expected = new HashMap<>();

        for (AclOperation operation : AclOperation.values()) {
            switch (operation) {
                case ALL:
                case READ:
                case WRITE:
                case CREATE:
                case DELETE:
                case ALTER:
                case DESCRIBE:
                case CLUSTER_ACTION:
                case DESCRIBE_CONFIGS:
                case ALTER_CONFIGS:
                case IDEMPOTENT_WRITE:
                case CREATE_TOKENS:
                case DESCRIBE_TOKENS:
                    StandardAcl acl = new StandardAcl(TOPIC, "foo", PatternType.LITERAL, WILDCARD_PRINCIPAL, WILDCARD, operation, ALLOW);
                    addAcl(data, acl);
                    expected.put(operation, new HashSet<>(Collections.singletonList(acl.toBinding())));
                    break;
                case UNKNOWN:
                    expected.put(operation, Collections.emptySet());
                    break;
                case ANY:
                    break;
            }
        }

        Set<AclBinding> set = new HashSet<>();
        expected.values().forEach(set::addAll);
        expected.put(AclOperation.ANY, set);

        for (AclOperation operation : AclOperation.values()) {
            AccessControlEntryFilter pattern = new AccessControlEntryFilter(null, null, operation, AclPermissionType.ANY);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(ResourcePatternFilter.ANY, pattern));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(operation), actual, () -> format("Failed on type %s", operation));
        }
    }


    @Test
    public void aclsRetrievePermissionTypeTest() {
        AuthorizerData data = getAuthorizerData();
        Map<AclPermissionType, Set<AclBinding>> expected = new HashMap<>();

        for (AclPermissionType permission : AclPermissionType.values()) {
            switch (permission) {
                case ALLOW:
                case DENY:
                    StandardAcl acl = new StandardAcl(TOPIC, "foo", PatternType.LITERAL, WILDCARD_PRINCIPAL, WILDCARD, AclOperation.CREATE, permission);
                    addAcl(data, acl);
                    expected.put(permission, new HashSet<>(Collections.singletonList(acl.toBinding())));
                    break;
                case UNKNOWN:
                    expected.put(permission, Collections.emptySet());
                    break;
                case ANY:
                    break;
            }
        }

        Set<AclBinding> set = new HashSet<>();
        expected.values().forEach(set::addAll);
        expected.put(AclPermissionType.ANY, set);

        for (AclPermissionType permission : AclPermissionType.values()) {
            AccessControlEntryFilter pattern = new AccessControlEntryFilter(null, null, AclOperation.ANY, permission);
            Iterable<AclBinding> iterable = data.acls(new AclBindingFilter(ResourcePatternFilter.ANY, pattern));
            Set<AclBinding> actual = new HashSet<>();
            iterable.forEach(actual::add);
            assertEquals(expected.get(permission), actual, () -> format("Failed on type %s", permission));
        }
    }

    protected class Builder implements Supplier<AuthorizerData> {
        private final Set<StandardAcl> acls = new HashSet<>();
        private final Set<String> superusers = new HashSet<>();
        private AclMutator mutator;
        private AuthorizationResult defaultResult = DENIED;

        Builder() {
        }

        public Builder clone() {
            Builder result = new Builder();
            result.acls.addAll(acls);
            result.superusers.addAll(superusers);
            result.mutator = mutator;
            result.defaultResult = defaultResult;
            return result;
        }

        public Builder addAcl(StandardAcl acl) {
            acls.add(acl);
            return this;
        }

        public Builder clearAcls() {
            acls.clear();
            return this;
        }

        public Builder addSuperuser(String user) {
            superusers.add(user);
            return this;
        }

        public Builder clearSuperusers() {
            superusers.clear();
            return this;
        }

        public Builder setMutator(AclMutator mutator) {
            this.mutator = mutator;
            return this;
        }

        public Builder setDefaultResult(AuthorizationResult defaultResult) {
            this.defaultResult = defaultResult;
            return this;
        }

        public AuthorizerData get() {
            AuthorizerData result = getAuthorizerData()
                    .copyWithNewConfig(500, superusers, defaultResult)
                    .copyWithNewAclMutator(mutator);
            if (!acls.isEmpty()) {
                Map<Uuid, StandardAcl> aclMap = new HashMap<>();
                for (StandardAcl acl : acls) {
                    StandardAclWithId aclWithId = AuthorizerTestUtils.withId(acl);
                    aclMap.put(aclWithId.id(), aclWithId.acl());
                }
                result = result.copyWithNewAcls(aclMap);
            }
            return result.copyWithNewLoadingComplete(true);
        }
    }

    class AuthorizeContext {
        MockAuthorizableRequestContext.Builder reqBuilder;
        Builder dataBuilder;

        AuthorizeContext() throws Exception {
            reqBuilder = new MockAuthorizableRequestContext.Builder();
            dataBuilder = new Builder();
        }

        public AuthorizeContext reset() throws Exception {
            reqBuilder = new MockAuthorizableRequestContext.Builder();
            dataBuilder = new Builder();
            return this;
        }

        DynamicTest getTest(AuthorizationResult result, String title, final Action action) throws Exception {
            final MockAuthorizableRequestContext.Builder ctxtBuilder = this.reqBuilder.clone();
            final Builder dataBuilder = this.dataBuilder.clone();

            return DynamicTest.dynamicTest(format("%s: %s", result, title), () -> assertEquals(result, dataBuilder.get().authorize(ctxtBuilder.build(), action)));
        }
    }

    @TestFactory
    Collection<DynamicTest> authorizeTests() throws Exception {
        List<DynamicTest> lst = new ArrayList<>();
        AuthorizeContext ctxt = new AuthorizeContext();

        /* no acls */
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        lst.add(ctxt.getTest(DENIED, "default authorize with no ACLs", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));

        ctxt.dataBuilder.addSuperuser("User:superman");
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "superman"));
        lst.add(ctxt.getTest(ALLOWED, "Superuser with no ACLs", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));

        ctxt.dataBuilder.setDefaultResult(ALLOWED);
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        lst.add(ctxt.getTest(ALLOWED, "authorize with no ACLs", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));


        /* with ACL */
        ctxt.dataBuilder.addAcl(new StandardAcl(TOPIC, "topic1", LITERAL, "User:alice", WILDCARD, READ, ALLOW));
        lst.add(ctxt.getTest(ALLOWED, "user with access to other resource and default ALLOWED", AuthorizerTestUtils.newAction(READ, TOPIC, "topic2")));
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"));
        lst.add(ctxt.getTest(DENIED, "user without access and default ALLOWED", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));
        lst.add(ctxt.getTest(ALLOWED, "user access to topic without ACLs and default ALLOWED", AuthorizerTestUtils.newAction(READ, TOPIC, "topic2")));


        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        ctxt.dataBuilder.setDefaultResult(DENIED);
        lst.add(ctxt.getTest(ALLOWED, "user with access", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));
        lst.add(ctxt.getTest(DENIED, "user with access to other resource", AuthorizerTestUtils.newAction(READ, TOPIC, "topic2")));

        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"));
        lst.add(ctxt.getTest(DENIED, "user without access", AuthorizerTestUtils.newAction(READ, TOPIC, "topic1")));

        /* with PREFIXED ACLs */
        ctxt.reset();
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"));
        ctxt.dataBuilder.addSuperuser("User:superman")
                .addAcl(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, WRITE, DENY))
                .addAcl(new StandardAcl(GROUP, "foo_bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, READ, ALLOW))
                .addAcl(new StandardAcl(GROUP, "foo_", PREFIXED, WILDCARD_PRINCIPAL, WILDCARD, WRITE, ALLOW));

        lst.add(ctxt.getTest(ALLOWED, "authorize with PREFIXED access", AuthorizerTestUtils.newAction(READ, TOPIC, "foo_bar")));
        lst.add(ctxt.getTest(ALLOWED, "authorize with PREFIXED access to exact resource name", AuthorizerTestUtils.newAction(READ, TOPIC, "foo_")));

        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "fred"));
        lst.add(ctxt.getTest(ALLOWED, "authorize with WILDCARD_PRINCIPAL access to exact resource name", AuthorizerTestUtils.newAction(READ, GROUP, "foo_bar")));
        lst.add(ctxt.getTest(ALLOWED, "authorize with WILDCARD_PRINCIPAL access to PREFIXED resource name", AuthorizerTestUtils.newAction(WRITE, GROUP, "foo_thing")));


        /* test Deny Precedence */
        ctxt.reset();
        ctxt.reqBuilder.setPrincipal(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        ctxt.dataBuilder.addSuperuser("User:superman")
                .addAcl(new StandardAcl(TOPIC, "foobar", LITERAL, "User:alice", WILDCARD, READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "foo", PREFIXED, WILDCARD_PRINCIPAL, WILDCARD, READ, DENY));
        lst.add(ctxt.getTest(DENIED, "authorize overridden by PREFIXED deny", AuthorizerTestUtils.newAction(READ, TOPIC, "foobar")));
        lst.add(ctxt.getTest(DENIED, "authorize denied PREFIXED deny on resource match", AuthorizerTestUtils.newAction(READ, TOPIC, "foo")));

        ctxt.dataBuilder.addSuperuser("User:superman")
                .addAcl(new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", WILDCARD, ALL, ALLOW));
        lst.add(ctxt.getTest(DENIED, "not overridden by LITERAL ALL ALLOW", AuthorizerTestUtils.newAction(READ, TOPIC, "foo")));
        lst.add(ctxt.getTest(DENIED, "not overridden by LITERAL ALL ALLOW", AuthorizerTestUtils.newAction(READ, TOPIC, "foobar")));
        lst.add(ctxt.getTest(ALLOWED, "WRITE by LITERAL ALL ALLOW with PREFIXED DENY READ present", AuthorizerTestUtils.newAction(WRITE, TOPIC, "foo")));

        return lst;
    }


    DynamicTest getTest(AuthorizationResult result, String title, Builder builder, KafkaPrincipal principal, String host, AclOperation operation, ResourceType resourceType) throws Exception {
        final Builder dataBuilder = builder.clone();
        String dyntitle = format("%s: %s: %s, %s, %s, %s", result, title, principal, host, operation, resourceType);
        return DynamicTest.dynamicTest(dyntitle, () -> {
            boolean anyOrUnknown = operation == AclOperation.ANY || operation == AclOperation.UNKNOWN || resourceType == ResourceType.ANY || resourceType == ResourceType.UNKNOWN;
            if (anyOrUnknown) {
                assertThrows(IllegalArgumentException.class, () -> dataBuilder.get().authorizeByResourceType(principal, host, operation, resourceType));
            } else {
                assertEquals(result, dataBuilder.get().authorizeByResourceType(principal, host, operation, resourceType), () -> format("%s: %s, %s, %s, %s", title, principal, host, operation, resourceType));
            }
        });
    }


    @TestFactory
    Collection<DynamicTest> authorizeByResourceTypeTests() throws Exception {
        List<DynamicTest> lst = new ArrayList<>();

        Builder builder = new Builder().addSuperuser("User:superman");
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");

        /** no ACLS in authorizer */
        for (AclOperation operation : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                lst.add(getTest(DENIED, "No ACLS, default not set", builder, principal, null, operation, type));
            }
        }

        builder.setDefaultResult(ALLOWED);
        for (AclOperation operation : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                lst.add(getTest(ALLOWED, "No ACLS, default ALLOWED", builder, principal, null, operation, type));
            }
        }

        builder.setDefaultResult(DENIED);
        for (AclOperation operation : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                lst.add(getTest(DENIED, "No ACLS, default DENIED", builder, principal, null, operation, type));
            }
        }

        /* with ACLs */


        builder.clearAcls();
        builder.addAcl(new StandardAcl(TOPIC, "topic", LITERAL, "User:alice", "localhost", READ, ALLOW));
        for (String host : Arrays.asList(null, "localhost", "example.com")) {
            boolean incorrectHost = "example.com".equals(host);
            for (AclOperation operation : Arrays.asList(AclOperation.ANY, AclOperation.READ, AclOperation.WRITE)) {
                boolean incorrectOperation = operation == WRITE;
                for (ResourceType type : Arrays.asList(ResourceType.ANY, ResourceType.TOPIC, ResourceType.GROUP)) {
                    boolean incorrectType = type == GROUP;
                    AuthorizationResult expected = incorrectHost || incorrectOperation || incorrectType ? DENIED : ALLOWED;
                    String title = format("%s operation, %s type, %s host", incorrectOperation ? "wrong" : "specified",
                            incorrectType ? "wrong" : "specified", incorrectHost ? "wrong" : "specified");
                    lst.add(getTest(expected, title, builder, principal, host, operation, type));
                }
            }
        }

        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        for (String host : Arrays.asList(null, "localhost", "example.com")) {
            boolean incorrectHost = "example.com".equals(host);
            for (AclOperation operation : Arrays.asList(AclOperation.ANY, AclOperation.READ, AclOperation.WRITE)) {
                boolean incorrectOperation = operation == WRITE;
                for (ResourceType type : Arrays.asList(ResourceType.ANY, ResourceType.TOPIC, ResourceType.GROUP)) {
                    boolean incorrectType = type == GROUP;
                    String title = format("wrong user: %s operation, %s type, %s host", incorrectOperation ? "wrong" : "specified",
                            incorrectType ? "wrong" : "specified", incorrectHost ? "wrong" : "specified");
                    lst.add(getTest(DENIED, title, builder, principal, host, operation, type));
                }
            }
        }

        /* wildcard host */
        builder.clearAcls();
        builder.addAcl(new StandardAcl(TOPIC, "topic", LITERAL, "User:alice", "*", READ, ALLOW));
        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        lst.add(getTest(ALLOWED, "Wildcard ACL host", builder, principal, "localhost", READ, TOPIC));

        /* wildcard USER principal */
        builder.clearAcls();
        builder.addAcl(new StandardAcl(TOPIC, "topic", LITERAL, WILDCARD_PRINCIPAL, "localhost", READ, ALLOW));
        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        lst.add(getTest(ALLOWED, "Wildcard ACL USER principal", builder, principal, "localhost", READ, TOPIC));

        builder.clearAcls();
        builder.addAcl(new StandardAcl(TOPIC, "topic", LITERAL, WILDCARD, "localhost", READ, ALLOW));
        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        lst.add(getTest(ALLOWED, "Wildcard ACL principal", builder, principal, "localhost", READ, TOPIC));
        return lst;
    }
}
