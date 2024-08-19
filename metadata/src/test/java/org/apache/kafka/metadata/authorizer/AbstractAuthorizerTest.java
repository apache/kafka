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

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
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
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
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
import static org.apache.kafka.common.resource.PatternType.ANY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.metadata.authorizer.AuthorizerData.findResult;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.SUPER_USERS_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.getConfiguredSuperUsers;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.getDefaultResult;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD_PRINCIPAL;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public abstract class AbstractAuthorizerTest<T extends Authorizer> {
    interface TestingWrapper<T extends Authorizer> {
        /**
         * Gets an instance of the Authorizer under test.
         * Performs the configuration, starts the authorizer with a PLAINTEXT endpoint,
         * Adds the ACLs and performs any other authorizer specific setup.
         *
         * @return an instance of the Authorizer under test.
         */
        T getAuthorizer();

        /**
         * Gets an unconfigured instance of the Authorizer under test.
         *
         * @return an instance of the Authorizer under test.
         */
        T getUnconfiguredAuthorizer();

        /**
         * Applies the configuration to the Authorizer.
         *
         * @param authorizer the authorizer to configure
         */
        T configure(T authorizer);

        /**
         * Adds the specified ACLs to the Authorizer.
         *
         * @param authorizer the Authorizer to add the ACLs to.
         */
        T addAcls(T authorizer);

        /**
         * Gets the superusers as defiend within the Authorizer
         *
         * @param authorizer the Authorizer under test.
         * @return the set of Superusers from the Authorizer.
         */
        Set<String> superUsers(T authorizer);

        /**
         * Gets the default Authorizer result as defined in the Authorizer.
         *
         * @param authorizer the Authorizer under test.
         * @return the default Authorizer result as defined in the Authorizer.
         */
        AuthorizationResult defaultResult(T authorizer);
    }

    abstract class Builder implements Supplier<TestingWrapper<T>> {
        private final List<StandardAclWithId> acls = new ArrayList<>();
        private final Map<String, Object> configs = new HashMap<>();

        protected void  applyConfigs(Consumer<Map<String, ?>> consumer) {
            consumer.accept(Collections.unmodifiableMap(configs));
        }

        protected  void applyAcls(BiConsumer<Uuid, StandardAcl> consumer) {
            acls.forEach( a -> consumer.accept(a.id(), a.acl()));
        }

        public final Builder addAcls(Stream<StandardAcl> acls) {
            acls.forEach(this::addAcl);
            return this;
        }

        public final Builder addAcl(StandardAcl acl) {
            acls.add(withId(acl));
            return this;
        }

        public final Builder superUser(String superUser) {
            String value = (String) configs.get(SUPER_USERS_CONFIG);
            if (value == null) {
                configs.put(SUPER_USERS_CONFIG, superUser);
            } else {
                configs.put(SUPER_USERS_CONFIG, format("%s;%s", value, superUser));
            }
            return this;
        }

        public final Builder config(String key, Object value) {
            configs.put(key, value);
            return this;
        }
    }

    protected abstract Builder getTestingWrapperBuilder();

    public static final Endpoint PLAINTEXT = new Endpoint("PLAINTEXT",
            SecurityProtocol.PLAINTEXT,
            "127.0.0.1",
            9020);

    public static final Endpoint CONTROLLER = new Endpoint("CONTROLLER",
            SecurityProtocol.PLAINTEXT,
            "127.0.0.1",
            9020);

    public static class AuthorizerTestServerInfo implements AuthorizerServerInfo {
        private final Collection<Endpoint> endpoints;

        public AuthorizerTestServerInfo(Collection<Endpoint> endpoints) {
            assertFalse(endpoints.isEmpty());
            this.endpoints = endpoints;
        }

        @Override
        public ClusterResource clusterResource() {
            return new ClusterResource(Uuid.fromString("r7mqHQrxTNmzbKvCvWZzLQ").toString());
        }

        @Override
        public int brokerId() {
            return 0;
        }

        @Override
        public Collection<Endpoint> endpoints() {
            return endpoints;
        }

        @Override
        public Endpoint interBrokerEndpoint() {
            return endpoints.iterator().next();
        }

        @Override
        public Collection<String> earlyStartListeners() {
            List<String> result = new ArrayList<>();
            for (Endpoint endpoint : endpoints) {
                if (endpoint.listenerName().get().equals("CONTROLLER")) {
                    result.add(endpoint.listenerName().get());
                }
            }
            return result;
        }
    }

    @Test
    public final void testGetConfiguredSuperUsers() {
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
    public final void testGetDefaultResult() {
        assertEquals(DENIED, getDefaultResult(Collections.emptyMap()));
        assertEquals(ALLOWED, getDefaultResult(Collections.singletonMap(
                ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true")));
        assertEquals(DENIED, getDefaultResult(Collections.singletonMap(
                ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false")));
    }

    @ParameterizedTest
    @MethodSource("configureData")
    public final void testConfigure(List<String> superUsers, Boolean allowAll, AuthorizationResult expected) {
        Builder builder = getTestingWrapperBuilder();
        for (String name : superUsers) {
            builder.superUser(name);
        }
        if (allowAll != null) {
            builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, Boolean.toString(allowAll));
        }
        TestingWrapper<T> wrapper = builder.get();
        T authorizer = wrapper.getAuthorizer();
        assertEquals(new HashSet<>(superUsers), wrapper.superUsers(authorizer));
        assertEquals(expected, wrapper.defaultResult(authorizer));
    }

    private static Stream<Arguments> configureData() {
        List<Arguments> lst = new ArrayList<>();
        lst.add(Arguments.of(Arrays.asList("User:bob"), null, DENIED));
        lst.add(Arguments.of(Arrays.asList("User:bob"), false, DENIED));
        lst.add(Arguments.of(Arrays.asList("User:bob"), true, ALLOWED));
        lst.add(Arguments.of(Arrays.asList("User:bob", "User:alice"), true, ALLOWED));
        lst.add(Arguments.of(Arrays.asList("Group:circus"), true, ALLOWED));
        return lst.stream();
    }

    static Action newAction(AclOperation aclOperation,
                            ResourceType resourceType,
                            String resourceName) {
        return new Action(aclOperation,
                new ResourcePattern(resourceType, resourceName, LITERAL), 1, false, false);
    }

    static StandardAclWithId withId(StandardAcl acl) {
        return new StandardAclWithId(new Uuid(acl.hashCode(), acl.hashCode()), acl);
    }

    @Test
    public final void testFindResultImplication() throws Exception {
        AuthorizableRequestContext ctxt = newRequestContext("bob");
        // These permissions all imply DESCRIBE.
        for (AclOperation op : asList(DESCRIBE, READ, WRITE, DELETE, ALTER)) {
            assertEquals(ALLOWED, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                    ctxt,
                    new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, op, ALLOW)));
        }
        // CREATE does not imply DESCRIBE
        assertNull(findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                ctxt,
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, CREATE, ALLOW)));
        // Deny ACLs don't do "implication".
        for (AclOperation op : asList(READ, WRITE, DELETE, ALTER)) {
            assertNull(findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                    ctxt,
                    new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, op, DENY)));
        }
        // Exact match
        assertEquals(DENIED, findResult(newAction(DESCRIBE, TOPIC, "foo_bar"),
                ctxt,
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, DESCRIBE, DENY)));
        // These permissions all imply DESCRIBE_CONFIGS.
        for (AclOperation op : asList(DESCRIBE_CONFIGS, ALTER_CONFIGS)) {
            assertEquals(ALLOWED, findResult(newAction(DESCRIBE_CONFIGS, TOPIC, "foo_bar"),
                    ctxt,
                    new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, op, ALLOW)));
        }
        // Deny ACLs don't do "implication".
        assertNull(findResult(newAction(DESCRIBE_CONFIGS, TOPIC, "foo_bar"),
                ctxt,
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, ALTER_CONFIGS, DENY)));
        // Exact match
        assertEquals(DENIED, findResult(newAction(ALTER_CONFIGS, TOPIC, "foo_bar"),
                ctxt,
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, ALTER_CONFIGS, DENY)));
    }

    @Test
    public final void testFindResultPrincipalMatching() throws Exception {
        assertEquals(ALLOWED, findResult(newAction(READ, TOPIC, "foo_bar"),
                newRequestContext("bob"),
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, READ, ALLOW)));
        // Principal does not match.
        assertNull(findResult(newAction(READ, TOPIC, "foo_bar"),
                newRequestContext("alice"),
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, READ, ALLOW)));
        // Wildcard principal matches anything.
        assertEquals(DENIED, findResult(newAction(READ, GROUP, "bar"),
                newRequestContext("alice"),
                new StandardAcl(GROUP, "bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, READ, DENY)));
    }

    protected static void assertContains(Iterable<AclBinding> iterable, StandardAcl... acls) {
        Iterator<AclBinding> iterator = iterable.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            AclBinding acl = iterator.next();
            assertTrue(i < acls.length, "Only expected " + i + " element(s)");
            assertEquals(acls[i].toBinding(), acl, "Unexpected element " + i);
        }
        assertFalse(iterator.hasNext(), "Expected only " + acls.length + " element(s)");
    }

    protected AuthorizableRequestContext newRequestContext(String principal) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
                .build();
    }

    protected AuthorizableRequestContext newRequestContext(String principal, InetAddress clientAddress) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
                .setClientAddress(clientAddress)
                .build();
    }


    /**
     * Test that ClusterMetadataAuthorizer#start returns a completed future for early start
     * listeners.
     */
    @Test
    public final void testStartWithEarlyStartListeners() {
        TestingWrapper<T> wrapper = getTestingWrapperBuilder().superUser("User:superman").get();
        T authorizer = wrapper.configure(wrapper.getUnconfiguredAuthorizer());
        Map<Endpoint, ? extends CompletionStage<Void>> futures2 = authorizer.start(new AuthorizerTestServerInfo(Arrays.asList(PLAINTEXT, CONTROLLER)));
        assertEquals(new HashSet<>(Arrays.asList(PLAINTEXT, CONTROLLER)), futures2.keySet());
        assertFalse(futures2.get(PLAINTEXT).toCompletableFuture().isDone());
        assertTrue(futures2.get(CONTROLLER).toCompletableFuture().isDone());
    }

    private void execAuthorizeByResourceType(Supplier<String> name, Authorizer authorizer, AuthorizableRequestContext requestContext, AclOperation operation, ResourceType resourceType, AuthorizationResult expected) {
        String fmt = "%s [Op:  %s, Type: %s, ctxt: %s, %s] failed";
        assertEquals(expected, authorizer.authorizeByResourceType(requestContext, operation, resourceType), () -> format(fmt, name.get(), operation, resourceType, requestContext.principal(), requestContext.clientAddress()));
    }

    @Test
    public final void testAuthorizeByResourceTypeNoAcls() throws Exception {

        Builder builder = getTestingWrapperBuilder();

        T authorizer = builder.get().getAuthorizer();
        AuthorizableRequestContext requestContext = newRequestContext("User:alice");

        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default not set, %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, DENIED);
                }
            }
        }

        builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false");

        authorizer = builder.get().getAuthorizer();
        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default 'false'', %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, DENIED);
                }
            }
        }

        builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        authorizer = builder.get().getAuthorizer();
        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default 'true'', %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, ALLOWED);
                }
            }
        }
    }

    @Test
    public final void testAuthorizeByResourceType() throws Exception {

        Builder builder = getTestingWrapperBuilder().superUser("User:superman")
                .addAcl(new StandardAcl(TOPIC, "foo", PREFIXED, "User:alice", WILDCARD, READ, DENY))
                .addAcl(new StandardAcl(TOPIC, "foobar", LITERAL, "User:alice", WILDCARD, READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "foo", PREFIXED, "User:bob", WILDCARD, READ, ALLOW));

        T authorizer = builder.get().getAuthorizer();
        AuthorizableRequestContext requestContext = newRequestContext("alice");

        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default not set, %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, DENIED);
                }
            }
        }

        builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false");

        authorizer = builder.get().getAuthorizer();
        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default 'false'', %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, DENIED);
                }
            }
        }

        builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        authorizer = builder.get().getAuthorizer();
        for (AclOperation op : AclOperation.values()) {
            for (ResourceType type : ResourceType.values()) {
                boolean anyOrUnknown = op == AclOperation.ANY | op == AclOperation.UNKNOWN | type == ResourceType.ANY || type == ResourceType.UNKNOWN;
                Supplier<String> name = () -> format("No ACLs, default 'true'', %s, %s", op, type);
                if (anyOrUnknown) {
                    final T auth = authorizer;
                    assertThrows(IllegalArgumentException.class, () -> auth.authorizeByResourceType(requestContext, op, type), name);
                } else {
                    execAuthorizeByResourceType(name, authorizer, requestContext, op, type, ALLOWED);
                }
            }
        }
    }

    public void execAuthorize(Supplier<String> name, Authorizer authorizer, AuthorizableRequestContext requestContext, Action action, AuthorizationResult expected) {
        String fmt = "%s [Action: %s, ctxt: %s, %s] failed";
        List<AuthorizationResult> lst = authorizer.authorize(requestContext, Arrays.asList(action));
        assertEquals(1, lst.size(), () -> format("Wrong result count (%s) for %s", lst.size(), name.get()));
        assertEquals(expected, lst.get(0), () -> format(fmt, name.get(), action, requestContext.principal(), requestContext.clientAddress()));
    }

    @Test
    public void testDenyPrecedenceWithOperationAll() throws Exception {
        Builder builder = getTestingWrapperBuilder().superUser("User:superman");

        List<StandardAcl> acls = Arrays.asList(
                new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", "*", ALL, DENY),
                new StandardAcl(TOPIC, "foo", PREFIXED, "User:alice", "*", READ, ALLOW),
                new StandardAcl(TOPIC, "foo", LITERAL, "User:*", "*", ALL, DENY),
                new StandardAcl(TOPIC, "foo", PREFIXED, "User:*", "*", DESCRIBE, ALLOW)
        );
        builder.addAcls(acls.stream());
        T authorizer = builder.get().getAuthorizer();

        AuthorizableRequestContext ctxt = newRequestContext("alice");
        execAuthorize(() -> "Test DENY precedence with operation all : alice write", authorizer, ctxt, newAction(WRITE, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : alice read", authorizer, ctxt, newAction(READ, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : alice describe", authorizer, ctxt, newAction(DESCRIBE, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : alice read foobar", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), ALLOWED);

        ctxt = newRequestContext("bob");
        execAuthorize(() -> "Test DENY precedence with operation all : bob describe", authorizer, ctxt, newAction(DESCRIBE, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : bob read", authorizer, ctxt, newAction(READ, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : bob write", authorizer, ctxt, newAction(WRITE, TOPIC, "foo"), DENIED);
        execAuthorize(() -> "Test DENY precedence with operation all : bob describe foobaz", authorizer, ctxt, newAction(DESCRIBE, TOPIC, "foobaz"), ALLOWED);
        execAuthorize(() -> "Test DENY precedence with operation all : bob read foobaz", authorizer, ctxt, newAction(READ, TOPIC, "foobaz"), DENIED);

        // same in groups
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
    public void testNoAcls() throws Exception {

        Builder builder = getTestingWrapperBuilder().superUser("User:superman").config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        T authorizer = builder.get().getAuthorizer();

        AuthorizableRequestContext bob = newRequestContext("bob");
        execAuthorize(() -> ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG + "=true, No ACLs, bob", authorizer, bob, newAction(READ, TOPIC, "topic1"), ALLOWED);

        AuthorizableRequestContext superuser = newRequestContext("superman");
        execAuthorize(() -> ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG + "=true, No ACLs, superuser", authorizer, superuser, newAction(READ, TOPIC, "topic1"), ALLOWED);

        authorizer = builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false").get().getAuthorizer();

        execAuthorize(() -> ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG + "=false, No ACLs, bob", authorizer, bob, newAction(READ, TOPIC, "topic2"), DENIED);

        execAuthorize(() -> ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG + "=false, No ACLs, superuser", authorizer, superuser, newAction(READ, TOPIC, "topic2"), ALLOWED);

    }

    protected Builder addManyAcls(Builder builder) {
        return builder.addAcl(new StandardAcl(TOPIC, "green2", LITERAL, "User:*", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "green", PREFIXED, "User:bob", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "betamax4", LITERAL, "User:bob", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "betamax", LITERAL, "User:bob", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "beta", PREFIXED, "User:*", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "alpha", PREFIXED, "User:*", "*", READ, ALLOW))
                .addAcl(new StandardAcl(TOPIC, "alp", PREFIXED, "User:bob", "*", READ, DENY))
                .addAcl(new StandardAcl(GROUP, "*", LITERAL, "User:bob", "*", WRITE, ALLOW))
                .addAcl(new StandardAcl(GROUP, "wheel", LITERAL, "User:*", "*", WRITE, DENY));
    }

    @Test
    public void testListAcls() throws Exception {
        StandardAcl fooRead = new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, READ, ALLOW);
        StandardAcl fooWrite = new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, WRITE, ALLOW);
        StandardAcl barDescribe = new StandardAcl(GROUP, "bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, DESCRIBE_CONFIGS, DENY);
        StandardAcl barAlter = new StandardAcl(GROUP, "bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, ALTER_CONFIGS, DENY);

        Authorizer authorizer = getTestingWrapperBuilder().superUser("User:superman")
                .addAcl(fooRead).addAcl(fooWrite).addAcl(barDescribe).addAcl(barAlter).get().getAuthorizer();


        assertContains(authorizer.acls(AclBindingFilter.ANY),
                fooRead, fooWrite, barDescribe, barAlter);

        assertContains(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
                        TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY)),
                fooRead, fooWrite);

        assertFalse(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
                TOPIC, null, LITERAL), AccessControlEntryFilter.ANY)).iterator().hasNext());

        assertContains(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
                        ResourceType.ANY, "bar", ANY), AccessControlEntryFilter.ANY)),
                barDescribe, barAlter);

        assertFalse(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
                ResourceType.ANY, "bar", PREFIXED), AccessControlEntryFilter.ANY)).iterator().hasNext());

        assertFalse(authorizer.acls(new AclBindingFilter(new ResourcePatternFilter(
                        ResourceType.GROUP, "bar", PREFIXED), AccessControlEntryFilter.ANY)).iterator().hasNext());

        // named user matches wildcard principal
        assertContains(authorizer.acls(new AclBindingFilter(ResourcePatternFilter.ANY, new AccessControlEntryFilter("User:bob", WILDCARD, DESCRIBE_CONFIGS, DENY))),
                barDescribe);

        // named host matches wildcard host
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        assertContains(authorizer.acls(new AclBindingFilter(ResourcePatternFilter.ANY, new AccessControlEntryFilter(WILDCARD_PRINCIPAL, host1.getHostName(), DESCRIBE_CONFIGS, DENY))),
                barDescribe);

    }

    @Test
    public void testAllowEveryoneIfNoAclFoundConfigEnabled() throws Exception {
        Builder builder = getTestingWrapperBuilder().superUser("User:alice;User:chris")
                .addAcl(new StandardAcl(TOPIC, "topic1", LITERAL, "User:Alice", WILDCARD, READ, ALLOW))
                .config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");
        T authorizer = builder.get().getAuthorizer();
        AuthorizableRequestContext ctxt = newRequestContext("Bob");

        execAuthorize(() -> "testAllowEveryoneIfNoAclFoundConfigEnabled - other ACLs for topic 1 exist", authorizer, ctxt, newAction(READ, TOPIC, "topic1"), DENIED);
        execAuthorize(() -> "testAllowEveryoneIfNoAclFoundConfigEnabled", authorizer, ctxt, newAction(READ, TOPIC, "topic2"), ALLOWED);

        authorizer = builder.addAcl(new StandardAcl(TOPIC, "top", PREFIXED, "User:Alice", WILDCARD, READ, ALLOW)).get().getAuthorizer();
        execAuthorize(() -> "testAllowEveryoneIfNoAclFoundConfigEnabled - with prefixed ACL for 'top'", authorizer, ctxt, newAction(READ, TOPIC, "topic3"), DENIED);
    }

    @Test
    public void testAllowEveryoneIfNoAclFoundConfigDisabled() throws Exception {
        T authorizer = getTestingWrapperBuilder().superUser("User:alice;User:chris")
                .addAcl(new StandardAcl(TOPIC, "topic1", LITERAL, "User:Alice", WILDCARD, READ, ALLOW))
                .config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false").get().getAuthorizer();
        AuthorizableRequestContext ctxt = newRequestContext("Bob");

        execAuthorize(() -> "testAllowEveryoneIfNoAclFoundConfigEnabled", authorizer, ctxt, newAction(READ, TOPIC, "topic1"), DENIED);
        execAuthorize(() -> "testAllowEveryoneIfNoAclFoundConfigEnabled", authorizer, ctxt, newAction(READ, TOPIC, "topic2"), DENIED);
    }

    @Test
    public void testConfigure() {
        TestingWrapper<T> wrapper = getTestingWrapperBuilder().superUser("User:alice;User:chris")
                .addAcl(new StandardAcl(TOPIC, "topic1", LITERAL, "User:Alice", WILDCARD, READ, ALLOW))
                .config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true").get();
        T authorizer = wrapper.getAuthorizer();
        assertEquals(new HashSet<>(asList("User:alice", "User:chris")), wrapper.superUsers(authorizer));
        assertEquals(ALLOWED, wrapper.defaultResult(authorizer));
    }

    @Test
    public void testSimpleAuthorizations() throws Exception {
        List<StandardAcl> acls = asList(
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, READ, ALLOW),
                new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, WRITE, ALLOW),
                new StandardAcl(GROUP, "bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, DESCRIBE_CONFIGS, ALLOW),
                new StandardAcl(GROUP, "bar", LITERAL, WILDCARD_PRINCIPAL, WILDCARD, ALTER_CONFIGS, ALLOW));
        T authorizer = getTestingWrapperBuilder().superUser("User:superman").addAcls(acls.stream()).get().getAuthorizer();

        AuthorizableRequestContext ctxt = newRequestContext("bob");
        execAuthorize(() -> "testSimpleAuthorizations", authorizer, ctxt, newAction(READ, TOPIC, "foo_"), ALLOWED);

        ctxt = newRequestContext("fred");
        execAuthorize(() -> "testSimpleAuthorizations", authorizer, ctxt, newAction(ALTER_CONFIGS, GROUP, "bar"), ALLOWED);
    }

    @Test
    public void testTopicAclWithOperationAll() throws Exception {
        List<StandardAcl> acls = Arrays.asList(
                new StandardAcl(TOPIC, "foo", LITERAL, "User:*", "*", ALL, ALLOW),
                new StandardAcl(TOPIC, "bar", PREFIXED, "User:alice", "*", ALL, ALLOW),
                new StandardAcl(TOPIC, "baz", LITERAL, "User:bob", "*", ALL, ALLOW)
        );

        T authorizer = getTestingWrapperBuilder().superUser("User:superman").addAcls(acls.stream()).get().getAuthorizer();


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

    @Test
    public void testHostAddressAclValidation() throws Exception {
        InetAddress host1 = InetAddress.getByName("192.168.1.1");
        InetAddress host2 = InetAddress.getByName("192.168.1.2");

        List<StandardAcl> acls = Arrays.asList(
                new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", host1.getHostAddress(), READ, DENY),
                new StandardAcl(TOPIC, "foo", LITERAL, "User:alice", "*", READ, ALLOW),
                new StandardAcl(TOPIC, "bar", LITERAL, "User:bob", host2.getHostAddress(), READ, ALLOW),
                new StandardAcl(TOPIC, "bar", LITERAL, "User:*", InetAddress.getLocalHost().getHostAddress(), DESCRIBE, ALLOW)
        );
        T authorizer = getTestingWrapperBuilder().superUser("User:superman").addAcls(acls.stream()).get().getAuthorizer();

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

    @Test
    public void testAuthorizeWithPrefix() throws Exception {
        Builder builder = getTestingWrapperBuilder().superUser("User:superman")
                .addAcl(new StandardAcl(TOPIC, "foo", PREFIXED, "User:alice", WILDCARD, READ, DENY))
                .addAcl(new StandardAcl(TOPIC, "foobar", LITERAL, "User:alice", WILDCARD, READ, ALLOW));
        T authorizer = builder.get().getAuthorizer();

        AuthorizableRequestContext ctxt = newRequestContext("alice");
        execAuthorize(() -> "noAcl=false, prefix overrides matching literal", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), DENIED);
        execAuthorize(() -> "noAcl=false, prefix override nothing", authorizer, ctxt, newAction(READ, TOPIC, "foob"), DENIED);

        authorizer = builder.config(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true").get().getAuthorizer();
        execAuthorize(() -> "noAcl=true, prefix overrides matching literal", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), DENIED);
        execAuthorize(() -> "noAcl=true, prefix override nothing", authorizer, ctxt, newAction(READ, TOPIC, "foob"), DENIED);

        ctxt = newRequestContext("bob");
        execAuthorize(() -> "noAcl=true, no matching ACLs", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), DENIED);

        authorizer = builder
                .addAcl(new StandardAcl(TOPIC, "foobar", LITERAL, "User:bob", WILDCARD, READ, ALLOW)).get().getAuthorizer();
        execAuthorize(() -> "noAcl=true, literal allow", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), ALLOWED);

        authorizer = builder.addAcl(new StandardAcl(TOPIC, "foobar", PREFIXED, "User:bob", WILDCARD, READ, DENY)).get().getAuthorizer();
        execAuthorize(() -> "noAcl=true, prefix deny and literal allow at same level", authorizer, ctxt, newAction(READ, TOPIC, "foobar"), DENIED);
    }
    @Test
    public void testAuthorizationWithManyAcls() throws Exception {

        T authorizer = addManyAcls(getTestingWrapperBuilder().superUser("User:superman")).get().getAuthorizer();
        AuthorizableRequestContext ctxt = newRequestContext("bob");

        execAuthorize(() -> "Test authorize with many ACLs : bob read green1", authorizer, ctxt, newAction(READ, TOPIC, "green1"), ALLOWED);
        execAuthorize(() -> "Test authorize with many ACLs : bob write wheel", authorizer, ctxt, newAction(WRITE, TOPIC, "wheel"), DENIED);
        execAuthorize(() -> "Test authorize with many ACLs : bob read alpha", authorizer, ctxt, newAction(READ, TOPIC, "alpha"), DENIED);
        execAuthorize(() -> "Test authorize with many ACLs : bob write arbitrary", authorizer, ctxt, newAction(WRITE, GROUP, "arbitrary"), ALLOWED);
        execAuthorize(() -> "Test authorize with many ACLs : bob read ala", authorizer, ctxt, newAction(READ, TOPIC, "ala"), DENIED);

        // the same set in groups.

        assertEquals(Arrays.asList(ALLOWED, DENIED),
                authorizer.authorize(ctxt,
                        Arrays.asList(newAction(READ, TOPIC, "green1"),
                                newAction(WRITE, GROUP, "wheel"))));
        assertEquals(Arrays.asList(DENIED, ALLOWED, DENIED),
                authorizer.authorize(ctxt,
                        Arrays.asList(newAction(READ, TOPIC, "alpha"),
                                newAction(WRITE, GROUP, "arbitrary"),
                                newAction(READ, TOPIC, "ala"))));
    }

    @Test
    public void testPrefixAcls() throws Exception {
        List<StandardAcl> acls = Arrays.asList(
                new StandardAcl(TOPIC, "fooa", PREFIXED, "User:alice", "*", ALL, ALLOW),
                new StandardAcl(TOPIC, "foobar", LITERAL, "User:bob", "*", ALL, ALLOW),
                new StandardAcl(TOPIC, "f", PREFIXED, "User:bob", "*", ALL, ALLOW)
        );
        T authorizer = getTestingWrapperBuilder().superUser("User:superman").addAcls(acls.stream()).get().getAuthorizer();

        assertEquals(Arrays.asList(ALLOWED, DENIED, ALLOWED), authorizer.authorize(
                newRequestContext("bob"),
                Arrays.asList(
                        newAction(WRITE, TOPIC, "foobarr"),
                        newAction(READ, TOPIC, "goobar"),
                        newAction(READ, TOPIC, "fooa"))));

        assertEquals(Arrays.asList(ALLOWED, DENIED, DENIED), authorizer.authorize(
                newRequestContext("alice"),
                Arrays.asList(
                        newAction(DESCRIBE, TOPIC, "fooa"),
                        newAction(WRITE, TOPIC, "bar"),
                        newAction(READ, TOPIC, "baz"))));
    }

}
