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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ClusterMetadataAuthorizerTest {
    static class MockAclMutator implements AclMutator {
        private CompletableFuture<List<AclCreateResult>> createAclsResponse;
        private CompletableFuture<List<AclDeleteResult>> deleteAclsResponse;

        void setCreateAclsResponse(CompletableFuture<List<AclCreateResult>> createAclsResponse) {
            this.createAclsResponse = createAclsResponse;
        }

        @Override
        public CompletableFuture<List<AclCreateResult>> createAcls(List<AclBinding> aclBindings) {
            return createAclsResponse;
        }

        void setDeleteAclsResponse(CompletableFuture<List<AclDeleteResult>> deleteAclsResponse) {
            this.deleteAclsResponse = deleteAclsResponse;
        }

        @Override
        public CompletableFuture<List<AclDeleteResult>> deleteAcls(List<AclBindingFilter> aclBindingFilters) {
            return deleteAclsResponse;
        }
    }

    static class MockClusterMetadataAuthorizer implements ClusterMetadataAuthorizer {
        volatile AclMutator aclMutator;

        @Override
        public void setAclMutator(AclMutator aclMutator) {
            this.aclMutator = aclMutator;
        }

        @Override
        public AclMutator aclMutatorOrException() {
            if (aclMutator == null) {
                throw new NotControllerException("The current node is not the active controller.");
            }
            return aclMutator;
        }

        @Override
        public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
            // do nothing
        }

        @Override
        public void addAcl(Uuid id, StandardAcl acl) {
            // do nothing
        }

        @Override
        public void removeAcl(Uuid id) {
            // do nothing
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return null; // do nothing
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            return null; // do nothing
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return null; // do nothing
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // do nothing
        }
    }

    static final List<AclBinding> TEST_BINDINGS = Arrays.asList(
        new AclBinding(new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL),
            new AccessControlEntry(WILDCARD_PRINCIPAL, WILDCARD, READ, ALLOW)),
        new AclBinding(new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL),
            new AccessControlEntry(WILDCARD_PRINCIPAL, WILDCARD, WRITE, ALLOW))
    );

    static final List<AclBindingFilter> TEST_FILTERS = TEST_BINDINGS.stream().
        map(b -> b.toFilter()).collect(Collectors.toList());

    @Test
    public void testCreateAcls() throws Exception {
        MockAclMutator mutator = new MockAclMutator();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.setAclMutator(mutator);
        CompletableFuture<List<AclCreateResult>> response = new CompletableFuture<>();
        response.complete(Arrays.asList(AclCreateResult.SUCCESS,
            new AclCreateResult(new InvalidRequestException("invalid"))));
        mutator.setCreateAclsResponse(response);
        List<? extends CompletionStage<AclCreateResult>> results = authorizer.createAcls(
            new MockAuthorizableRequestContext.Builder().build(), TEST_BINDINGS);
        assertEquals(2, results.size());
        assertEquals(Optional.empty(), results.get(0).toCompletableFuture().get().exception());
        assertEquals(InvalidRequestException.class,
            results.get(1).toCompletableFuture().get().exception().get().getClass());
    }

    @Test
    public void testCreateAclsError() throws Exception {
        MockAclMutator mutator = new MockAclMutator();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.setAclMutator(mutator);
        CompletableFuture<List<AclCreateResult>> response = new CompletableFuture<>();
        response.completeExceptionally(new AuthorizationException("not authorized"));
        mutator.setCreateAclsResponse(response);
        List<? extends CompletionStage<AclCreateResult>> results = authorizer.createAcls(
            new MockAuthorizableRequestContext.Builder().build(), TEST_BINDINGS);
        assertEquals(2, results.size());
        assertEquals(AuthorizationException.class,
            results.get(0).toCompletableFuture().get().exception().get().getClass());
        assertEquals(AuthorizationException.class,
            results.get(1).toCompletableFuture().get().exception().get().getClass());
    }

    @Test
    public void testDeleteAcls() throws Exception {
        MockAclMutator mutator = new MockAclMutator();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.setAclMutator(mutator);
        CompletableFuture<List<AclDeleteResult>> response = new CompletableFuture<>();
        response.complete(Arrays.asList(new AclDeleteResult(
                Collections.singleton(new AclBindingDeleteResult(TEST_BINDINGS.get(0)))),
            new AclDeleteResult(new InvalidRequestException("invalid"))));
        mutator.setDeleteAclsResponse(response);
        List<? extends CompletionStage<AclDeleteResult>> results = authorizer.deleteAcls(
            new MockAuthorizableRequestContext.Builder().build(), TEST_FILTERS);
        assertEquals(2, results.size());

        Collection<AclBindingDeleteResult> deleteResults0 = results.get(0).toCompletableFuture().
            get().aclBindingDeleteResults();
        assertEquals(1, deleteResults0.size());
        AclBindingDeleteResult deleteResult0 = deleteResults0.iterator().next();
        assertEquals(TEST_BINDINGS.get(0), deleteResult0.aclBinding());
        assertEquals(Optional.empty(), deleteResult0.exception());
        AclDeleteResult deleteResult1 = results.get(1).toCompletableFuture().get();
        assertEquals(0, deleteResult1.aclBindingDeleteResults().size());
        assertEquals(InvalidRequestException.class, deleteResult1.exception().get().getClass());
    }

    @Test
    public void testDeleteAclsError() throws Exception {
        MockAclMutator mutator = new MockAclMutator();
        MockClusterMetadataAuthorizer authorizer = new MockClusterMetadataAuthorizer();
        authorizer.setAclMutator(mutator);
        CompletableFuture<List<AclDeleteResult>> response = new CompletableFuture<>();
        response.completeExceptionally(new AuthorizationException("not authorized"));
        mutator.setDeleteAclsResponse(response);
        List<? extends CompletionStage<AclDeleteResult>> results = authorizer.deleteAcls(
            new MockAuthorizableRequestContext.Builder().build(), TEST_FILTERS);
        assertEquals(2, results.size());
        for (int i = 0; i < 2; i++) {
            AclDeleteResult deleteResult = results.get(i).toCompletableFuture().get();
            assertEquals(0, deleteResult.aclBindingDeleteResults().size());
            assertEquals(AuthorizationException.class, deleteResult.exception().get().getClass());
        }
    }
}
