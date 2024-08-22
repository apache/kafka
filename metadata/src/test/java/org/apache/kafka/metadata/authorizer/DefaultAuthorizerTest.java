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
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class DefaultAuthorizerTest extends AbstractAuthorizerTest<DefaultAuthorizerTest.DefaultAuthorizer> {
    @Override
    protected Builder getTestingWrapperBuilder() {
        return new Builder() {
            public TestingWrapper<DefaultAuthorizer> get() {
                return new TestingWrapper<DefaultAuthorizer>() {

                    @Override
                    public DefaultAuthorizer getUnconfiguredAuthorizer() {
                        return new DefaultAuthorizer();
                    }

                    @Override
                    public DefaultAuthorizer configure(DefaultAuthorizer authorizer) {
                        applyConfigs(authorizer::configure);
                        return authorizer;
                    }

                    @Override
                    public DefaultAuthorizer addAcls(DefaultAuthorizer authorizer) {
                        applyAcls(authorizer::addAcl);
                        return authorizer;
                    }

                    @Override
                    public DefaultAuthorizer getAuthorizer() {
                        DefaultAuthorizer authorizer = configure(getUnconfiguredAuthorizer());
                        authorizer.start(new AuthorizerTestServerInfo(Collections.singletonList(PLAINTEXT)));
                        addAcls(authorizer).completeInitialLoad();
                        return authorizer;
                    }

                    @Override
                    public Set<String> superUsers(DefaultAuthorizer authorizer) {
                        return authorizer.getData().superUsers();
                    }

                    @Override
                    public AuthorizationResult defaultResult(DefaultAuthorizer authorizer) {
                        return authorizer.defaultResult();
                    }
                };

            }
        };
    }


    public static class DefaultAuthorizer implements Authorizer {

        StandardAuthorizer delegate = new StandardAuthorizer();

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
            return delegate.createAcls(requestContext, aclBindings);
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> filters) {
            return delegate.deleteAcls(requestContext, filters);
        }

        public AuthorizerData getData() {
            return delegate.getData();
        }

        public void setAclMutator(AclMutator aclMutator) {
            delegate.setAclMutator(aclMutator);
        }

        public AclMutator aclMutatorOrException() {
            return delegate.aclMutatorOrException();
        }

        public void completeInitialLoad() {
            delegate.completeInitialLoad();
        }

        public CompletableFuture<Void> initialLoadFuture() {
            return delegate.initialLoadFuture();
        }

        public void completeInitialLoad(Exception e) {
            delegate.completeInitialLoad(e);
        }

        public void addAcl(Uuid id, StandardAcl acl) {
            delegate.addAcl(id, acl);
        }

        public void removeAcl(Uuid id) {
            delegate.removeAcl(id);
        }

        public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
            delegate.loadSnapshot(acls);
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return delegate.start(serverInfo);
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            return delegate.authorize(requestContext, actions);
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return delegate.acls(filter);
        }

        @Override
        public int aclCount() {
            return delegate.aclCount();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            delegate.configure(configs);
        }

        public AuthorizationResult defaultResult() {
            return delegate.defaultResult();
        }
    }
}
