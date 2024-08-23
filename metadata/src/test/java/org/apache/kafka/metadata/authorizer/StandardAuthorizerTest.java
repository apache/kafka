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

import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.Set;


@Timeout(value = 40)
public class StandardAuthorizerTest extends AbstractClusterMetadataAuthorizerTest<StandardAuthorizer> {

    @Override
    protected Builder getTestingWrapperBuilder() {
        return new Builder() {

            @Override
            public TestingWrapper<StandardAuthorizer> get() {

                return new TestingWrapper<StandardAuthorizer>() {

                    @Override
                    public org.apache.kafka.metadata.authorizer.StandardAuthorizer getUnconfiguredAuthorizer() {
                        return new StandardAuthorizer();
                    }

                    @Override
                    public StandardAuthorizer configure(StandardAuthorizer authorizer) {
                        applyConfigs(authorizer::configure);
                        return authorizer;
                    }

                    @Override
                    public StandardAuthorizer addAcls(StandardAuthorizer authorizer) {
                        applyAcls(authorizer::addAcl);
                        return authorizer;
                    }

                    @Override
                    public org.apache.kafka.metadata.authorizer.StandardAuthorizer getAuthorizer() {
                        StandardAuthorizer authorizer = configure(getUnconfiguredAuthorizer());
                        authorizer.start(new AuthorizerTestServerInfo(Collections.singletonList(PLAINTEXT)));
                        addAcls(authorizer).completeInitialLoad();
                        return authorizer;
                    }

                    @Override
                    public Set<String> superUsers(org.apache.kafka.metadata.authorizer.StandardAuthorizer authorizer) {
                        return authorizer.getData().superUsers();
                    }

                    @Override
                    public AuthorizationResult defaultResult(org.apache.kafka.metadata.authorizer.StandardAuthorizer authorizer) {
                        return authorizer.defaultResult();
                    }
                };
            }
        };
    }
}
