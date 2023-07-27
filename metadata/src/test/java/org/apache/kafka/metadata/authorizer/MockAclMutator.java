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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.MockAclControlManager;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
public class MockAclMutator implements AclMutator {
    MockAclControlManager aclControlManager;

    public MockAclMutator(StandardAuthorizer authorizer) {
        aclControlManager = createAclControlManager(authorizer);
    }

    private MockAclControlManager createAclControlManager(StandardAuthorizer standardAuthorizer) {
        LogContext logContext = new LogContext();
        return new MockAclControlManager(logContext, Optional.of(standardAuthorizer));
    }

    @Override
    public CompletableFuture<List<AclCreateResult>> createAcls(
        ControllerRequestContext context,
        List<AclBinding> aclBindings
    ) {
        CompletableFuture<List<AclCreateResult>> future = new CompletableFuture<>();
        future.complete(aclControlManager.createAndReplayAcls(aclBindings));
        return future;
    }

    @Override
    public CompletableFuture<List<AclDeleteResult>> deleteAcls(
        ControllerRequestContext context,
        List<AclBindingFilter> aclBindingFilters
    ) {
        CompletableFuture<List<AclDeleteResult>> future = new CompletableFuture<>();
        future.complete(aclControlManager.deleteAndReplayAcls(aclBindingFilters));
        return future;
    }
}