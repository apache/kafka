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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.authorizer.AclMutator;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;


/**
 * The MockAclMutator is a class which connects a StandardAuthorizer up to an AclControlManager.
 * Normally, this connection goes through the QuorumController. However, this class just attaches
 * the two directly, for the purpose of unit testing.
 */
public class MockAclMutator implements AclMutator {
    private final StandardAuthorizer authorizer;
    private final AclControlManager aclControl;

    public MockAclMutator(
        StandardAuthorizer authorizer
    ) {
        this.authorizer = authorizer;
        this.aclControl = new AclControlManager.Builder().build();
    }

    private void syncIdToAcl(
        Map<Uuid, StandardAcl> prevIdToAcl,
        Map<Uuid, StandardAcl> nextIdToAcl
    ) {
        for (Entry<Uuid, StandardAcl> entry : prevIdToAcl.entrySet()) {
            if (!entry.getValue().equals(nextIdToAcl.get(entry.getKey()))) {
                authorizer.removeAcl(entry.getKey());
            }
        }
        for (Entry<Uuid, StandardAcl> entry : nextIdToAcl.entrySet()) {
            if (!entry.getValue().equals(prevIdToAcl.get(entry.getKey()))) {
                authorizer.addAcl(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public synchronized CompletableFuture<List<AclCreateResult>> createAcls(
        ControllerRequestContext context,
        List<AclBinding> aclBindings
    ) {
        Map<Uuid, StandardAcl> prevIdToAcl = new HashMap<>(aclControl.idToAcl());
        ControllerResult<List<AclCreateResult>> result = aclControl.createAcls(aclBindings);
        RecordTestUtils.replayAll(aclControl, result.records());
        syncIdToAcl(prevIdToAcl, aclControl.idToAcl());
        return CompletableFuture.completedFuture(result.response());
    }

    @Override
    public synchronized CompletableFuture<List<AclDeleteResult>> deleteAcls(
        ControllerRequestContext context,
        List<AclBindingFilter> aclBindingFilters
    ) {
        Map<Uuid, StandardAcl> prevIdToAcl = new HashMap<>(aclControl.idToAcl());
        ControllerResult<List<AclDeleteResult>> result = aclControl.deleteAcls(aclBindingFilters);
        RecordTestUtils.replayAll(aclControl, result.records());
        syncIdToAcl(prevIdToAcl, aclControl.idToAcl());
        return CompletableFuture.completedFuture(result.response());
    }
}
