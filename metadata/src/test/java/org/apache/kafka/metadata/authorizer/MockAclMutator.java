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
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class MockAclMutator implements AclMutator {
    private final StandardAuthorizer authorizer;
    private long nextUuid;
    private Map<Uuid, StandardAcl> aclsById;
    private Set<StandardAcl> acls;

    public MockAclMutator(StandardAuthorizer authorizer) {
        this.authorizer = authorizer;
        this.nextUuid = 1L;
        this.aclsById = new HashMap<>();
        this.acls = new HashSet<>();
    }

    @Override
    public synchronized CompletableFuture<List<AclCreateResult>> createAcls(
        ControllerRequestContext context,
        List<AclBinding> aclBindings
    ) {
        List<AclCreateResult> results = new ArrayList<>();
        for (AclBinding aclBinding : aclBindings) {
            StandardAcl acl = StandardAcl.fromAclBinding(aclBinding);
            if (acls.add(acl)) {
                Uuid id = new Uuid(1L, ++nextUuid);
                authorizer.addAcl(id, acl);
                aclsById.put(id, acl);
            }
            results.add(AclCreateResult.SUCCESS);
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    public synchronized CompletableFuture<List<AclDeleteResult>> deleteAcls(
        ControllerRequestContext context,
        List<AclBindingFilter> aclBindingFilters
    ) {
        List<AclDeleteResult> results = new ArrayList<>();
        for (AclBindingFilter aclBindingFilter : aclBindingFilters) {
            List<AclBindingDeleteResult> resultList = new ArrayList<>();
            for (Iterator<Entry<Uuid, StandardAcl>> iterator = aclsById.entrySet().iterator();
                    iterator.hasNext(); ) {
                Entry<Uuid, StandardAcl> entry = iterator.next();
                AclBinding aclBinding = entry.getValue().toBinding();
                if (aclBindingFilter.matches(aclBinding)) {
                    resultList.add(new AclBindingDeleteResult(aclBinding));
                    authorizer.removeAcl(entry.getKey());
                    iterator.remove();
                }
            }
            results.add(new AclDeleteResult(resultList));
        }
        return CompletableFuture.completedFuture(results);
    }
}