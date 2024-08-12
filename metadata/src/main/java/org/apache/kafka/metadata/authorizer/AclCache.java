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
import org.apache.kafka.server.immutable.ImmutableMap;
import org.apache.kafka.server.immutable.ImmutableNavigableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * An immutable class that stores the ACLs in KRaft-based clusters.
 */
public class AclCache {
    /**
     * Contains all of the current ACLs sorted by (resource type, resource name).
     */
    private final ImmutableNavigableSet<StandardAcl> aclsByResource;

    /**
     * Contains all of the current ACLs indexed by UUID.
     */
    private final ImmutableMap<Uuid, StandardAcl> aclsById;

    AclCache() {
        this(ImmutableNavigableSet.empty(), ImmutableMap.empty());
    }

    private AclCache(final ImmutableNavigableSet<StandardAcl> aclsByResource, final ImmutableMap<Uuid, StandardAcl> aclsById) {
        this.aclsByResource = aclsByResource;
        this.aclsById = aclsById;
    }

    public ImmutableNavigableSet<StandardAcl> aclsByResource() {
        return aclsByResource;
    }

    Iterable<AclBinding> acls(Predicate<AclBinding> filter) {
        List<AclBinding> aclBindingList = new ArrayList<>();
        aclsByResource.forEach(acl -> {
            AclBinding aclBinding = acl.toBinding();
            if (filter.test(aclBinding)) {
                aclBindingList.add(aclBinding);
            }
        });
        return aclBindingList;
    }

    int count() {
        return aclsById.size();
    }

    StandardAcl getAcl(Uuid id) {
        return aclsById.get(id);
    }

    AclCache addAcl(Uuid id, StandardAcl acl) {
        StandardAcl prevAcl = this.aclsById.get(id);
        if (prevAcl != null) {
            throw new RuntimeException("An ACL with ID " + id + " already exists.");
        }

        ImmutableMap<Uuid, StandardAcl> aclsById = this.aclsById.updated(id, acl);

        if (this.aclsByResource.contains(acl)) {
            throw new RuntimeException("Unable to add the ACL with ID " + id +
                    " to aclsByResource");
        }

        ImmutableNavigableSet<StandardAcl> aclsByResource = this.aclsByResource.added(acl);
        return new AclCache(aclsByResource, aclsById);
    }

    AclCache removeAcl(Uuid id) {
        StandardAcl acl = this.aclsById.get(id);
        if (acl == null) {
            throw new RuntimeException("ID " + id + " not found in aclsById.");
        }
        ImmutableMap<Uuid, StandardAcl> aclsById = this.aclsById.removed(id);

        if (!this.aclsByResource.contains(acl)) {
            throw new RuntimeException("Unable to remove the ACL with ID " + id +
                    " from aclsByResource");
        }

        ImmutableNavigableSet<StandardAcl> aclsByResource = this.aclsByResource.removed(acl);
        return new AclCache(aclsByResource, aclsById);
    }
}
