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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;


/**
 * The standard authorizer which is used in KRaft-based clusters if no other authorizer is
 * configured.
 */
class AclLoader {
    private final Map<Uuid, StandardAcl> prevAclsById;
    private final Map<Resource, ResourceAcls> prevLiterals;
    private final Map<ResourceType, PrefixNode> prevPrefixed;
    private final Map<Uuid, StandardAcl> newAclsById;
    private final Map<Resource, ResourceAclsChanges> literalChanges;
    private final Map<ResourceType, PrefixTreeBuilder> prefixChanges;

    AclLoader(Map<Uuid, StandardAcl> snapshot) {
        this.prevAclsById = Collections.emptyMap();
        this.prevLiterals = Collections.emptyMap();
        this.prevPrefixed = Collections.emptyMap();
        this.newAclsById = snapshot;
        this.literalChanges = new HashMap<>();
        this.prefixChanges = new HashMap<>();
        for (StandardAcl newAcl : snapshot.values()) {
            getOrCreateChangeObject(newAcl).newAddition(newAcl);
        }
    }

    AclLoader(
        Map<Uuid, StandardAcl> prevAclsById,
        Map<Resource, ResourceAcls> prevLiterals,
        Map<ResourceType, PrefixNode> prevPrefixed,
        Map<Uuid, Optional<StandardAcl>> changes
    ) {
        this.prevAclsById = prevAclsById;
        this.prevLiterals = prevLiterals;
        this.prevPrefixed = prevPrefixed;
        this.newAclsById = new HashMap<>(prevAclsById.size());
        this.literalChanges = new HashMap<>(0);
        this.prefixChanges = new HashMap<>(0);
        for (Entry<Uuid, Optional<StandardAcl>> entry : changes.entrySet()) {
            Uuid id = entry.getKey();
            StandardAcl oldAcl = prevAclsById.get(id);
            if (oldAcl == null) {
                if (!entry.getValue().isPresent()) {
                    // Remove operations MUST find their target ACL. Upserts may or may not
                    // replace something.
                    throw new RuntimeException("Unable to remove ACL with UUID " + id +
                            ": not found.");
                }
            } else {
                getOrCreateChangeObject(oldAcl).newRemoval(oldAcl);
            }
            entry.getValue().ifPresent(newAcl -> {
                newAclsById.put(id, newAcl);
                getOrCreateChangeObject(newAcl).newAddition(newAcl);
            });
        }
        for (Entry<Uuid, StandardAcl> entry : prevAclsById.entrySet()) {
            if (!changes.containsKey(entry.getKey())) {
                newAclsById.put(entry.getKey(), entry.getValue());
            }
        }
    }

    AclChanges getOrCreateChangeObject(StandardAcl acl) {
        if (acl.isWildcardOrPrefix()) {
            return prefixChanges.computeIfAbsent(acl.resourceType(),
                    __ -> new PrefixTreeBuilder(prevPrefixed.getOrDefault(acl.resourceType(), PrefixNode.EMPTY)));
        } else if (acl.patternType() == PatternType.LITERAL) {
            return literalChanges.computeIfAbsent(acl.resource(), __ -> new ResourceAclsChanges());
        } else {
            throw new RuntimeException("Unsupported patternType " + acl.patternType());
        }
    }

    Result build() {
        Map<Resource, ResourceAcls> newLiterals = buildLiteralMap(prevLiterals, literalChanges);
        Map<ResourceType, PrefixNode> newPrefixed = buildPrefixedMap(prevPrefixed, prefixChanges);
        return new Result(newAclsById, newLiterals, newPrefixed);
    }

    static Map<Resource, ResourceAcls> buildLiteralMap(
        Map<Resource, ResourceAcls> prevMap,
        Map<Resource, ResourceAclsChanges> changeMap
    ) {
        if (changeMap.isEmpty()) return prevMap;
        Map<Resource, ResourceAcls> newMap = new HashMap<>();
        for (Entry<Resource, ResourceAclsChanges> entry : changeMap.entrySet()) {
            Resource resource = entry.getKey();
            ResourceAclsChanges changes = entry.getValue();
            ResourceAcls resourceAcls = prevMap.getOrDefault(resource, ResourceAcls.EMPTY);
            ResourceAcls newResourceAcls = resourceAcls.copyWithChanges(changes);
            if (!newResourceAcls.isEmpty()) {
                newMap.put(resource, newResourceAcls);
            }
        }
        for (Entry<Resource, ResourceAcls> entry : prevMap.entrySet()) {
            if (!changeMap.containsKey(entry.getKey())) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        return newMap;
    }

    static Map<ResourceType, PrefixNode> buildPrefixedMap(
        Map<ResourceType, PrefixNode> prevPrefixed,
        Map<ResourceType, PrefixTreeBuilder> prefixChanges
    ) {
        if (prefixChanges.isEmpty()) return prevPrefixed;
        Map<ResourceType, PrefixNode> newMap = new HashMap<>();
        for (Entry<ResourceType, PrefixTreeBuilder> entry : prefixChanges.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue().build());
        }
        for (Entry<ResourceType, PrefixNode> entry : prevPrefixed.entrySet()) {
            if (!prefixChanges.containsKey(entry.getKey())) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        return newMap;
    }

    static class Result {
        private final Map<Uuid, StandardAcl> newAclsById;
        private final Map<Resource, ResourceAcls> newLiterals;
        private final Map<ResourceType, PrefixNode> newPrefixed;

        Result(
            Map<Uuid, StandardAcl> newAclsById,
            Map<Resource, ResourceAcls> newLiterals,
            Map<ResourceType, PrefixNode> newPrefixed
        ) {
            this.newAclsById = newAclsById;
            this.newLiterals = newLiterals;
            this.newPrefixed = newPrefixed;
        }

        Map<Uuid, StandardAcl> newAclsById() {
            return newAclsById;
        }

        Map<Resource, ResourceAcls> newLiterals() {
            return newLiterals;
        }

        Map<ResourceType, PrefixNode> newPrefixed() {
            return newPrefixed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(newAclsById, newLiterals, newPrefixed);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || (!o.getClass().equals(this.getClass()))) return false;
            Result other = (Result) o;
            return newAclsById.equals(other.newAclsById) &&
                newLiterals.equals(other.newLiterals) &&
                newPrefixed.equals(other.newPrefixed);
        }

        @Override
        public String toString() {
            return "Result(newAclsById=" + newAclsById +
                    ", newLiterals=" + newLiterals +
                    ", newPrefixed=" + newPrefixed +
                    ")";
        }
    }
}
