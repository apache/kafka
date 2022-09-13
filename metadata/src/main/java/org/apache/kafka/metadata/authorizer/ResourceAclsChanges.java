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

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Changes planned for a ResourceAcls object. This class is mutable.
 */
class ResourceAclsChanges implements AclChanges {
    private final IdentityHashMap<StandardAcl, Boolean> changes = new IdentityHashMap<>(0);
    private int numRemovals = 0;

    @Override
    public void newRemoval(StandardAcl acl) {
        Boolean prev = changes.put(acl, false);
        if (prev == null || prev) numRemovals++;
    }

    @Override
    public void newAddition(StandardAcl acl) {
        Boolean prev = changes.put(acl, true);
        if (prev != null && !prev) numRemovals--;
    }

    int numRemovals() {
        return numRemovals;
    }

    int netSizeChange() {
        return changes.size() - (2 * numRemovals);
    }

    boolean isRemoved(StandardAcl acl) {
        Boolean existing = changes.get(acl);
        return existing != null && !existing;
    }

    void apply(StandardAcl[] source, StandardAcl[] dest) {
        int j = 0, numRemoved = 0;
        for (int i = 0; i < source.length; i++) {
            StandardAcl acl = source[i];
            Boolean value = changes.remove(acl);
            if (value == null) {
                dest[j++] = acl;
            } else if (value) {
                throw new RuntimeException("ACL " + acl + " already exists in ResourceAcls.");
            } else {
                numRemoved++;
            }
        }
        if (numRemoved != numRemovals) {
            throw new RuntimeException("Unable to find " + (numRemovals - numRemoved) + " ACL(s) to remove.");
        }
        for (Iterator<StandardAcl> iterator = changes.keySet().iterator(); iterator.hasNext(); ) {
            dest[j++] = iterator.next();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ResourceAclsChanges(");
        String prefix = "";
        for (Map.Entry<StandardAcl, Boolean> entry : changes.entrySet()) {
            builder.append(prefix);
            builder.append(entry.getKey()).append(": ");
            if (entry.getValue()) {
                builder.append("added");
            } else {
                builder.append("removed");
            }
            prefix = ", ";
        }
        builder.append(")");
        return builder.toString();
    }
}
