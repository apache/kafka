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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Represents changes to the ACLs in the metadata image.
 */
public final class AclsDelta {
    private final AclsImage image;
    private final Map<Uuid, Optional<StandardAcl>> changes = new LinkedHashMap<>();
    private boolean isSnapshotDelta = false;

    public AclsDelta(AclsImage image) {
        this.image = image;
    }

    public Map<Uuid, Optional<StandardAcl>> changes() {
        return changes;
    }

    void finishSnapshot() {
        this.isSnapshotDelta = true;
    }

    public boolean isSnapshotDelta() {
        return isSnapshotDelta;
    }

    public void replay(AccessControlEntryRecord record) {
        StandardAclWithId aclWithId = StandardAclWithId.fromRecord(record);
        changes.put(aclWithId.id(), Optional.of(aclWithId.acl()));
    }

    public void replay(RemoveAccessControlEntryRecord record) {
        changes.put(record.id(), Optional.empty());
    }

    public AclsImage apply() {
        Map<Uuid, StandardAcl> newAcls = new HashMap<>();
        if (!isSnapshotDelta) {
            for (Entry<Uuid, StandardAcl> entry : image.acls().entrySet()) {
                Optional<StandardAcl> change = changes.get(entry.getKey());
                if (change == null) {
                    newAcls.put(entry.getKey(), entry.getValue());
                } else if (change.isPresent()) {
                    newAcls.put(entry.getKey(), change.get());
                }
            }
        }
        for (Entry<Uuid, Optional<StandardAcl>> entry : changes.entrySet()) {
            if (!newAcls.containsKey(entry.getKey())) {
                if (entry.getValue().isPresent()) {
                    newAcls.put(entry.getKey(), entry.getValue().get());
                }
            }
        }
        return new AclsImage(newAcls);
    }

    @Override
    public String toString() {
        return "AclsDelta(isSnapshotDelta=" + isSnapshotDelta +
            ", changes=" + changes.entrySet().stream().
                map(e -> "" + e.getKey() + "=" + e.getValue()).
                collect(Collectors.joining(", ")) + ")";
    }
}
