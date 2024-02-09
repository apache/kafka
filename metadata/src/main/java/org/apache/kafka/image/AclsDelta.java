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
import org.apache.kafka.server.common.MetadataVersion;

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

    public AclsDelta(AclsImage image) {
        this.image = image;
    }

    /**
     * Returns a Map of deltas from ACL ID to optional StandardAcl. An empty optional value indicates the ACL
     * is for removal. An optional with a value indicates the ACL is to be added.
     *
     * @return Map of deltas.
     */
    public Map<Uuid, Optional<StandardAcl>> changes() {
        return changes;
    }

    void finishSnapshot() {
        for (Entry<Uuid, StandardAcl> entry : image.acls().entrySet()) {
            if (!changes.containsKey(entry.getKey())) {
                changes.put(entry.getKey(), Optional.empty());
            }
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public void replay(AccessControlEntryRecord record) {
        StandardAclWithId aclWithId = StandardAclWithId.fromRecord(record);
        changes.put(aclWithId.id(), Optional.of(aclWithId.acl()));
    }

    /**
     * This method replays a RemoveAccessControlEntryRecord record. If the current image contains the ACL
     * the removal is stored as an Optional.empty() value in the Map. If the changes Map contains the ACL
     * it means the ACL was recently applied and isn't in the image yet, in which case the ACL can be totally removed
     * from the Map because there is no need to add it then delete it when the changes are applied.
     *
     * @param record Log metadata record to replay.
     */
    public void replay(RemoveAccessControlEntryRecord record) {
        if (image.acls().containsKey(record.id())) {
            changes.put(record.id(), Optional.empty());
        } else if (changes.containsKey(record.id())) {
            changes.remove(record.id());
            // No need to track a ACL that was added and deleted within the same delta
        } else {
            throw new IllegalStateException("Failed to find existing ACL with ID " + record.id() + " in either image or changes");
        }
    }

    public AclsImage apply() {
        Map<Uuid, StandardAcl> newAcls = new HashMap<>();
        for (Entry<Uuid, StandardAcl> entry : image.acls().entrySet()) {
            Optional<StandardAcl> change = changes.get(entry.getKey());
            if (change == null) {
                newAcls.put(entry.getKey(), entry.getValue());
            } else if (change.isPresent()) {
                newAcls.put(entry.getKey(), change.get());
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
        return "AclsDelta(" +
            ", changes=" + changes.entrySet().stream().
                map(e -> "" + e.getKey() + "=" + e.getValue()).
                collect(Collectors.joining(", ")) + ")";
    }
}
