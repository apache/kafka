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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * The AclControlManager manages any ACLs that are stored in the __cluster_metadata topic.
 * If the ACLs are stored externally (such as in ZooKeeper) then there will be nothing for
 * this manager to do, and the authorizer field will always be Optional.empty.
 *
 * Because the Authorizer is being concurrently used by other threads, we need to be
 * careful about snapshots. We don't want the Authorizer to act based on partial state
 * during the loading process. Therefore, unlike most of the other managers,
 * AclControlManager needs to receive callbacks when we start loading a snapshot and when
 * we finish. The prepareForSnapshotLoad callback clears the authorizer field, preventing
 * any changes from affecting the authorizer until completeSnapshotLoad is called.
 * Note that the Authorizer's start() method will block until the first snapshot load has
 * completed, which is another reason the prepare / complete callbacks are needed.
 */
public class AclControlManager {
    private final TimelineHashMap<Uuid, StandardAcl> idToAcl;
    private final TimelineHashSet<StandardAcl> existingAcls;
    private final Optional<ClusterMetadataAuthorizer> authorizer;

    AclControlManager(SnapshotRegistry snapshotRegistry,
                      Optional<ClusterMetadataAuthorizer> authorizer) {
        this.idToAcl = new TimelineHashMap<>(snapshotRegistry, 0);
        this.existingAcls = new TimelineHashSet<>(snapshotRegistry, 0);
        this.authorizer = authorizer;
    }

    ControllerResult<List<AclCreateResult>> createAcls(List<AclBinding> acls) {
        List<AclCreateResult> results = new ArrayList<>(acls.size());
        List<ApiMessageAndVersion> records = new ArrayList<>(acls.size());
        for (AclBinding acl : acls) {
            try {
                validateNewAcl(acl);
            } catch (Throwable t) {
                ApiException e = (t instanceof ApiException) ? (ApiException) t :
                    new UnknownServerException("Unknown error while trying to create ACL", t);
                results.add(new AclCreateResult(e));
                continue;
            }
            StandardAcl standardAcl = StandardAcl.fromAclBinding(acl);
            if (existingAcls.add(standardAcl)) {
                StandardAclWithId standardAclWithId = new StandardAclWithId(newAclId(), standardAcl);
                idToAcl.put(standardAclWithId.id(), standardAcl);
                records.add(new ApiMessageAndVersion(standardAclWithId.toRecord(), (short) 0));
            }
            results.add(AclCreateResult.SUCCESS);
        }
        return new ControllerResult<>(records, results, true);
    }

    Uuid newAclId() {
        Uuid uuid;
        do {
            uuid = Uuid.randomUuid();
        } while (idToAcl.containsKey(uuid));
        return uuid;
    }

    static void validateNewAcl(AclBinding binding) {
        switch (binding.pattern().resourceType()) {
            case UNKNOWN:
            case ANY:
                throw new InvalidRequestException("Invalid resourceType " +
                    binding.pattern().resourceType());
            default:
                break;
        }
        switch (binding.pattern().patternType()) {
            case LITERAL:
            case PREFIXED:
                break;
            default:
                throw new InvalidRequestException("Invalid patternType " +
                    binding.pattern().patternType());
        }
        switch (binding.entry().operation()) {
            case UNKNOWN:
            case ANY:
                throw new InvalidRequestException("Invalid operation " +
                    binding.entry().operation());
            default:
                break;
        }
        switch (binding.entry().permissionType()) {
            case DENY:
            case ALLOW:
                break;
            default:
                throw new InvalidRequestException("Invalid permissionType " +
                    binding.entry().permissionType());
        }
    }

    ControllerResult<List<AclDeleteResult>> deleteAcls(List<AclBindingFilter> filters) {
        List<AclDeleteResult> results = new ArrayList<>();
        Set<ApiMessageAndVersion> records = new HashSet<>();
        for (AclBindingFilter filter : filters) {
            try {
                validateFilter(filter);
                AclDeleteResult result = deleteAclsForFilter(filter, records);
                results.add(result);
            } catch (Throwable e) {
                results.add(new AclDeleteResult(ApiError.fromThrowable(e).exception()));
            }
        }
        return ControllerResult.atomicOf(records.stream().collect(Collectors.toList()), results);
    }

    AclDeleteResult deleteAclsForFilter(AclBindingFilter filter,
                                        Set<ApiMessageAndVersion> records) {
        List<AclBindingDeleteResult> deleted = new ArrayList<>();
        for (Entry<Uuid, StandardAcl> entry : idToAcl.entrySet()) {
            Uuid id = entry.getKey();
            StandardAcl acl = entry.getValue();
            AclBinding binding = acl.toBinding();
            if (filter.matches(binding)) {
                deleted.add(new AclBindingDeleteResult(binding));
                records.add(new ApiMessageAndVersion(
                    new RemoveAccessControlEntryRecord().setId(id), (short) 0));
            }
        }
        return new AclDeleteResult(deleted);
    }

    static void validateFilter(AclBindingFilter filter) {
        if (filter.patternFilter().isUnknown()) {
            throw new InvalidRequestException("Unknown patternFilter.");
        }
        if (filter.entryFilter().isUnknown()) {
            throw new InvalidRequestException("Unknown entryFilter.");
        }
    }

    public void replay(AccessControlEntryRecord record,
                       Optional<OffsetAndEpoch> snapshotId) {
        StandardAclWithId aclWithId = StandardAclWithId.fromRecord(record);
        idToAcl.put(aclWithId.id(), aclWithId.acl());
        existingAcls.add(aclWithId.acl());
        if (!snapshotId.isPresent()) {
            authorizer.ifPresent(a -> {
                a.addAcl(aclWithId.id(), aclWithId.acl());
            });
        }
    }

    public void replay(RemoveAccessControlEntryRecord record,
                       Optional<OffsetAndEpoch> snapshotId) {
        StandardAcl acl = idToAcl.remove(record.id());
        if (acl == null) {
            throw new RuntimeException("Unable to replay " + record + ": no acl with " +
                "that ID found.");
        }
        if (!existingAcls.remove(acl)) {
            throw new RuntimeException("Unable to replay " + record + " for " + acl +
                ": acl not found " + "in existingAcls.");
        }
        if (!snapshotId.isPresent()) {
            authorizer.ifPresent(a -> {
                a.removeAcl(record.id());
            });
        }
    }

    Map<Uuid, StandardAcl> idToAcl() {
        return Collections.unmodifiableMap(idToAcl);
    }
}
