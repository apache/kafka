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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.List;
import java.util.Optional;

public class MockAclControlManager extends AclControlManager {
    public MockAclControlManager(LogContext logContext,
                                 Optional<ClusterMetadataAuthorizer> authorizer) {
        super(new SnapshotRegistry(logContext), authorizer);
    }

    public List<AclCreateResult> createAndReplayAcls(List<AclBinding> acls) {
        ControllerResult<List<AclCreateResult>> createResults = createAcls(acls);
        createResults.records().forEach(record -> replay((AccessControlEntryRecord) record.message(), Optional.empty()));
        return createResults.response();
    }

    public List<AclDeleteResult> deleteAndReplayAcls(List<AclBindingFilter> filters) {
        ControllerResult<List<AclDeleteResult>> deleteResults = deleteAcls(filters);
        deleteResults.records().forEach(record -> replay((RemoveAccessControlEntryRecord) record.message(), Optional.empty()));
        return deleteResults.response();
    }
}
