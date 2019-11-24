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
package org.apache.kafka.common.requests;

import java.util.stream.Collectors;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.protocol.ApiKeys.DELETE_ACLS;

public class DeleteAclsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteAclsRequest> {
        private final DeleteAclsRequestData data;

        public Builder(DeleteAclsRequestData data) {
            super(DELETE_ACLS);
            this.data = data;
        }

        @Override
        public DeleteAclsRequest build(short version) {
            return new DeleteAclsRequest(version, data);
        }

        @Override
        public String toString() {
            return "(type=DeleteAclsRequest, filters=" + Utils.join(data.filters(), ", ") + ")";
        }
    }

    private final DeleteAclsRequestData data;

    DeleteAclsRequest(short version, List<AclBindingFilter> filters) {
        this(version, new DeleteAclsRequestData()
                .setFilters(filters.stream().map(DeleteAclsRequest::toDeleteFilter).collect(Collectors.toList())));
        validate(version, filters);
    }

    DeleteAclsRequest(short version, DeleteAclsRequestData data) {
        super(ApiKeys.DELETE_ACLS, version);
        this.data = data;
    }

    public DeleteAclsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_ACLS, version);
        this.data = new DeleteAclsRequestData(struct, version);
    }

    public List<AclBindingFilter> filters() {
        return data.filters().stream().map(DeleteAclsRequest::toAclBindingFilter).collect(Collectors.toList());
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        List<DeleteAclsResponse.AclFilterResponse> responses = new ArrayList<>();
        for (int i = 0; i < data.filters().size(); i++) {
            responses.add(new DeleteAclsResponse.AclFilterResponse(
                ApiError.fromThrowable(throwable), Collections.emptySet()));
        }
        return new DeleteAclsResponse(throttleTimeMs, responses);
    }

    public static DeleteAclsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteAclsRequest(DELETE_ACLS.parseRequest(version, buffer), version);
    }

    private void validate(short version, List<AclBindingFilter> filters) {
        if (version == 0) {
            final boolean unsupported = filters.stream()
                .map(AclBindingFilter::patternFilter)
                .map(ResourcePatternFilter::patternType)
                .anyMatch(patternType -> patternType != PatternType.LITERAL && patternType != PatternType.ANY);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = filters.stream().anyMatch(AclBindingFilter::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("Filters contain UNKNOWN elements");
        }
    }

    //FIXME Make naming consistent with Create...
    public static DeleteAclsRequestData.DeleteAclsFilter toDeleteFilter(AclBindingFilter filter) {
        return new DeleteAclsRequestData.DeleteAclsFilter()
            .setResourceNameFilter(filter.patternFilter().name())
            .setResourceTypeFilter(filter.patternFilter().resourceType().code())
            .setPatternTypeFilter(filter.patternFilter().patternType().code())
            .setHostFilter(filter.entryFilter().host())
            .setOperation(filter.entryFilter().operation().code())
            .setPermissionType(filter.entryFilter().permissionType().code())
            .setPrincipalFilter(filter.entryFilter().principal());
    }

    //FIXME Make naming consistent with Create...
    private static AclBindingFilter toAclBindingFilter(DeleteAclsRequestData.DeleteAclsFilter filter) {
        ResourcePatternFilter patternFilter = new ResourcePatternFilter(
            ResourceType.fromCode(filter.resourceTypeFilter()),
            filter.resourceNameFilter(),
            PatternType.fromCode(filter.patternTypeFilter()));
        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(
            filter.principalFilter(),
            filter.hostFilter(),
            AclOperation.fromCode(filter.operation()),
            AclPermissionType.fromCode(filter.permissionType()));
        return new AclBindingFilter(patternFilter, entryFilter);
    }
}
