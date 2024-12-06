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

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedProtocolFieldException;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
            return new DeleteAclsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

    }

    private final DeleteAclsRequestData data;

    private DeleteAclsRequest(DeleteAclsRequestData data, short version) {
        super(ApiKeys.DELETE_ACLS, version);
        this.data = data;
        normalizeAndValidate();
    }

    private void normalizeAndValidate() {
        if (version() == 0) {
            for (DeleteAclsRequestData.DeleteAclsFilter filter : data.filters()) {
                PatternType patternType = PatternType.fromCode(filter.patternTypeFilter());

                // On older brokers, no pattern types existed except LITERAL (effectively). So even though ANY is not
                // directly supported on those brokers, we can get the same effect as ANY by setting the pattern type
                // to LITERAL. Note that the wildcard `*` is considered `LITERAL` for compatibility reasons.
                if (patternType == PatternType.ANY)
                    filter.setPatternTypeFilter(PatternType.LITERAL.code());
                else if (patternType != PatternType.LITERAL) {
                    String unsupportedTypes = Arrays.stream(PatternType.values())
                        .filter(type -> type != PatternType.ANY && type != PatternType.LITERAL)
                        .map(PatternType::name)
                        .collect(Collectors.joining(","));
                    throw new UnsupportedProtocolFieldException(unsupportedTypes, apiKey().name(), version(), 1);
                }
            }
        }

        final boolean unknown = data.filters().stream().anyMatch(filter ->
                filter.patternTypeFilter() == PatternType.UNKNOWN.code()
                        || filter.resourceTypeFilter() == ResourceType.UNKNOWN.code()
                        || filter.operation() == AclOperation.UNKNOWN.code()
                        || filter.permissionType() == AclPermissionType.UNKNOWN.code()
        );

        if (unknown) {
            throw new IllegalArgumentException("Filters contain UNKNOWN elements, filters: " + data.filters());
        }
    }

    public List<AclBindingFilter> filters() {
        return data.filters().stream().map(DeleteAclsRequest::aclBindingFilter).collect(Collectors.toList());
    }

    @Override
    public DeleteAclsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        ApiError apiError = ApiError.fromThrowable(throwable);
        List<DeleteAclsFilterResult> filterResults = Collections.nCopies(data.filters().size(),
            new DeleteAclsResponseData.DeleteAclsFilterResult()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message()));
        return new DeleteAclsResponse(new DeleteAclsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setFilterResults(filterResults), version());
    }

    public static DeleteAclsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteAclsRequest(new DeleteAclsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static DeleteAclsFilter deleteAclsFilter(AclBindingFilter filter) {
        return new DeleteAclsFilter()
            .setResourceNameFilter(filter.patternFilter().name())
            .setResourceTypeFilter(filter.patternFilter().resourceType().code())
            .setPatternTypeFilter(filter.patternFilter().patternType().code())
            .setHostFilter(filter.entryFilter().host())
            .setOperation(filter.entryFilter().operation().code())
            .setPermissionType(filter.entryFilter().permissionType().code())
            .setPrincipalFilter(filter.entryFilter().principal());
    }

    private static AclBindingFilter aclBindingFilter(DeleteAclsFilter filter) {
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
