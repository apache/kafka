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
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteAclsResponse extends AbstractResponse {
    public static final Logger log = LoggerFactory.getLogger(DeleteAclsResponse.class);

    public static class AclDeletionResult {
        private final ApiError error;
        private final AclBinding acl;

        public AclDeletionResult(ApiError error, AclBinding acl) {
            this.error = error;
            this.acl = acl;
        }

        public AclDeletionResult(AclBinding acl) {
            this(ApiError.NONE, acl);
        }

        public ApiError error() {
            return error;
        }

        public AclBinding acl() {
            return acl;
        }

        @Override
        public String toString() {
            return "(error=" + error + ", acl=" + acl + ")";
        }
    }

    public static class AclFilterResponse {
        private final ApiError error;
        private final Collection<AclDeletionResult> deletions;

        public AclFilterResponse(ApiError error, Collection<AclDeletionResult> deletions) {
            this.error = error;
            this.deletions = deletions;
        }

        public AclFilterResponse(Collection<AclDeletionResult> deletions) {
            this(ApiError.NONE, deletions);
        }

        public ApiError error() {
            return error;
        }

        public Collection<AclDeletionResult> deletions() {
            return deletions;
        }

        @Override
        public String toString() {
            return "(error=" + error + ", deletions=" + Utils.join(deletions, ",") + ")";
        }
    }

    private final DeleteAclsResponseData data;

    public DeleteAclsResponse(int throttleTimeMs, List<AclFilterResponse> responses) {
        data = new DeleteAclsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setFilterResults(responses.stream().map(this::toResult).collect(Collectors.toList()));
    }

    public DeleteAclsResponse(Struct struct, short version) {
        data = new DeleteAclsResponseData(struct, version);
    }

    @Override
    protected Struct toStruct(short version) {
        validate(version);
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public List<AclFilterResponse> responses() {
        return data.filterResults().stream().map(this::toFilterResponse).collect(Collectors.toList());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (AclFilterResponse response : responses())
            updateErrorCounts(errorCounts, response.error.error());
        return errorCounts;
    }

    public static DeleteAclsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteAclsResponse(ApiKeys.DELETE_ACLS.parseResponse(version, buffer), version);
    }

    public String toString() {
        return "(responses=" + Utils.join(responses(), ",") + ")";
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    private void validate(short version) {
        if (version == 0) {
            final boolean unsupported = responses().stream()
                    .flatMap(r -> r.deletions.stream())
                    .map(AclDeletionResult::acl)
                    .map(AclBinding::pattern)
                    .map(ResourcePattern::patternType)
                    .anyMatch(patternType -> patternType != PatternType.LITERAL);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = responses().stream()
                .flatMap(r -> r.deletions.stream())
                .map(AclDeletionResult::acl)
                .anyMatch(AclBinding::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("Response contains UNKNOWN elements");
        }
    }

    private DeleteAclsResponseData.DeleteAclsFilterResult toResult(AclFilterResponse filterResponse) {
        return new DeleteAclsResponseData.DeleteAclsFilterResult()
                .setErrorCode(filterResponse.error.error().code())
                .setErrorMessage(filterResponse.error.message())
                .setMatchingAcls(filterResponse.deletions.stream().map(this::toMatchingAcl).collect(Collectors.toList()));
    }

    private DeleteAclsResponseData.DeleteAclsMatchingAcl toMatchingAcl(AclDeletionResult result) {
        return new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                .setErrorCode(result.error.error().code())
                .setErrorMessage(result.error.message())
                .setResourceName(result.acl.pattern().name())
                .setResourceType(result.acl.pattern().resourceType().code())
                .setPatternType(result.acl.pattern().patternType().code())
                .setHost(result.acl.entry().host())
                .setOperation(result.acl.entry().operation().code())
                .setPermissionType(result.acl.entry().permissionType().code())
                .setPrincipal(result.acl.entry().principal());
    }

    private AclFilterResponse toFilterResponse(DeleteAclsResponseData.DeleteAclsFilterResult result) {

        return new AclFilterResponse(
                new ApiError(Errors.forCode(result.errorCode()), result.errorMessage()),
                result.matchingAcls().stream().map(this::toDeletionResult).collect(Collectors.toList()));
    }

    private AclDeletionResult toDeletionResult(DeleteAclsResponseData.DeleteAclsMatchingAcl result) {
        ResourcePattern pattern = new ResourcePattern(
                ResourceType.fromCode(result.resourceType()),
                result.resourceName(),
                PatternType.fromCode(result.patternType()));
        AccessControlEntry entry = new AccessControlEntry(
                result.principal(),
                result.host(),
                AclOperation.fromCode(result.operation()),
                AclPermissionType.fromCode(result.permissionType()));
        return new AclDeletionResult(
                new ApiError(Errors.forCode(result.errorCode()), result.errorMessage()),
                new AclBinding(pattern, entry));
    }
}
