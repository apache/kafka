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

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class CreateAclsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CreateAclsRequest> {
        private final CreateAclsRequestData data;

        public Builder(CreateAclsRequestData data) {
            super(ApiKeys.CREATE_ACLS);
            this.data = data;
        }

        @Override
        public CreateAclsRequest build(short version) {
            return new CreateAclsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateAclsRequestData data;

    CreateAclsRequest(CreateAclsRequestData data, short version) {
        super(ApiKeys.CREATE_ACLS, version);
        validate(data);
        this.data = data;
    }

    public List<AclCreation> aclCreations() {
        return data.creations();
    }

    @Override
    public CreateAclsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        CreateAclsResponseData.AclCreationResult result = CreateAclsRequest.aclResult(throwable);
        List<CreateAclsResponseData.AclCreationResult> results = Collections.nCopies(data.creations().size(), result);
        return new CreateAclsResponse(new CreateAclsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(results));
    }

    public static CreateAclsRequest parse(ByteBuffer buffer, short version) {
        return new CreateAclsRequest(new CreateAclsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    private void validate(CreateAclsRequestData data) {
        if (version() == 0) {
            final boolean unsupported = data.creations().stream().anyMatch(creation ->
                creation.resourcePatternType() != PatternType.LITERAL.code());
            if (unsupported)
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
        }

        final boolean unknown = data.creations().stream().anyMatch(creation ->
            creation.resourcePatternType() == PatternType.UNKNOWN.code()
                || creation.resourceType() == ResourceType.UNKNOWN.code()
                || creation.permissionType() == AclPermissionType.UNKNOWN.code()
                || creation.operation() == AclOperation.UNKNOWN.code());
        if (unknown)
            throw new IllegalArgumentException("CreatableAcls contain unknown elements: " + data.creations());
    }

    public static AclBinding aclBinding(AclCreation acl) {
        ResourcePattern pattern = new ResourcePattern(
            ResourceType.fromCode(acl.resourceType()),
            acl.resourceName(),
            PatternType.fromCode(acl.resourcePatternType()));
        AccessControlEntry entry = new AccessControlEntry(
            acl.principal(),
            acl.host(),
            AclOperation.fromCode(acl.operation()),
            AclPermissionType.fromCode(acl.permissionType()));
        return new AclBinding(pattern, entry);
    }

    public static AclCreation aclCreation(AclBinding binding) {
        return new AclCreation()
            .setHost(binding.entry().host())
            .setOperation(binding.entry().operation().code())
            .setPermissionType(binding.entry().permissionType().code())
            .setPrincipal(binding.entry().principal())
            .setResourceName(binding.pattern().name())
            .setResourceType(binding.pattern().resourceType().code())
            .setResourcePatternType(binding.pattern().patternType().code());
    }

    private static AclCreationResult aclResult(Throwable throwable) {
        ApiError apiError = ApiError.fromThrowable(throwable);
        return new AclCreationResult()
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message());
    }
}
