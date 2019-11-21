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
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateAclsRequest extends AbstractRequest {
    public static class AclCreation {
        private final AclBinding acl;

        public AclCreation(AclBinding acl) {
            this.acl = acl;
        }

        public AclBinding acl() {
            return acl;
        }

        @Override
        public String toString() {
            return "(acl=" + acl + ")";
        }
    }

    public static class Builder extends AbstractRequest.Builder<CreateAclsRequest> {
        private final CreateAclsRequestData data;

        public Builder(List<AclCreation> creations) {
            this(createAclsRequestData(creations));
        }

        public Builder(CreateAclsRequestData data) {
            super(ApiKeys.CREATE_ACLS);
            this.data = data;
        }

        @Override
        public CreateAclsRequest build(short version) {
            return new CreateAclsRequest(version, data);
        }

        @Override
        public String toString() {
            return "(type=CreateAclsRequest, creations=" + Utils.join(data.creations(), ", ") + ")";
        }
    }

    private final List<AclCreation> aclCreations;
    private final CreateAclsRequestData data;

    CreateAclsRequest(short version, List<AclCreation> aclCreations) {
        super(ApiKeys.CREATE_ACLS, version);
        this.aclCreations = aclCreations;
        this.data = createAclsRequestData(aclCreations);
        validate(aclCreations);
    }

    private CreateAclsRequest(short version, CreateAclsRequestData data) {
        super(ApiKeys.CREATE_ACLS, version);
        this.data = data;
        this.aclCreations = new ArrayList<>(data.creations().size());
        for (CreateAclsRequestData.CreatableAcl creation : data.creations()) {
            ResourcePattern pattern = new ResourcePattern(
                ResourceType.fromCode(creation.resourceType()),
                creation.resourceName(),
                PatternType.fromCode(creation.resourcePatternType()));
            AccessControlEntry entry = new AccessControlEntry(
                creation.principal(),
                creation.host(),
                AclOperation.fromCode(creation.operation()),
                AclPermissionType.fromCode(creation.permissionType()));
            aclCreations.add(new AclCreation(new AclBinding(pattern, entry)));
        }
    }

    public CreateAclsRequest(Struct struct, short version) {
        this(version, new CreateAclsRequestData(struct, version));
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    public List<AclCreation> aclCreations() {
        return aclCreations;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        List<CreateAclsResponse.AclCreationResponse> responses = new ArrayList<>();
        for (int i = 0; i < aclCreations.size(); i++)
            responses.add(new CreateAclsResponse.AclCreationResponse(ApiError.fromThrowable(throwable)));
        return new CreateAclsResponse(throttleTimeMs, responses);
    }

    public static CreateAclsRequest parse(ByteBuffer buffer, short version) {
        return new CreateAclsRequest(ApiKeys.CREATE_ACLS.parseRequest(version, buffer), version);
    }

    private void validate(List<AclCreation> aclCreations) {
        if (version() == 0) {
            final boolean unsupported = aclCreations.stream()
                .map(AclCreation::acl)
                .map(AclBinding::pattern)
                .map(ResourcePattern::patternType)
                .anyMatch(patternType -> patternType != PatternType.LITERAL);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = aclCreations.stream()
            .map(AclCreation::acl)
            .anyMatch(AclBinding::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("You can not create ACL bindings with unknown elements");
        }
    }

    private static CreateAclsRequestData createAclsRequestData(List<AclCreation> aclCreations) {
        CreateAclsRequestData data = new CreateAclsRequestData();
        for (AclCreation creation : aclCreations) {
            data.creations().add(new CreateAclsRequestData.CreatableAcl()
                .setHost(creation.acl().entry().host())
                .setOperation(creation.acl.entry().operation().code())
                .setPermissionType(creation.acl.entry().permissionType().code())
                .setPrincipal(creation.acl.entry().principal())
                .setResourceName(creation.acl.pattern().name())
                .setResourceType(creation.acl.pattern().resourceType().code())
                .setResourcePatternType(creation.acl.pattern().patternType().code()));
        }
        return data;
    }
}
