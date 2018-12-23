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
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.OPERATION;
import static org.apache.kafka.common.protocol.CommonFields.PERMISSION_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_PATTERN_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_TYPE;

public class CreateAclsRequest extends AbstractRequest {
    private final static String CREATIONS_KEY_NAME = "creations";

    private static final Schema CREATE_ACLS_REQUEST_V0 = new Schema(
            new Field(CREATIONS_KEY_NAME, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME,
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    /**
     * Version 1 adds RESOURCE_PATTERN_TYPE, to support more than just literal resource patterns.
     * For more info, see {@link PatternType}.
     *
     * Also, when the quota is violated, brokers will respond to a version 1 or later request before throttling.
     */
    private static final Schema CREATE_ACLS_REQUEST_V1 = new Schema(
            new Field(CREATIONS_KEY_NAME, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME,
                    RESOURCE_PATTERN_TYPE,
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_ACLS_REQUEST_V0, CREATE_ACLS_REQUEST_V1};
    }

    public static class AclCreation {
        private final AclBinding acl;

        public AclCreation(AclBinding acl) {
            this.acl = acl;
        }

        static AclCreation fromStruct(Struct struct) {
            ResourcePattern pattern = RequestUtils.resourcePatternromStructFields(struct);
            AccessControlEntry entry = RequestUtils.aceFromStructFields(struct);
            return new AclCreation(new AclBinding(pattern, entry));
        }

        public AclBinding acl() {
            return acl;
        }

        void setStructFields(Struct struct) {
            RequestUtils.resourcePatternSetStructFields(acl.pattern(), struct);
            RequestUtils.aceSetStructFields(acl.entry(), struct);
        }

        @Override
        public String toString() {
            return "(acl=" + acl + ")";
        }
    }

    public static class Builder extends AbstractRequest.Builder<CreateAclsRequest> {
        private final List<AclCreation> creations;

        public Builder(List<AclCreation> creations) {
            super(ApiKeys.CREATE_ACLS);
            this.creations = creations;
        }

        @Override
        public CreateAclsRequest build(short version) {
            return new CreateAclsRequest(version, creations);
        }

        @Override
        public String toString() {
            return "(type=CreateAclsRequest, creations=" + Utils.join(creations, ", ") + ")";
        }
    }

    private final List<AclCreation> aclCreations;

    CreateAclsRequest(short version, List<AclCreation> aclCreations) {
        super(ApiKeys.CREATE_ACLS, version);
        this.aclCreations = aclCreations;

        validate(aclCreations);
    }

    public CreateAclsRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_ACLS, version);
        this.aclCreations = new ArrayList<>();
        for (Object creationStructObj : struct.getArray(CREATIONS_KEY_NAME)) {
            Struct creationStruct = (Struct) creationStructObj;
            aclCreations.add(AclCreation.fromStruct(creationStruct));
        }
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.CREATE_ACLS.requestSchema(version()));
        List<Struct> requests = new ArrayList<>();
        for (AclCreation creation : aclCreations) {
            Struct creationStruct = struct.instance(CREATIONS_KEY_NAME);
            creation.setStructFields(creationStruct);
            requests.add(creationStruct);
        }
        struct.set(CREATIONS_KEY_NAME, requests.toArray());
        return struct;
    }

    public List<AclCreation> aclCreations() {
        return aclCreations;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                List<CreateAclsResponse.AclCreationResponse> responses = new ArrayList<>();
                for (int i = 0; i < aclCreations.size(); i++)
                    responses.add(new CreateAclsResponse.AclCreationResponse(ApiError.fromThrowable(throwable)));
                return new CreateAclsResponse(throttleTimeMs, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CREATE_ACLS.latestVersion()));
        }
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
}
