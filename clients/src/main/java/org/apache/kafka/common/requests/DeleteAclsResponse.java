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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.OPERATION;
import static org.apache.kafka.common.protocol.CommonFields.PERMISSION_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class DeleteAclsResponse extends AbstractResponse {
    public static final Logger log = LoggerFactory.getLogger(DeleteAclsResponse.class);
    private final static String FILTER_RESPONSES_KEY_NAME = "filter_responses";
    private final static String MATCHING_ACLS_KEY_NAME = "matching_acls";

    private static final Schema MATCHING_ACL_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            RESOURCE_TYPE,
            RESOURCE_NAME,
            PRINCIPAL,
            HOST,
            OPERATION,
            PERMISSION_TYPE);

    /**
     * V1 sees a new `RESOURCE_NAME_TYPE` that describes how the resource name is interpreted.
     *
     * For more info, see {@link org.apache.kafka.common.resource.ResourceNameType}.
     */
    private static final Schema MATCHING_ACL_V1 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            RESOURCE_TYPE,
            RESOURCE_NAME,
            RESOURCE_NAME_TYPE,
            PRINCIPAL,
            HOST,
            OPERATION,
            PERMISSION_TYPE);

    private static final Schema DELETE_ACLS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(FILTER_RESPONSES_KEY_NAME,
                    new ArrayOf(new Schema(
                            ERROR_CODE,
                            ERROR_MESSAGE,
                            new Field(MATCHING_ACLS_KEY_NAME, new ArrayOf(MATCHING_ACL_V0), "The matching ACLs")))));

    /**
     * V1 sees a new `RESOURCE_NAME_TYPE` field added to MATCHING_ACL_V1, that describes how the resource name is interpreted
     * and version was bumped to indicate that, on quota violation, brokers send out responses before throttling.
     *
     * For more info, see {@link org.apache.kafka.common.resource.ResourceNameType}.
     */
    private static final Schema DELETE_ACLS_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(FILTER_RESPONSES_KEY_NAME,
                    new ArrayOf(new Schema(
                            ERROR_CODE,
                            ERROR_MESSAGE,
                            new Field(MATCHING_ACLS_KEY_NAME, new ArrayOf(MATCHING_ACL_V1), "The matching ACLs")))));

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_ACLS_RESPONSE_V0, DELETE_ACLS_RESPONSE_V1};
    }

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

    private final int throttleTimeMs;

    private final List<AclFilterResponse> responses;

    public DeleteAclsResponse(int throttleTimeMs, List<AclFilterResponse> responses) {
        this.throttleTimeMs = throttleTimeMs;
        this.responses = responses;
    }

    public DeleteAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.responses = new ArrayList<>();
        for (Object responseStructObj : struct.getArray(FILTER_RESPONSES_KEY_NAME)) {
            Struct responseStruct = (Struct) responseStructObj;
            ApiError error = new ApiError(responseStruct);
            List<AclDeletionResult> deletions = new ArrayList<>();
            for (Object matchingAclStructObj : responseStruct.getArray(MATCHING_ACLS_KEY_NAME)) {
                Struct matchingAclStruct = (Struct) matchingAclStructObj;
                ApiError matchError = new ApiError(matchingAclStruct);
                AccessControlEntry entry = RequestUtils.aceFromStructFields(matchingAclStruct);
                ResourcePattern resource = RequestUtils.resourcePatternromStructFields(matchingAclStruct);
                deletions.add(new AclDeletionResult(matchError, new AclBinding(resource, entry)));
            }
            this.responses.add(new AclFilterResponse(error, deletions));
        }
    }

    @Override
    protected Struct toStruct(short version) {
        validate(version);

        Struct struct = new Struct(ApiKeys.DELETE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> responseStructs = new ArrayList<>();
        for (AclFilterResponse response : responses) {
            Struct responseStruct = struct.instance(FILTER_RESPONSES_KEY_NAME);
            response.error.write(responseStruct);
            List<Struct> deletionStructs = new ArrayList<>();
            for (AclDeletionResult deletion : response.deletions()) {
                Struct deletionStruct = responseStruct.instance(MATCHING_ACLS_KEY_NAME);
                deletion.error.write(deletionStruct);
                RequestUtils.resourcePatternSetStructFields(deletion.acl().pattern(), deletionStruct);
                RequestUtils.aceSetStructFields(deletion.acl().entry(), deletionStruct);
                deletionStructs.add(deletionStruct);
            }
            responseStruct.set(MATCHING_ACLS_KEY_NAME, deletionStructs.toArray(new Struct[0]));
            responseStructs.add(responseStruct);
        }
        struct.set(FILTER_RESPONSES_KEY_NAME, responseStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<AclFilterResponse> responses() {
        return responses;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (AclFilterResponse response : responses)
            updateErrorCounts(errorCounts, response.error.error());
        return errorCounts;
    }

    public static DeleteAclsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteAclsResponse(ApiKeys.DELETE_ACLS.responseSchema(version).read(buffer));
    }

    public String toString() {
        return "(responses=" + Utils.join(responses, ",") + ")";
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    private void validate(short version) {
        if (version == 0) {
            final boolean unsupported = responses.stream()
                .flatMap(r -> r.deletions.stream())
                .map(AclDeletionResult::acl)
                .map(AclBinding::pattern)
                .map(ResourcePattern::nameType)
                .anyMatch(nameType -> nameType != ResourceNameType.LITERAL);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource name types");
            }
        }

        final boolean unknown = responses.stream()
            .flatMap(r -> r.deletions.stream())
            .map(AclDeletionResult::acl)
            .anyMatch(AclBinding::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("Response contains UNKNOWN elements");
        }
    }
}
