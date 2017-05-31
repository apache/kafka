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

import org.apache.kafka.clients.admin.AccessControlEntry;
import org.apache.kafka.clients.admin.AclBinding;
import org.apache.kafka.clients.admin.Resource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DeleteAclsResponse extends AbstractResponse {
    public static final Logger log = LoggerFactory.getLogger(DeleteAclsResponse.class);
    private final static String FILTER_RESPONSES = "filter_responses";
    private final static String ERROR_CODE = "error_code";
    private final static String ERROR_MESSAGE = "error_message";
    private final static String MATCHING_ACLS = "matching_acls";

    public static class AclDeletionResult {
        private final ApiException exception;
        private final AclBinding acl;

        public AclDeletionResult(ApiException exception, AclBinding acl) {
            this.exception = exception;
            this.acl = acl;
        }

        public ApiException exception() {
            return exception;
        }

        public AclBinding acl() {
            return acl;
        }

        @Override
        public String toString() {
            return "(apiException=" + exception + ", acl=" + acl + ")";
        }
    }

    public static class AclFilterResponse {
        private final Throwable throwable;
        private final Collection<AclDeletionResult> deletions;

        public AclFilterResponse(Throwable throwable, Collection<AclDeletionResult> deletions) {
            this.throwable = throwable;
            this.deletions = deletions;
        }

        public Throwable throwable() {
            return throwable;
        }

        public Collection<AclDeletionResult> deletions() {
            return deletions;
        }

        @Override
        public String toString() {
            return "(throwable=" + throwable + ", deletions=" + Utils.join(deletions, ",") + ")";
        }
    }

    private final int throttleTimeMs;

    private final List<AclFilterResponse> responses;

    public DeleteAclsResponse(int throttleTimeMs, List<AclFilterResponse> responses) {
        this.throttleTimeMs = throttleTimeMs;
        this.responses = responses;
    }

    public DeleteAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.getInt(THROTTLE_TIME_KEY_NAME);
        this.responses = new ArrayList<>();
        for (Object responseStructObj : struct.getArray(FILTER_RESPONSES)) {
            Struct responseStruct = (Struct) responseStructObj;
            short responseErrorCode = responseStruct.getShort(ERROR_CODE);
            String responseErrorMessage = responseStruct.getString(ERROR_MESSAGE);
            if (responseErrorCode != 0) {
                this.responses.add(new AclFilterResponse(
                    Errors.forCode(responseErrorCode).exception(responseErrorMessage),
                    Collections.<AclDeletionResult>emptySet()));
            } else {
                List<AclDeletionResult> deletions = new ArrayList<>();
                for (Object matchingAclStructObj : responseStruct.getArray(MATCHING_ACLS)) {
                    Struct matchingAclStruct = (Struct) matchingAclStructObj;
                    short matchErrorCode = matchingAclStruct.getShort(ERROR_CODE);
                    ApiException exception = null;
                    if (matchErrorCode != 0) {
                        Errors errors = Errors.forCode(matchErrorCode);
                        String matchErrorMessage = matchingAclStruct.getString(ERROR_MESSAGE);
                        exception = errors.exception(matchErrorMessage);
                    }
                    AccessControlEntry entry = RequestUtils.aceFromStructFields(matchingAclStruct);
                    Resource resource = RequestUtils.resourceFromStructFields(matchingAclStruct);
                    deletions.add(new AclDeletionResult(exception, new AclBinding(resource, entry)));
                }
                this.responses.add(new AclFilterResponse(null, deletions));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        List<Struct> responseStructs = new ArrayList<>();
        for (AclFilterResponse response : responses) {
            Struct responseStruct = struct.instance(FILTER_RESPONSES);
            if (response.throwable() != null) {
                Errors error = Errors.forException(response.throwable());
                responseStruct.set(ERROR_CODE, error.code());
                responseStruct.set(ERROR_MESSAGE, response.throwable().getMessage());
                responseStruct.set(MATCHING_ACLS, new Struct[0]);
            } else {
                responseStruct.set(ERROR_CODE, (short) 0);
                List<Struct> deletionStructs = new ArrayList<>();
                for (AclDeletionResult deletion : response.deletions()) {
                    Struct deletionStruct = responseStruct.instance(MATCHING_ACLS);
                    if (deletion.exception() != null) {
                        Errors error = Errors.forException(deletion.exception);
                        deletionStruct.set(ERROR_CODE, error.code());
                        deletionStruct.set(ERROR_MESSAGE, deletion.exception.getMessage());
                    } else {
                        deletionStruct.set(ERROR_CODE, (short) 0);
                    }
                    RequestUtils.resourceSetStructFields(deletion.acl().resource(), deletionStruct);
                    RequestUtils.aceSetStructFields(deletion.acl().entry(), deletionStruct);
                    deletionStructs.add(deletionStruct);
                }
                responseStruct.set(MATCHING_ACLS, deletionStructs.toArray(new Struct[0]));
            }
            responseStructs.add(responseStruct);
        }
        struct.set(FILTER_RESPONSES, responseStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<AclFilterResponse> responses() {
        return responses;
    }

    public static DeleteAclsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteAclsResponse(ApiKeys.DELETE_ACLS.responseSchema(version).read(buffer));
    }

    public String toString() {
        return "(responses=" + Utils.join(responses, ",") + ")";
    }

}
