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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateAclsResponse extends AbstractResponse {
    private final static String THROTTLE_TIME_MS = "throttle_time_ms";
    private final static String CREATION_RESPONSES = "creation_responses";
    private final static String ERROR_CODE = "error_code";
    private final static String ERROR_MESSAGE = "error_message";

    public static class AclCreationResponse {
        private final Throwable throwable;

        public AclCreationResponse(Throwable throwable) {
            this.throwable = throwable;
        }

        public Throwable throwable() {
            return throwable;
        }

        @Override
        public String toString() {
            return "(" + throwable + ")";
        }
    }

    private final int throttleTimeMs;

    private final List<AclCreationResponse> aclCreationResponses;

    public CreateAclsResponse(int throttleTimeMs, List<AclCreationResponse> aclCreationResponses) {
        this.throttleTimeMs = throttleTimeMs;
        this.aclCreationResponses = aclCreationResponses;
    }

    public CreateAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.getInt(THROTTLE_TIME_MS);
        this.aclCreationResponses = new ArrayList<>();
        for (Object responseStructObj : struct.getArray(CREATION_RESPONSES)) {
            Struct responseStruct = (Struct) responseStructObj;
            short errorCode = responseStruct.getShort(ERROR_CODE);
            String errorMessage = responseStruct.getString(ERROR_MESSAGE);
            if (errorCode != 0) {
                this.aclCreationResponses.add(new AclCreationResponse(
                        Errors.forCode(errorCode).exception(errorMessage)));
            } else {
                this.aclCreationResponses.add(new AclCreationResponse(null));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> responseStructs = new ArrayList<>();
        for (AclCreationResponse response : aclCreationResponses) {
            Struct responseStruct = struct.instance(CREATION_RESPONSES);
            if (response.throwable() == null) {
                responseStruct.set(ERROR_CODE, (short) 0);
            } else {
                Errors errors = Errors.forException(response.throwable());
                responseStruct.set(ERROR_CODE, errors.code());
                responseStruct.set(ERROR_MESSAGE, response.throwable().getMessage());
            }
            responseStructs.add(responseStruct);
        }
        struct.set(CREATION_RESPONSES, responseStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<AclCreationResponse> aclCreationResponses() {
        return aclCreationResponses;
    }

    public static CreateAclsResponse parse(ByteBuffer buffer, short version) {
        return new CreateAclsResponse(ApiKeys.CREATE_ACLS.responseSchema(version).read(buffer));
    }
}
