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

import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateAclsResponse extends AbstractResponse {
    public static class AclCreationResponse {
        private final ApiError error;

        public AclCreationResponse(ApiError error) {
            this.error = error;
        }

        public ApiError error() {
            return error;
        }

        @Override
        public String toString() {
            return "(" + error + ")";
        }
    }

    private final CreateAclsResponseData data;

    public CreateAclsResponse(int throttleTimeMs, List<AclCreationResponse> aclCreationResponses) {
        this.data = new CreateAclsResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        List<CreateAclsResponseData.CreatableAclResult> results = new ArrayList<>(aclCreationResponses.size());
        data.setResults(results);
        for (AclCreationResponse response : aclCreationResponses) {
            results.add(new CreateAclsResponseData.CreatableAclResult()
                .setErrorCode(response.error.error().code())
                .setErrorMessage(response.error.message()));
        }
    }

    public CreateAclsResponse(Struct struct, short version) {
        this.data = new CreateAclsResponseData(struct, version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public List<AclCreationResponse> aclCreationResponses() {
        List<AclCreationResponse> aclCreationResponses = new ArrayList<>(data.results().size());
        for (CreateAclsResponseData.CreatableAclResult result : data.results()) {
            aclCreationResponses.add(new AclCreationResponse(
                new ApiError(Errors.forCode(result.errorCode()), result.errorMessage())));
        }
        return aclCreationResponses;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (AclCreationResponse response : aclCreationResponses())
            updateErrorCounts(errorCounts, response.error.error());
        return errorCounts;
    }

    public static CreateAclsResponse parse(ByteBuffer buffer, short version) {
        return new CreateAclsResponse(ApiKeys.CREATE_ACLS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
