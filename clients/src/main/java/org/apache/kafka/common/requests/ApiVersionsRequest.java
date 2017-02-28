/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ApiVersionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ApiVersionsRequest> {

        public Builder() {
            super(ApiKeys.API_VERSIONS);
        }

        @Override
        public ApiVersionsRequest build(short version) {
            return new ApiVersionsRequest(version);
        }

        @Override
        public String toString() {
            return "(type=ApiVersionsRequest)";
        }
    }

    public ApiVersionsRequest(short version) {
        super(version);
    }

    public ApiVersionsRequest(Struct struct, short version) {
        super(version);
    }

    @Override
    protected Struct toStruct() {
        return new Struct(ApiKeys.API_VERSIONS.requestSchema(version()));
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new ApiVersionsResponse(Errors.forException(e), Collections.<ApiVersionsResponse.ApiVersion>emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.API_VERSIONS.latestVersion()));
        }
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer, short version) {
        return new ApiVersionsRequest(ApiKeys.API_VERSIONS.parseRequest(version, buffer), version);
    }

}
