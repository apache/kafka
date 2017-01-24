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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ApiVersionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ApiVersionsRequest> {
        public Builder() {
            super(ApiKeys.API_VERSIONS);
        }

        @Override
        public ApiVersionsRequest build() {
            return new ApiVersionsRequest(version());
        }

        @Override
        public String toString() {
            return "(type=ApiVersionsRequest)";
        }
    }

    public ApiVersionsRequest(short version) {
        this(new Struct(ProtoUtils.requestSchema(ApiKeys.API_VERSIONS.id, version)),
                version);
    }

    public ApiVersionsRequest(Struct struct, short versionId) {
        super(struct, versionId);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                short errorCode = Errors.forException(e).code();
                return new ApiVersionsResponse(errorCode, Collections.<ApiVersionsResponse.ApiVersion>emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.API_VERSIONS.id)));
        }
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer, int versionId) {
        return new ApiVersionsRequest(
                ProtoUtils.parseRequest(ApiKeys.API_VERSIONS.id, versionId, buffer),
                (short) versionId);
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.API_VERSIONS.id));
    }
}
