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

import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class IncrementalAlterConfigsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<IncrementalAlterConfigsRequest> {
        private final IncrementalAlterConfigsRequestData data;

        public Builder(IncrementalAlterConfigsRequestData data) {
            super(ApiKeys.INCREMENTAL_ALTER_CONFIGS);
            this.data = data;
        }

        @Override
        public IncrementalAlterConfigsRequest build(short version) {
            return new IncrementalAlterConfigsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final IncrementalAlterConfigsRequestData data;
    private final short version;

    private IncrementalAlterConfigsRequest(IncrementalAlterConfigsRequestData data, short version) {
        super(ApiKeys.INCREMENTAL_ALTER_CONFIGS, version);
        this.data = data;
        this.version = version;
    }

    IncrementalAlterConfigsRequest(final Struct struct, final short version) {
        super(ApiKeys.INCREMENTAL_ALTER_CONFIGS, version);
        this.data = new IncrementalAlterConfigsRequestData(struct, version);
        this.version = version;
    }

    public static IncrementalAlterConfigsRequest parse(ByteBuffer buffer, short version) {
        return new IncrementalAlterConfigsRequest(ApiKeys.INCREMENTAL_ALTER_CONFIGS.parseRequest(version, buffer), version);
    }

    public IncrementalAlterConfigsRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AbstractResponse getErrorResponse(final int throttleTimeMs, final Throwable e) {
        IncrementalAlterConfigsResponseData response = new IncrementalAlterConfigsResponseData();
        ApiError apiError = ApiError.fromThrowable(e);
        for (AlterConfigsResource resource : data.resources()) {
            response.responses().add(new AlterConfigsResourceResult()
                    .setResourceName(resource.resourceName())
                    .setResourceType(resource.resourceType())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message()));
        }
        return new IncrementalAlterConfigsResponse(response);
    }
}
