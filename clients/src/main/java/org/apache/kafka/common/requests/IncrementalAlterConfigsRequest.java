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

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class IncrementalAlterConfigsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<IncrementalAlterConfigsRequest> {
        private final IncrementalAlterConfigsRequestData data;

        public Builder(IncrementalAlterConfigsRequestData data) {
            super(ApiKeys.INCREMENTAL_ALTER_CONFIGS);
            this.data = data;
        }

        public Builder(final Collection<ConfigResource> resources,
                       final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                       final boolean validateOnly) {
            super(ApiKeys.INCREMENTAL_ALTER_CONFIGS);
            this.data = new IncrementalAlterConfigsRequestData()
                            .setValidateOnly(validateOnly);
            for (ConfigResource resource : resources) {
                IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigSet =
                    new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
                for (AlterConfigOp configEntry : configs.get(resource))
                    alterableConfigSet.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                                               .setName(configEntry.configEntry().name())
                                               .setValue(configEntry.configEntry().value())
                                               .setConfigOperation(configEntry.opType().id()));
                IncrementalAlterConfigsRequestData.AlterConfigsResource alterConfigsResource = new IncrementalAlterConfigsRequestData.AlterConfigsResource();
                alterConfigsResource.setResourceType(resource.type().id())
                    .setResourceName(resource.name()).setConfigs(alterableConfigSet);
                data.resources().add(alterConfigsResource);
            }
        }

        public Builder(final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                       final boolean validateOnly) {
            this(configs.keySet(), configs, validateOnly);
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

    public static IncrementalAlterConfigsRequest parse(ByteBuffer buffer, short version) {
        return new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public IncrementalAlterConfigsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(final int throttleTimeMs, final Throwable e) {
        IncrementalAlterConfigsResponseData response = new IncrementalAlterConfigsResponseData();
        ApiError apiError = ApiError.fromThrowable(e);
        for (AlterConfigsResource resource : data.resources()) {
            response.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName(resource.resourceName())
                    .setResourceType(resource.resourceType())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message()));
        }
        return new IncrementalAlterConfigsResponse(response);
    }
}
