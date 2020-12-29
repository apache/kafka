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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AlterConfigsRequest extends AbstractRequest {

    public static class Config {
        private final Collection<ConfigEntry> entries;

        public Config(Collection<ConfigEntry> entries) {
            this.entries = Objects.requireNonNull(entries, "entries");
        }

        public Collection<ConfigEntry> entries() {
            return entries;
        }
    }

    public static class ConfigEntry {
        private final String name;
        private final String value;

        public ConfigEntry(String name, String value) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = Objects.requireNonNull(value, "value");
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

    }

    public static class Builder extends AbstractRequest.Builder<AlterConfigsRequest> {

        private final AlterConfigsRequestData data = new AlterConfigsRequestData();

        public Builder(Map<ConfigResource, Config> configs, boolean validateOnly) {
            super(ApiKeys.ALTER_CONFIGS);
            Objects.requireNonNull(configs, "configs");
            for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
                AlterConfigsRequestData.AlterConfigsResource resource =
                    new AlterConfigsRequestData.AlterConfigsResource()
                        .setResourceName(entry.getKey().name())
                        .setResourceType(entry.getKey().type().id());
                for (ConfigEntry x : entry.getValue().entries) {
                    resource.configs().add(new AlterConfigsRequestData.AlterableConfig()
                                               .setName(x.name())
                                               .setValue(x.value()));
                }
                this.data.resources().add(resource);
            }
            this.data.setValidateOnly(validateOnly);
        }

        @Override
        public AlterConfigsRequest build(short version) {
            return new AlterConfigsRequest(data, version);
        }
    }

    private final AlterConfigsRequestData data;

    public AlterConfigsRequest(AlterConfigsRequestData data, short version) {
        super(ApiKeys.ALTER_CONFIGS, version);
        this.data = data;
    }

    public Map<ConfigResource, Config> configs() {
        return data.resources().stream().collect(Collectors.toMap(
            resource -> new ConfigResource(
                    ConfigResource.Type.forId(resource.resourceType()),
                    resource.resourceName()),
            resource -> new Config(resource.configs().stream()
                    .map(entry -> new ConfigEntry(entry.name(), entry.value()))
                    .collect(Collectors.toList()))));
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    @Override
    public AlterConfigsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        AlterConfigsResponseData data = new AlterConfigsResponseData()
                .setThrottleTimeMs(throttleTimeMs);
        for (AlterConfigsRequestData.AlterConfigsResource resource : this.data.resources()) {
            data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                    .setResourceType(resource.resourceType())
                    .setResourceName(resource.resourceName())
                    .setErrorMessage(error.message())
                    .setErrorCode(error.error().code()));
        }
        return new AlterConfigsResponse(data);

    }

    public static AlterConfigsRequest parse(ByteBuffer buffer, short version) {
        return new AlterConfigsRequest(new AlterConfigsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
