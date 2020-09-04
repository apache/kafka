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

import org.apache.kafka.common.message.DescribeClientConfigsRequestData;
import org.apache.kafka.common.message.DescribeClientConfigsRequestData.ComponentData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

import java.util.ArrayList;
import java.util.List;

public class DescribeClientConfigsRequest extends AbstractRequest {
    // These values must not change.
    private static final byte MATCH_TYPE_EXACT = 0;
    private static final byte MATCH_TYPE_DEFAULT = 1;
    private static final byte MATCH_TYPE_SPECIFIED = 2;

    public static class Builder extends AbstractRequest.Builder<DescribeClientConfigsRequest> {

        private final DescribeClientConfigsRequestData data;

        public Builder(ClientQuotaFilter filter, List<String> supportedConfigs, boolean applicationRequest) {
            super(ApiKeys.DESCRIBE_CLIENT_CONFIGS);

            List<ComponentData> componentData = new ArrayList<>(filter.components().size());
            for (ClientQuotaFilterComponent component : filter.components()) {
                ComponentData fd = new ComponentData().setEntityType(component.entityType());
                if (component.match() == null) {
                    fd.setMatchType(MATCH_TYPE_SPECIFIED);
                    fd.setMatch(null);
                } else if (component.match().isPresent()) {
                    fd.setMatchType(MATCH_TYPE_EXACT);
                    fd.setMatch(component.match().get());
                } else {
                    fd.setMatchType(MATCH_TYPE_DEFAULT);
                    fd.setMatch(null);
                }
                componentData.add(fd);
            }
            this.data = new DescribeClientConfigsRequestData()
                .setComponents(componentData)
                .setStrict(filter.strict())
                .setApplicationRequest(applicationRequest)
                .setSupportedConfigs(supportedConfigs);
        }

        @Override
        public DescribeClientConfigsRequest build(short version) {
            return new DescribeClientConfigsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeClientConfigsRequestData data;

    public DescribeClientConfigsRequest(DescribeClientConfigsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_CLIENT_CONFIGS, version);
        this.data = data;
    }

    public DescribeClientConfigsRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_CLIENT_CONFIGS, version);
        this.data = new DescribeClientConfigsRequestData(struct, version);
    }

    public ClientQuotaFilter filter(String user) {
        List<ClientQuotaFilterComponent> components = new ArrayList<>(data.components().size());
        for (ComponentData componentData : data.components()) {
            ClientQuotaFilterComponent component;
            switch (componentData.matchType()) {
                case MATCH_TYPE_EXACT:
                    component = ClientQuotaFilterComponent.ofEntity(componentData.entityType(), componentData.match());
                    break;
                case MATCH_TYPE_DEFAULT:
                    component = ClientQuotaFilterComponent.ofDefaultEntity(componentData.entityType());
                    break;
                case MATCH_TYPE_SPECIFIED:
                    component = ClientQuotaFilterComponent.ofEntityType(componentData.entityType());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected match type: " + componentData.matchType());
            }
            components.add(component);
        }
        if (data.applicationRequest()) {
            components.add(ClientQuotaFilterComponent.ofEntity("user", user));
        }
        if (data.strict()) {
            return ClientQuotaFilter.containsOnly(components);
        } else {
            return ClientQuotaFilter.contains(components);
        }
    }

    public boolean applicationRequest() {
        return data.applicationRequest();
    }

    public List<String> supportedConfigs() {
        return data.supportedConfigs();
    }

    @Override
    public DescribeClientConfigsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeClientConfigsResponse(throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
