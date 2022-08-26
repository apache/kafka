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

import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData.ComponentData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DescribeClientQuotasRequest extends AbstractRequest {
    // These values must not change.
    public static final byte MATCH_TYPE_EXACT = 0;
    public static final byte MATCH_TYPE_DEFAULT = 1;
    public static final byte MATCH_TYPE_SPECIFIED = 2;

    public static class Builder extends AbstractRequest.Builder<DescribeClientQuotasRequest> {

        private final DescribeClientQuotasRequestData data;

        public Builder(ClientQuotaFilter filter) {
            super(ApiKeys.DESCRIBE_CLIENT_QUOTAS);

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
            this.data = new DescribeClientQuotasRequestData()
                .setComponents(componentData)
                .setStrict(filter.strict());
        }

        @Override
        public DescribeClientQuotasRequest build(short version) {
            return new DescribeClientQuotasRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeClientQuotasRequestData data;

    public DescribeClientQuotasRequest(DescribeClientQuotasRequestData data, short version) {
        super(ApiKeys.DESCRIBE_CLIENT_QUOTAS, version);
        this.data = data;
    }

    public ClientQuotaFilter filter() {
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
        if (data.strict()) {
            return ClientQuotaFilter.containsOnly(components);
        } else {
            return ClientQuotaFilter.contains(components);
        }
    }

    @Override
    public DescribeClientQuotasRequestData data() {
        return data;
    }

    @Override
    public DescribeClientQuotasResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        return new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message())
            .setEntries(null));
    }

    public static DescribeClientQuotasRequest parse(ByteBuffer buffer, short version) {
        return new DescribeClientQuotasRequest(new DescribeClientQuotasRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }

}
