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
import org.apache.kafka.common.message.DescribeClientQuotasRequestData.FilterData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.quota.QuotaFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

public class DescribeClientQuotasRequest extends AbstractRequest {
    // These values must not change.
    private static final byte MATCH_TYPE_EXACT = 0;
    private static final byte MATCH_TYPE_SPECIFIED = 1;

    public static class Builder extends AbstractRequest.Builder<DescribeClientQuotasRequest> {

        private final DescribeClientQuotasRequestData data;

        public Builder(Collection<QuotaFilter> filters, boolean includeUnspecifiedTypes) {
            super(ApiKeys.DESCRIBE_CLIENT_QUOTAS);

            List<FilterData> filterData = new ArrayList<>(filters.size());
            for (QuotaFilter filter : filters) {
                FilterData fd = new FilterData().setEntityType(filter.entityType());
                if (filter.isMatchSpecified()) {
                    fd.setMatchType(MATCH_TYPE_SPECIFIED);
                    fd.setMatch(null);
                } else {
                    fd.setMatchType(MATCH_TYPE_EXACT);
                    fd.setMatch(filter.match());
                }
                filterData.add(fd);
            }
            this.data = new DescribeClientQuotasRequestData()
                .setFilters(filterData)
                .setIncludeUnspecifiedTypes(includeUnspecifiedTypes);
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

    public DescribeClientQuotasRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_CLIENT_QUOTAS, version);
        this.data = new DescribeClientQuotasRequestData(struct, version);
    }

    public Collection<QuotaFilter> filters() {
        List<QuotaFilter> filters = new ArrayList<>(data.filters().size());
        for (FilterData filterData : data.filters()) {
            QuotaFilter filter;
            switch (filterData.matchType()) {
                case MATCH_TYPE_EXACT:
                    filter = QuotaFilter.matchExact(filterData.entityType(), filterData.match());
                    break;
                case MATCH_TYPE_SPECIFIED:
                    filter = QuotaFilter.matchSpecified(filterData.entityType());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected match type: " + filterData.matchType());
            }
            filters.add(filter);
        }
        return filters;
    }

    public boolean includeUnspecifiedTypes() {
        return data.includeUnspecifiedTypes();
    }

    @Override
    public DescribeClientQuotasResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeClientQuotasResponse(throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
