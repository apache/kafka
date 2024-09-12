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

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.nio.ByteBuffer;

public class DescribeAclsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeAclsRequest> {
        private final DescribeAclsRequestData data;

        public Builder(AclBindingFilter filter) {
            super(ApiKeys.DESCRIBE_ACLS);
            ResourcePatternFilter patternFilter = filter.patternFilter();
            AccessControlEntryFilter entryFilter = filter.entryFilter();
            data = new DescribeAclsRequestData()
                .setHostFilter(entryFilter.host())
                .setOperation(entryFilter.operation().code())
                .setPermissionType(entryFilter.permissionType().code())
                .setPrincipalFilter(entryFilter.principal())
                .setResourceNameFilter(patternFilter.name())
                .setPatternTypeFilter(patternFilter.patternType().code())
                .setResourceTypeFilter(patternFilter.resourceType().code());
        }

        @Override
        public DescribeAclsRequest build(short version) {
            return new DescribeAclsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeAclsRequestData data;

    private DescribeAclsRequest(DescribeAclsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_ACLS, version);
        this.data = data;
        normalizeAndValidate(version);
    }

    private void normalizeAndValidate(short version) {
        if (version == 0) {
            PatternType patternType = PatternType.fromCode(data.patternTypeFilter());
            // On older brokers, no pattern types existed except LITERAL (effectively). So even though ANY is not
            // directly supported on those brokers, we can get the same effect as ANY by setting the pattern type
            // to LITERAL. Note that the wildcard `*` is considered `LITERAL` for compatibility reasons.
            if (patternType == PatternType.ANY)
                data.setPatternTypeFilter(PatternType.LITERAL.code());
            else if (patternType != PatternType.LITERAL)
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
        }

        if (data.patternTypeFilter() == PatternType.UNKNOWN.code()
                || data.resourceTypeFilter() == ResourceType.UNKNOWN.code()
                || data.permissionType() == AclPermissionType.UNKNOWN.code()
                || data.operation() == AclOperation.UNKNOWN.code()) {
            throw new IllegalArgumentException("DescribeAclsRequest contains UNKNOWN elements: " + data);
        }
    }

    @Override
    public DescribeAclsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        ApiError error = ApiError.fromThrowable(throwable);
        DescribeAclsResponseData response = new DescribeAclsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message());
        return new DescribeAclsResponse(response, version());
    }

    public static DescribeAclsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeAclsRequest(new DescribeAclsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public AclBindingFilter filter() {
        ResourcePatternFilter rpf = new ResourcePatternFilter(
                ResourceType.fromCode(data.resourceTypeFilter()),
                data.resourceNameFilter(),
                PatternType.fromCode(data.patternTypeFilter()));
        AccessControlEntryFilter acef =  new AccessControlEntryFilter(
                data.principalFilter(),
                data.hostFilter(),
                AclOperation.fromCode(data.operation()),
                AclPermissionType.fromCode(data.permissionType()));
        return new AclBindingFilter(rpf, acef);
    }

}
