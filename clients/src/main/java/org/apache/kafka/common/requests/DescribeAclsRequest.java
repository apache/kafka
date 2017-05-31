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

import org.apache.kafka.clients.admin.AccessControlEntryFilter;
import org.apache.kafka.clients.admin.AclBinding;
import org.apache.kafka.clients.admin.AclBindingFilter;
import org.apache.kafka.clients.admin.ResourceFilter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class DescribeAclsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeAclsRequest> {
        private final AclBindingFilter filter;

        public Builder(AclBindingFilter filter) {
            super(ApiKeys.DESCRIBE_ACLS);
            this.filter = filter;
        }

        @Override
        public DescribeAclsRequest build(short version) {
            return new DescribeAclsRequest(filter, version);
        }

        @Override
        public String toString() {
            return "(type=DescribeAclsRequest, filter=" + filter + ")";
        }
    }

    private final AclBindingFilter filter;

    DescribeAclsRequest(AclBindingFilter filter, short version) {
        super(version);
        this.filter = filter;
    }

    public DescribeAclsRequest(Struct struct, short version) {
        super(version);
        ResourceFilter resourceFilter = RequestUtils.resourceFilterFromStructFields(struct);
        AccessControlEntryFilter entryFilter = RequestUtils.aceFilterFromStructFields(struct);
        this.filter = new AclBindingFilter(resourceFilter, entryFilter);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_ACLS.requestSchema(version()));
        RequestUtils.resourceFilterSetStructFields(filter.resourceFilter(), struct);
        RequestUtils.aceFilterSetStructFields(filter.entryFilter(), struct);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new DescribeAclsResponse(throttleTimeMs, throwable, Collections.<AclBinding>emptySet());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_ACLS.latestVersion()));
        }
    }

    public static DescribeAclsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeAclsRequest(ApiKeys.DESCRIBE_ACLS.parseRequest(version, buffer), version);
    }

    public AclBindingFilter filter() {
        return filter;
    }
}
