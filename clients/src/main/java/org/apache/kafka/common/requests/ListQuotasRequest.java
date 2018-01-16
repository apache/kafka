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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.requests.DescribeQuotasRequest.unwrapQuotaConfigResource;
import static org.apache.kafka.common.requests.DescribeQuotasRequest.wrapQuotaConfigResource;

public class ListQuotasRequest extends AbstractRequest {

    private static final String QUOTA_CONFIG_RESOURCE_KEY_NAME = "quota_config_resource";
    private static final String TYPE_KEY_NAME = "type";
    private static final String NAME_KEY_NAME = "name";

    private static final Schema QUOTA_SCHEMA_V0 = new Schema(
            new Field(TYPE_KEY_NAME, INT8),
            new Field(NAME_KEY_NAME, NULLABLE_STRING)
    );

    private static final Schema LIST_QUOTAS_REQUEST_SCHEMA_V0 = new Schema(
            new Field(QUOTA_CONFIG_RESOURCE_KEY_NAME, QUOTA_SCHEMA_V0)
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{LIST_QUOTAS_REQUEST_SCHEMA_V0};
    }

    public static class Builder extends AbstractRequest.Builder {

        private final Resource quotaConfigResource;

        public Builder(Resource quotaConfigResource) {
            super(ApiKeys.LIST_QUOTAS);
            this.quotaConfigResource = quotaConfigResource;
        }

        @Override
        public ListQuotasRequest build(short version) {
            return new ListQuotasRequest(version, quotaConfigResource);
        }
    }

    private Resource quotaConfigResource;

    public Resource quotaConfigResource() {
        return quotaConfigResource;
    }

    public ListQuotasRequest(short version, Resource quotaConfigResource) {
        super(version);
        this.quotaConfigResource = quotaConfigResource;
    }

    public ListQuotasRequest(Struct struct, short version) {
        super(version);
        this.quotaConfigResource = unwrapQuotaConfigResource(struct.getStruct(QUOTA_CONFIG_RESOURCE_KEY_NAME));
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(LIST_QUOTAS_REQUEST_SCHEMA_V0);
        Struct quotaConfigEntity = wrapQuotaConfigResource(quotaConfigResource, struct, QUOTA_CONFIG_RESOURCE_KEY_NAME);
        struct.set(QUOTA_CONFIG_RESOURCE_KEY_NAME, quotaConfigEntity);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
                return new ListQuotasResponse(throttleTimeMs, ApiError.fromThrowable(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_QUOTAS.latestVersion()));
        }
    }
}
