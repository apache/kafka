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

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.Resource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.OPERATION;
import static org.apache.kafka.common.protocol.CommonFields.PERMISSION_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class DescribeAclsResponse extends AbstractResponse {
    private final static String RESOURCES_KEY_NAME = "resources";
    private final static String ACLS_KEY_NAME = "acls";

    private static final Schema DESCRIBE_ACLS_RESOURCE = new Schema(
            RESOURCE_TYPE,
            RESOURCE_NAME,
            new Field(ACLS_KEY_NAME, new ArrayOf(new Schema(
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    private static final Schema DESCRIBE_ACLS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_ACLS_RESOURCE), "The resources and their associated ACLs."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_ACLS_RESPONSE_V1 = DESCRIBE_ACLS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_ACLS_RESPONSE_V0, DESCRIBE_ACLS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final ApiError error;
    private final Collection<AclBinding> acls;

    public DescribeAclsResponse(int throttleTimeMs, ApiError error, Collection<AclBinding> acls) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.acls = acls;
    }

    public DescribeAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.error = new ApiError(struct);
        this.acls = new ArrayList<>();
        for (Object resourceStructObj : struct.getArray(RESOURCES_KEY_NAME)) {
            Struct resourceStruct = (Struct) resourceStructObj;
            Resource resource = RequestUtils.resourceFromStructFields(resourceStruct);
            for (Object aclDataStructObj : resourceStruct.getArray(ACLS_KEY_NAME)) {
                Struct aclDataStruct = (Struct) aclDataStructObj;
                AccessControlEntry entry = RequestUtils.aceFromStructFields(aclDataStruct);
                this.acls.add(new AclBinding(resource, entry));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        error.write(struct);

        Map<Resource, List<AccessControlEntry>> resourceToData = new HashMap<>();
        for (AclBinding acl : acls) {
            List<AccessControlEntry> entry = resourceToData.get(acl.resource());
            if (entry == null) {
                entry = new ArrayList<>();
                resourceToData.put(acl.resource(), entry);
            }
            entry.add(acl.entry());
        }

        List<Struct> resourceStructs = new ArrayList<>();
        for (Map.Entry<Resource, List<AccessControlEntry>> tuple : resourceToData.entrySet()) {
            Resource resource = tuple.getKey();
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);
            RequestUtils.resourceSetStructFields(resource, resourceStruct);
            List<Struct> dataStructs = new ArrayList<>();
            for (AccessControlEntry entry : tuple.getValue()) {
                Struct dataStruct = resourceStruct.instance(ACLS_KEY_NAME);
                RequestUtils.aceSetStructFields(entry, dataStruct);
                dataStructs.add(dataStruct);
            }
            resourceStruct.set(ACLS_KEY_NAME, dataStructs.toArray());
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public ApiError error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error.error());
    }

    public Collection<AclBinding> acls() {
        return acls;
    }

    public static DescribeAclsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeAclsResponse(ApiKeys.DESCRIBE_ACLS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
