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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.Resource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeAclsResponse extends AbstractResponse {
    private final static String RESOURCES = "resources";
    private final static String ACLS = "acls";

    private final int throttleTimeMs;
    private final ApiError error;
    private final Collection<AclBinding> acls;

    public DescribeAclsResponse(int throttleTimeMs, ApiError error, Collection<AclBinding> acls) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.acls = acls;
    }

    public DescribeAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.getInt(THROTTLE_TIME_KEY_NAME);
        this.error = new ApiError(struct);
        this.acls = new ArrayList<>();
        for (Object resourceStructObj : struct.getArray(RESOURCES)) {
            Struct resourceStruct = (Struct) resourceStructObj;
            Resource resource = RequestUtils.resourceFromStructFields(resourceStruct);
            for (Object aclDataStructObj : resourceStruct.getArray(ACLS)) {
                Struct aclDataStruct = (Struct) aclDataStructObj;
                AccessControlEntry entry = RequestUtils.aceFromStructFields(aclDataStruct);
                this.acls.add(new AclBinding(resource, entry));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
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
            Struct resourceStruct = struct.instance(RESOURCES);
            RequestUtils.resourceSetStructFields(resource, resourceStruct);
            List<Struct> dataStructs = new ArrayList<>();
            for (AccessControlEntry entry : tuple.getValue()) {
                Struct dataStruct = resourceStruct.instance(ACLS);
                RequestUtils.aceSetStructFields(entry, dataStruct);
                dataStructs.add(dataStruct);
            }
            resourceStruct.set(ACLS, dataStructs.toArray());
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES, resourceStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public ApiError error() {
        return error;
    }

    public Collection<AclBinding> acls() {
        return acls;
    }

    public static DescribeAclsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeAclsResponse(ApiKeys.DESCRIBE_ACLS.responseSchema(version).read(buffer));
    }
}
