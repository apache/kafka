/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.Acl;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.Operation;
import org.apache.kafka.common.security.auth.PermissionType;
import org.apache.kafka.common.security.auth.Resource;
import org.apache.kafka.common.security.auth.ResourceType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ListAclsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.LIST_ACLS.id);

    public static final String RESPONSES_KEY_NAME = "responses";
    public static final String ERROR_CODE_KEY_NAME = "error_code";

    public static final String RESOURCE_KEY_NAME = "resource";
    public static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    public static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    public static final String ACLS_KEY_NAME = "acls";
    public static final String ACL_PRINCIPLE_KEY_NAME = "acl_principle";
    public static final String ACL_PERMISSION_TYPE_KEY_NAME = "acl_permission_type";
    public static final String ACL_HOST_KEY_NAME = "acl_host";
    public static final String ACL_OPERATION_KEY_NAME = "acl_operation";

    /**
     * Possible error codes:
     *
     * CLUSTER_AUTHORIZATION_FAILED(31)
     */

    private final Map<Resource, Set<Acl>> acls;
    private final Errors error;

    public ListAclsResponse(Map<Resource, Set<Acl>> acls, Errors error) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> responses = new ArrayList<>();
        for (Map.Entry<Resource, Set<Acl>> aclEntry : acls.entrySet()) {
            Struct response = struct.instance(RESPONSES_KEY_NAME);

            Struct resource = response.instance(RESOURCE_KEY_NAME);
            resource.set(RESOURCE_TYPE_KEY_NAME, aclEntry.getKey().getResourceType().id);
            resource.set(RESOURCE_NAME_KEY_NAME, aclEntry.getKey().getName());

            Set<Struct> aclsSet = new HashSet<>();
            for (Acl acl : aclEntry.getValue()) {
                Struct aclStruct = response.instance(ACLS_KEY_NAME);
                aclStruct.set(ACL_PRINCIPLE_KEY_NAME, acl.getPrincipal().toString());
                aclStruct.set(ACL_PERMISSION_TYPE_KEY_NAME, acl.getPermissionType().id);
                aclStruct.set(ACL_HOST_KEY_NAME, acl.getHost());
                aclStruct.set(ACL_OPERATION_KEY_NAME, acl.getOperation().id);

                aclsSet.add(aclStruct);
            }

            response.set(RESOURCE_KEY_NAME, resource);
            response.set(ACLS_KEY_NAME, aclsSet.toArray());

            responses.add(response);
        }

        struct.set(RESPONSES_KEY_NAME, responses.toArray());
        struct.set(ERROR_CODE_KEY_NAME, error.code());

        this.acls = acls;
        this.error = error;
    }

    public ListAclsResponse(Struct struct) {
        super(struct);

        Map<Resource, Set<Acl>> acls = new HashMap<>();

        Object[] responses = (Object[]) struct.get(RESPONSES_KEY_NAME);
        for (Object responseObj : responses) {
            Struct response = (Struct) responseObj;

            Struct resourceStruct = response.getStruct(RESOURCE_KEY_NAME);
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            Resource resource = new Resource(resourceType, resourceName);

            Set<Acl> aclList = new HashSet<>();
            Object[] aclArray = (Object[]) response.get(ACLS_KEY_NAME);
            for (Object aclObj : aclArray) {
                Struct aclStruct = (Struct) aclObj;
                KafkaPrincipal principal = KafkaPrincipal.fromString(aclStruct.getString(ACL_PRINCIPLE_KEY_NAME));
                PermissionType permissionType = PermissionType.forId(aclStruct.getByte(ACL_PERMISSION_TYPE_KEY_NAME));
                String host = aclStruct.getString(ACL_HOST_KEY_NAME);
                Operation operation = Operation.forId(aclStruct.getByte(ACL_OPERATION_KEY_NAME));
                Acl acl = new Acl(principal, permissionType, host, operation);

                aclList.add(acl);
            }
            acls.put(resource, aclList);
        }

        this.acls = acls;
        this.error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
    }

    public Errors error() {
        return this.error;
    }

    public Map<Resource, Set<Acl>> acls() {
        return this.acls;
    }

    public static ListAclsResponse parse(ByteBuffer buffer) {
        return new ListAclsResponse(CURRENT_SCHEMA.read(buffer));
    }
}
