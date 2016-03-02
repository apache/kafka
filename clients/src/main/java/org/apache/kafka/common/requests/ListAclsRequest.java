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
import org.apache.kafka.common.security.auth.Resource;
import org.apache.kafka.common.security.auth.ResourceType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

public class ListAclsRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.LIST_ACLS.id);

    public static final String PRINCIPLE_KEY_NAME = "principal";

    public static final String RESOURCE_KEY_NAME = "resource";
    public static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    public static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    public static final Byte ALL_RESOURCE_TYPE_ID = -1;

    private final KafkaPrincipal principal;
    private final Resource resource;

    public ListAclsRequest() {
        this(null, null);
    }

    public ListAclsRequest(KafkaPrincipal principal) {
        this(principal, null);
    }

    public ListAclsRequest(Resource resource) {
        this(null, resource);
    }

    public ListAclsRequest(KafkaPrincipal principal, Resource resource) {
        super(new Struct(CURRENT_SCHEMA));

        if (principal == null) {
            struct.set(PRINCIPLE_KEY_NAME, null);
        } else {
            struct.set(PRINCIPLE_KEY_NAME, principal.toString());
        }

        Struct resourceStruct = struct.instance(RESOURCE_KEY_NAME);
        if (resource == null) {
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, ALL_RESOURCE_TYPE_ID);
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, "");
        } else {
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.getResourceType().id);
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.getName());
        }
        struct.set(RESOURCE_KEY_NAME, resourceStruct);

        this.principal = principal;
        this.resource = resource;
    }

    public ListAclsRequest(Struct struct) {
        super(struct);

        String principleStr = struct.getString(PRINCIPLE_KEY_NAME);
        if (principleStr == null) {
            this.principal = null;
        } else {
            this.principal = KafkaPrincipal.fromString(struct.getString(PRINCIPLE_KEY_NAME));
        }

        Struct resourceStruct = struct.getStruct(RESOURCE_KEY_NAME);
        byte resourceTypeId = resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME);
        if (resourceTypeId == ALL_RESOURCE_TYPE_ID) {
            this.resource = null;
        } else {
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            this.resource = new Resource(resourceType, resourceName);
        }
    }

    public boolean hasPrincipal() {
        return principal != null;
    }

    public KafkaPrincipal getPrincipal() {
        return principal;
    }

    public boolean hasResource() {
        return resource != null;
    }

    public Resource getResource() {
        return resource;
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new ListAclsResponse(Collections.<Resource, Set<Acl>>emptyMap(), Errors.forException(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.LIST_ACLS.id)));
        }
    }

    public static ListAclsRequest parse(ByteBuffer buffer, int versionId) {
        return new ListAclsRequest(ProtoUtils.parseRequest(ApiKeys.LIST_ACLS.id, versionId, buffer));
    }

    public static ListAclsRequest parse(ByteBuffer buffer) {
        return new ListAclsRequest(CURRENT_SCHEMA.read(buffer));
    }


}
