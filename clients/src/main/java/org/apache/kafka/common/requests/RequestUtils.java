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
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceType;

class RequestUtils {
    static Resource resourceFromStructFields(Struct struct) {
        byte resourceType = struct.getByte("resource_type");
        String name = struct.getString("resource_name");
        return new Resource(ResourceType.fromCode(resourceType), name);
    }

    static void resourceSetStructFields(Resource resource, Struct struct) {
        struct.set("resource_type", resource.resourceType().code());
        struct.set("resource_name", resource.name());
    }

    static ResourceFilter resourceFilterFromStructFields(Struct struct) {
        byte resourceType = struct.getByte("resource_type");
        String name = struct.getString("resource_name");
        return new ResourceFilter(ResourceType.fromCode(resourceType), name);
    }

    static void resourceFilterSetStructFields(ResourceFilter resourceFilter, Struct struct) {
        struct.set("resource_type", resourceFilter.resourceType().code());
        struct.set("resource_name", resourceFilter.name());
    }

    static AccessControlEntry aceFromStructFields(Struct struct) {
        String principal = struct.getString("principal");
        String host = struct.getString("host");
        byte operation = struct.getByte("operation");
        byte permissionType = struct.getByte("permission_type");
        return new AccessControlEntry(principal, host, AclOperation.fromCode(operation),
            AclPermissionType.fromCode(permissionType));
    }

    static void aceSetStructFields(AccessControlEntry data, Struct struct) {
        struct.set("principal", data.principal());
        struct.set("host", data.host());
        struct.set("operation", data.operation().code());
        struct.set("permission_type", data.permissionType().code());
    }

    static AccessControlEntryFilter aceFilterFromStructFields(Struct struct) {
        String principal = struct.getString("principal");
        String host = struct.getString("host");
        byte operation = struct.getByte("operation");
        byte permissionType = struct.getByte("permission_type");
        return new AccessControlEntryFilter(principal, host, AclOperation.fromCode(operation),
            AclPermissionType.fromCode(permissionType));
    }

    static void aceFilterSetStructFields(AccessControlEntryFilter filter, Struct struct) {
        struct.set("principal", filter.principal());
        struct.set("host", filter.host());
        struct.set("operation", filter.operation().code());
        struct.set("permission_type", filter.permissionType().code());
    }
}
