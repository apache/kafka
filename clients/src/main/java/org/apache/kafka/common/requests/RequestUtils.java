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
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.resource.ResourceType;

import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.HOST_FILTER;
import static org.apache.kafka.common.protocol.CommonFields.OPERATION;
import static org.apache.kafka.common.protocol.CommonFields.PERMISSION_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL;
import static org.apache.kafka.common.protocol.CommonFields.PRINCIPAL_FILTER;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME_FILTER;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME_TYPE;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_NAME_TYPE_FILTER;
import static org.apache.kafka.common.protocol.CommonFields.RESOURCE_TYPE;

final class RequestUtils {

    private RequestUtils() {}

    static ResourcePattern resourcePatternromStructFields(Struct struct) {
        byte resourceType = struct.get(RESOURCE_TYPE);
        String name = struct.get(RESOURCE_NAME);
        ResourceNameType resourceNameType = ResourceNameType.fromCode(
            struct.getOrElse(RESOURCE_NAME_TYPE, ResourceNameType.LITERAL.code()));
        return new ResourcePattern(ResourceType.fromCode(resourceType), name, resourceNameType);
    }

    static void resourcePatternSetStructFields(ResourcePattern pattern, Struct struct) {
        struct.set(RESOURCE_TYPE, pattern.resourceType().code());
        struct.set(RESOURCE_NAME, pattern.name());
        struct.setIfExists(RESOURCE_NAME_TYPE, pattern.nameType().code());
    }

    static ResourcePatternFilter resourcePatternFilterFromStructFields(Struct struct) {
        byte resourceType = struct.get(RESOURCE_TYPE);
        String name = struct.get(RESOURCE_NAME_FILTER);
        ResourceNameType resourceNameType = ResourceNameType.fromCode(
            struct.getOrElse(RESOURCE_NAME_TYPE_FILTER, ResourceNameType.LITERAL.code()));
        return new ResourcePatternFilter(ResourceType.fromCode(resourceType), name, resourceNameType);
    }

    static void resourcePatternFilterSetStructFields(ResourcePatternFilter patternFilter, Struct struct) {
        struct.set(RESOURCE_TYPE, patternFilter.resourceType().code());
        struct.set(RESOURCE_NAME_FILTER, patternFilter.name());
        struct.setIfExists(RESOURCE_NAME_TYPE_FILTER, patternFilter.nameType().code());
    }

    static AccessControlEntry aceFromStructFields(Struct struct) {
        String principal = struct.get(PRINCIPAL);
        String host = struct.get(HOST);
        byte operation = struct.get(OPERATION);
        byte permissionType = struct.get(PERMISSION_TYPE);
        return new AccessControlEntry(principal, host, AclOperation.fromCode(operation),
            AclPermissionType.fromCode(permissionType));
    }

    static void aceSetStructFields(AccessControlEntry data, Struct struct) {
        struct.set(PRINCIPAL, data.principal());
        struct.set(HOST, data.host());
        struct.set(OPERATION, data.operation().code());
        struct.set(PERMISSION_TYPE, data.permissionType().code());
    }

    static AccessControlEntryFilter aceFilterFromStructFields(Struct struct) {
        String principal = struct.get(PRINCIPAL_FILTER);
        String host = struct.get(HOST_FILTER);
        byte operation = struct.get(OPERATION);
        byte permissionType = struct.get(PERMISSION_TYPE);
        return new AccessControlEntryFilter(principal, host, AclOperation.fromCode(operation),
            AclPermissionType.fromCode(permissionType));
    }

    static void aceFilterSetStructFields(AccessControlEntryFilter filter, Struct struct) {
        struct.set(PRINCIPAL_FILTER, filter.principal());
        struct.set(HOST_FILTER, filter.host());
        struct.set(OPERATION, filter.operation().code());
        struct.set(PERMISSION_TYPE, filter.permissionType().code());
    }
}
