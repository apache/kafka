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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription;
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource;
import org.apache.kafka.common.resource.ResourceType;

public class DescribeAclsResponse extends AbstractResponse {

    private final DescribeAclsResponseData data;

    public DescribeAclsResponse(DescribeAclsResponseData data, short version) {
        super(ApiKeys.DESCRIBE_ACLS);
        this.data = data;
        validate(Optional.of(version));
    }

    // Skips version validation, visible for testing
    DescribeAclsResponse(DescribeAclsResponseData data) {
        super(ApiKeys.DESCRIBE_ACLS);
        this.data = data;
        validate(Optional.empty());
    }

    @Override
    public DescribeAclsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public ApiError error() {
        return new ApiError(Errors.forCode(data.errorCode()), data.errorMessage());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public List<DescribeAclsResource> acls() {
        return data.resources();
    }

    public static DescribeAclsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeAclsResponse(new DescribeAclsResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    private void validate(Optional<Short> version) {
        if (version.isPresent() && version.get() == 0) {
            final boolean unsupported = acls().stream()
                .anyMatch(acl -> acl.patternType() != PatternType.LITERAL.code());
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        for (DescribeAclsResource resource : acls()) {
            if (resource.patternType() == PatternType.UNKNOWN.code() || resource.resourceType() == ResourceType.UNKNOWN.code())
                throw new IllegalArgumentException("Contain UNKNOWN elements");
            for (AclDescription acl : resource.acls()) {
                if (acl.operation() == AclOperation.UNKNOWN.code() || acl.permissionType() == AclPermissionType.UNKNOWN.code()) {
                    throw new IllegalArgumentException("Contain UNKNOWN elements");
                }
            }
        }
    }

    private static Stream<AclBinding> aclBindings(DescribeAclsResource resource) {
        return resource.acls().stream().map(acl -> {
            ResourcePattern pattern = new ResourcePattern(
                    ResourceType.fromCode(resource.resourceType()),
                    resource.resourceName(),
                    PatternType.fromCode(resource.patternType()));
            AccessControlEntry entry = new AccessControlEntry(
                    acl.principal(),
                    acl.host(),
                    AclOperation.fromCode(acl.operation()),
                    AclPermissionType.fromCode(acl.permissionType()));
            return new AclBinding(pattern, entry);
        });
    }

    public static List<AclBinding> aclBindings(List<DescribeAclsResource> resources) {
        return resources.stream().flatMap(DescribeAclsResponse::aclBindings).collect(Collectors.toList());
    }

    public static List<DescribeAclsResource> aclsResources(Collection<AclBinding> acls) {
        Map<ResourcePattern, List<AccessControlEntry>> patternToEntries = new HashMap<>();
        for (AclBinding acl : acls) {
            patternToEntries.computeIfAbsent(acl.pattern(), v -> new ArrayList<>()).add(acl.entry());
        }
        List<DescribeAclsResource> resources = new ArrayList<>(patternToEntries.size());
        for (Entry<ResourcePattern, List<AccessControlEntry>> entry : patternToEntries.entrySet()) {
            ResourcePattern key = entry.getKey();
            List<AclDescription> aclDescriptions = new ArrayList<>();
            for (AccessControlEntry ace : entry.getValue()) {
                AclDescription ad = new AclDescription()
                    .setHost(ace.host())
                    .setOperation(ace.operation().code())
                    .setPermissionType(ace.permissionType().code())
                    .setPrincipal(ace.principal());
                aclDescriptions.add(ad);
            }
            DescribeAclsResource dar = new DescribeAclsResource()
                .setResourceName(key.name())
                .setPatternType(key.patternType().code())
                .setResourceType(key.resourceType().code())
                .setAcls(aclDescriptions);
            resources.add(dar);
        }
        return resources;
    }
}
