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
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription;
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeAclsResponseTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclDescription ALLOW_CREATE_ACL = buildAclDescription(
            "127.0.0.1",
            "User:ANONYMOUS",
            AclOperation.CREATE,
            AclPermissionType.ALLOW);

    private static final AclDescription DENY_READ_ACL = buildAclDescription(
            "127.0.0.1",
            "User:ANONYMOUS",
            AclOperation.READ,
            AclPermissionType.DENY);

    private static final DescribeAclsResource UNKNOWN_ACL = buildResource(
            "foo",
            ResourceType.UNKNOWN,
            PatternType.LITERAL,
            Collections.singletonList(DENY_READ_ACL));

    private static final DescribeAclsResource PREFIXED_ACL1 = buildResource(
            "prefix",
            ResourceType.GROUP,
            PatternType.PREFIXED,
            Collections.singletonList(ALLOW_CREATE_ACL));

    private static final DescribeAclsResource LITERAL_ACL1 = buildResource(
            "foo",
            ResourceType.TOPIC,
            PatternType.LITERAL,
            Collections.singletonList(ALLOW_CREATE_ACL));

    private static final DescribeAclsResource LITERAL_ACL2 = buildResource(
            "group",
            ResourceType.GROUP,
            PatternType.LITERAL,
            Collections.singletonList(DENY_READ_ACL));

    @Test
    public void shouldThrowOnV0IfNotLiteral() {
        assertThrows(UnsupportedVersionException.class,
            () -> buildResponse(10, Errors.NONE, Collections.singletonList(PREFIXED_ACL1)).serialize(V0));
    }

    @Test
    public void shouldThrowIfUnknown() {
        assertThrows(IllegalArgumentException.class,
            () -> buildResponse(10, Errors.NONE, Collections.singletonList(UNKNOWN_ACL)).serialize(V0));
    }

    @Test
    public void shouldRoundTripV0() {
        List<DescribeAclsResource> resources = Arrays.asList(LITERAL_ACL1, LITERAL_ACL2);
        final DescribeAclsResponse original = buildResponse(10, Errors.NONE, resources);
        final ByteBuffer buffer = original.serialize(V0);

        final DescribeAclsResponse result = DescribeAclsResponse.parse(buffer, V0);
        assertResponseEquals(original, result);

        final DescribeAclsResponse result2 = buildResponse(10, Errors.NONE, DescribeAclsResponse.aclsResources(
            DescribeAclsResponse.aclBindings(resources)));
        assertResponseEquals(original, result2);
    }

    @Test
    public void shouldRoundTripV1() {
        List<DescribeAclsResource> resources = Arrays.asList(LITERAL_ACL1, PREFIXED_ACL1);
        final DescribeAclsResponse original = buildResponse(100, Errors.NONE, resources);
        final ByteBuffer buffer = original.serialize(V1);

        final DescribeAclsResponse result = DescribeAclsResponse.parse(buffer, V1);
        assertResponseEquals(original, result);

        final DescribeAclsResponse result2 = buildResponse(100, Errors.NONE, DescribeAclsResponse.aclsResources(
            DescribeAclsResponse.aclBindings(resources)));
        assertResponseEquals(original, result2);
    }

    @Test
    public void testAclBindings() {
        final AclBinding original = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL),
                new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

        final List<AclBinding> result = DescribeAclsResponse.aclBindings(Collections.singletonList(LITERAL_ACL1));
        assertEquals(1, result.size());
        assertEquals(original, result.get(0));
    }

    private static void assertResponseEquals(final DescribeAclsResponse original, final DescribeAclsResponse actual) {
        final Set<DescribeAclsResource> originalBindings = new HashSet<>(original.acls());
        final Set<DescribeAclsResource> actualBindings = new HashSet<>(actual.acls());

        assertEquals(originalBindings, actualBindings);
    }

    private static DescribeAclsResponse buildResponse(int throttleTimeMs, Errors error, List<DescribeAclsResource> resources) {
        return new DescribeAclsResponse(new DescribeAclsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setErrorMessage(error.message())
            .setResources(resources));
    }

    private static DescribeAclsResource buildResource(String name, ResourceType type, PatternType patternType, List<AclDescription> acls) {
        return new DescribeAclsResource()
            .setResourceName(name)
            .setResourceType(type.code())
            .setPatternType(patternType.code())
            .setAcls(acls);
    }

    private static AclDescription buildAclDescription(String host, String principal, AclOperation operation, AclPermissionType permission) {
        return new AclDescription()
            .setHost(host)
            .setPrincipal(principal)
            .setOperation(operation.code())
            .setPermissionType(permission.code());
    }
}
