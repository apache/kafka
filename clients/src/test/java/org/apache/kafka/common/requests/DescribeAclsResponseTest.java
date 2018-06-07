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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DescribeAclsResponseTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclBinding LITERAL_ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", ResourceNameType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBinding LITERAL_ACL2 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "group", ResourceNameType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW));

    private static final AclBinding PREFIXED_ACL1 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "prefix", ResourceNameType.PREFIXED),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBinding UNKNOWN_ACL = new AclBinding(new ResourcePattern(ResourceType.UNKNOWN, "foo", ResourceNameType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    @Test(expected = UnsupportedVersionException.class)
    public void shouldThrowOnV0IfNotLiteral() {
        new DescribeAclsResponse(10, ApiError.NONE, aclBindings(PREFIXED_ACL1)).toStruct(V0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfUnknown() {
        new DescribeAclsResponse(10, ApiError.NONE, aclBindings(UNKNOWN_ACL)).toStruct(V0);
    }

    @Test
    public void shouldRoundTripV0() {
        final DescribeAclsResponse original = new DescribeAclsResponse(10, ApiError.NONE, aclBindings(LITERAL_ACL1, LITERAL_ACL2));
        final Struct struct = original.toStruct(V0);

        final DescribeAclsResponse result = new DescribeAclsResponse(struct);

        assertResponseEquals(original, result);
    }

    @Test
    public void shouldRoundTripV1() {
        final DescribeAclsResponse original = new DescribeAclsResponse(100, ApiError.NONE, aclBindings(LITERAL_ACL1, PREFIXED_ACL1));
        final Struct struct = original.toStruct(V1);

        final DescribeAclsResponse result = new DescribeAclsResponse(struct);

        assertResponseEquals(original, result);
    }

    private static void assertResponseEquals(final DescribeAclsResponse original, final DescribeAclsResponse actual) {
        final Set<AclBinding> originalBindings = new HashSet<>(original.acls());
        final Set<AclBinding> actualBindings = new HashSet<>(actual.acls());

        assertEquals(originalBindings, actualBindings);
    }

    private static List<AclBinding> aclBindings(final AclBinding... bindings) {
        return Arrays.asList(bindings);
    }
}