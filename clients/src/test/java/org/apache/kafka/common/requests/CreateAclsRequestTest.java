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
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateAclsRequestTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclBinding LITERAL_ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBinding LITERAL_ACL2 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "group", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW));

    private static final AclBinding PREFIXED_ACL1 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "prefix", PatternType.PREFIXED),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBinding UNKNOWN_ACL1 = new AclBinding(new ResourcePattern(ResourceType.UNKNOWN, "unknown", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    @Test
    public void shouldThrowOnV0IfNotLiteral() {
        assertThrows(UnsupportedVersionException.class, () -> new CreateAclsRequest(data(PREFIXED_ACL1), V0));
    }

    @Test
    public void shouldThrowOnIfUnknown() {
        assertThrows(IllegalArgumentException.class, () -> new CreateAclsRequest(data(UNKNOWN_ACL1), V0));
    }

    @Test
    public void shouldRoundTripV0() {
        final CreateAclsRequest original = new CreateAclsRequest(data(LITERAL_ACL1, LITERAL_ACL2), V0);
        final ByteBuffer buffer = original.serialize();

        final CreateAclsRequest result = CreateAclsRequest.parse(buffer, V0);

        assertRequestEquals(original, result);
    }

    @Test
    public void shouldRoundTripV1() {
        final CreateAclsRequest original = new CreateAclsRequest(data(LITERAL_ACL1, PREFIXED_ACL1), V1);
        final ByteBuffer buffer = original.serialize();

        final CreateAclsRequest result = CreateAclsRequest.parse(buffer, V1);

        assertRequestEquals(original, result);
    }

    private static void assertRequestEquals(final CreateAclsRequest original, final CreateAclsRequest actual) {
        assertEquals(original.aclCreations().size(), actual.aclCreations().size(), "Number of Acls wrong");

        for (int idx = 0; idx != original.aclCreations().size(); ++idx) {
            final AclBinding originalBinding = CreateAclsRequest.aclBinding(original.aclCreations().get(idx));
            final AclBinding actualBinding = CreateAclsRequest.aclBinding(actual.aclCreations().get(idx));
            assertEquals(originalBinding, actualBinding);
        }
    }

    private static CreateAclsRequestData data(final AclBinding... acls) {
        List<CreateAclsRequestData.AclCreation> aclCreations = Arrays.stream(acls)
            .map(CreateAclsRequest::aclCreation)
            .collect(Collectors.toList());
        return new CreateAclsRequestData().setCreations(aclCreations);
    }
}
