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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclFilterResponse;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DeleteAclsResponseTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclBinding LITERAL_ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBinding LITERAL_ACL2 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "group", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW));

    private static final AclBinding PREFIXED_ACL1 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "prefix", PatternType.PREFIXED),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBinding UNKNOWN_ACL = new AclBinding(new ResourcePattern(ResourceType.UNKNOWN, "group", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW));

    private static final AclFilterResponse LITERAL_RESPONSE = new AclFilterResponse(aclDeletions(LITERAL_ACL1, LITERAL_ACL2));

    private static final AclFilterResponse PREFIXED_RESPONSE = new AclFilterResponse(aclDeletions(LITERAL_ACL1, PREFIXED_ACL1));

    private static final AclFilterResponse UNKNOWN_RESPONSE = new AclFilterResponse(aclDeletions(UNKNOWN_ACL));

    @Test(expected = UnsupportedVersionException.class)
    public void shouldThrowOnV0IfNotLiteral() {
        new DeleteAclsResponse(10, aclResponses(PREFIXED_RESPONSE)).toStruct(V0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIfUnknown() {
        new DeleteAclsResponse(10, aclResponses(UNKNOWN_RESPONSE)).toStruct(V1);
    }

    @Test
    public void shouldRoundTripV0() {
        final DeleteAclsResponse original = new DeleteAclsResponse(10, aclResponses(LITERAL_RESPONSE));
        final Struct struct = original.toStruct(V0);

        final DeleteAclsResponse result = new DeleteAclsResponse(struct);

        assertResponseEquals(original, result);
    }

    @Test
    public void shouldRoundTripV1() {
        final DeleteAclsResponse original = new DeleteAclsResponse(100, aclResponses(LITERAL_RESPONSE, PREFIXED_RESPONSE));
        final Struct struct = original.toStruct(V1);

        final DeleteAclsResponse result = new DeleteAclsResponse(struct);

        assertResponseEquals(original, result);
    }

    private static void assertResponseEquals(final DeleteAclsResponse original, final DeleteAclsResponse actual) {
        assertEquals("Number of responses wrong", original.responses().size(), actual.responses().size());

        for (int idx = 0; idx != original.responses().size(); ++idx) {
            final List<AclBinding> originalBindings = original.responses().get(idx).deletions().stream()
                .map(AclDeletionResult::acl)
                .collect(Collectors.toList());

            final List<AclBinding> actualBindings = actual.responses().get(idx).deletions().stream()
                .map(AclDeletionResult::acl)
                .collect(Collectors.toList());

            assertEquals(originalBindings, actualBindings);
        }
    }

    private static List<AclFilterResponse> aclResponses(final AclFilterResponse... responses) {
        return Arrays.asList(responses);
    }

    private static Collection<AclDeletionResult> aclDeletions(final AclBinding... acls) {
        return Arrays.stream(acls)
            .map(AclDeletionResult::new)
            .collect(Collectors.toList());
    }
}