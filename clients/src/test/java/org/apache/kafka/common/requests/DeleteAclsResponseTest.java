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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import java.nio.ByteBuffer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DeleteAclsResponseTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final DeleteAclsMatchingAcl LITERAL_ACL1 = new DeleteAclsMatchingAcl()
        .setResourceType(ResourceType.TOPIC.code())
        .setResourceName("foo")
        .setPatternType(PatternType.LITERAL.code())
        .setPrincipal("User:ANONYMOUS")
        .setHost("127.0.0.1")
        .setOperation(AclOperation.READ.code())
        .setPermissionType(AclPermissionType.DENY.code());

    private static final DeleteAclsMatchingAcl LITERAL_ACL2 = new DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.GROUP.code())
            .setResourceName("group")
            .setPatternType(PatternType.LITERAL.code())
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.WRITE.code())
            .setPermissionType(AclPermissionType.ALLOW.code());

    private static final DeleteAclsMatchingAcl PREFIXED_ACL1 = new DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.GROUP.code())
            .setResourceName("prefix")
            .setPatternType(PatternType.PREFIXED.code())
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.CREATE.code())
            .setPermissionType(AclPermissionType.ALLOW.code());

    private static final DeleteAclsMatchingAcl UNKNOWN_ACL = new DeleteAclsMatchingAcl()
            .setResourceType(ResourceType.UNKNOWN.code())
            .setResourceName("group")
            .setPatternType(PatternType.LITERAL.code())
            .setPrincipal("User:*")
            .setHost("127.0.0.1")
            .setOperation(AclOperation.WRITE.code())
            .setPermissionType(AclPermissionType.ALLOW.code());

    private static final DeleteAclsFilterResult LITERAL_RESPONSE = new DeleteAclsFilterResult().setMatchingAcls(asList(
        LITERAL_ACL1, LITERAL_ACL2));

    private static final DeleteAclsFilterResult PREFIXED_RESPONSE = new DeleteAclsFilterResult().setMatchingAcls(asList(
        LITERAL_ACL1, PREFIXED_ACL1));

    private static final DeleteAclsFilterResult UNKNOWN_RESPONSE = new DeleteAclsFilterResult().setMatchingAcls(asList(
            UNKNOWN_ACL));

    @Test
    public void shouldThrowOnV0IfNotLiteral() {
        assertThrows(UnsupportedVersionException.class, () -> new DeleteAclsResponse(
            new DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(singletonList(PREFIXED_RESPONSE)),
            V0));
    }

    @Test
    public void shouldThrowOnIfUnknown() {
        assertThrows(IllegalArgumentException.class, () -> new DeleteAclsResponse(
            new DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(singletonList(UNKNOWN_RESPONSE)),
            V1));
    }

    @Test
    public void shouldRoundTripV0() {
        final DeleteAclsResponse original = new DeleteAclsResponse(
            new DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(singletonList(LITERAL_RESPONSE)),
            V0);
        final ByteBuffer buffer = original.serialize(V0);

        final DeleteAclsResponse result = DeleteAclsResponse.parse(buffer, V0);
        assertEquals(original.filterResults(), result.filterResults());
    }

    @Test
    public void shouldRoundTripV1() {
        final DeleteAclsResponse original = new DeleteAclsResponse(
            new DeleteAclsResponseData()
                .setThrottleTimeMs(10)
                .setFilterResults(asList(LITERAL_RESPONSE, PREFIXED_RESPONSE)),
            V1);
        final ByteBuffer buffer = original.serialize(V1);

        final DeleteAclsResponse result = DeleteAclsResponse.parse(buffer, V1);
        assertEquals(original.filterResults(), result.filterResults());
    }

}
