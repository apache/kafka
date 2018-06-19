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

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DescribeAclsRequestTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclBindingFilter LITERAL_FILTER = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foo", PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBindingFilter PREFIXED_FILTER = new AclBindingFilter(new ResourcePatternFilter(ResourceType.GROUP, "prefix", PatternType.PREFIXED),
        new AccessControlEntryFilter("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBindingFilter ANY_FILTER = new AclBindingFilter(new ResourcePatternFilter(ResourceType.GROUP, "bar", PatternType.ANY),
        new AccessControlEntryFilter("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(new ResourcePatternFilter(ResourceType.UNKNOWN, "foo", PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    @Test(expected = UnsupportedVersionException.class)
    public void shouldThrowOnV0IfPrefixed() {
        new DescribeAclsRequest(PREFIXED_FILTER, V0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfUnknown() {
        new DescribeAclsRequest(UNKNOWN_FILTER, V0);
    }

    @Test
    public void shouldRoundTripLiteralV0() {
        final DescribeAclsRequest original = new DescribeAclsRequest(LITERAL_FILTER, V0);
        final Struct struct = original.toStruct();

        final DescribeAclsRequest result = new DescribeAclsRequest(struct, V0);

        assertRequestEquals(original, result);
    }

    @Test
    public void shouldRoundTripAnyV0AsLiteral() {
        final DescribeAclsRequest original = new DescribeAclsRequest(ANY_FILTER, V0);
        final DescribeAclsRequest expected = new DescribeAclsRequest(
            new AclBindingFilter(new ResourcePatternFilter(
                ANY_FILTER.patternFilter().resourceType(),
                ANY_FILTER.patternFilter().name(),
                PatternType.LITERAL),
                ANY_FILTER.entryFilter()), V0);

        final Struct struct = original.toStruct();
        final DescribeAclsRequest result = new DescribeAclsRequest(struct, V0);

        assertRequestEquals(expected, result);
    }

    @Test
    public void shouldRoundTripLiteralV1() {
        final DescribeAclsRequest original = new DescribeAclsRequest(LITERAL_FILTER, V1);
        final Struct struct = original.toStruct();

        final DescribeAclsRequest result = new DescribeAclsRequest(struct, V1);

        assertRequestEquals(original, result);
    }

    @Test
    public void shouldRoundTripPrefixedV1() {
        final DescribeAclsRequest original = new DescribeAclsRequest(PREFIXED_FILTER, V1);
        final Struct struct = original.toStruct();

        final DescribeAclsRequest result = new DescribeAclsRequest(struct, V1);

        assertRequestEquals(original, result);
    }

    @Test
    public void shouldRoundTripAnyV1() {
        final DescribeAclsRequest original = new DescribeAclsRequest(ANY_FILTER, V1);
        final Struct struct = original.toStruct();

        final DescribeAclsRequest result = new DescribeAclsRequest(struct, V1);

        assertRequestEquals(original, result);
    }

    private static void assertRequestEquals(final DescribeAclsRequest original, final DescribeAclsRequest actual) {
        final AclBindingFilter originalFilter = original.filter();
        final AclBindingFilter acttualFilter = actual.filter();
        assertEquals(originalFilter, acttualFilter);
    }
}