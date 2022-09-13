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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.ALL;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.LITERAL_ACLS;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.PREFIXED_ACLS;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTestConstants.WILDCARD_ACLS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class StandardAclTest {
    @Test
    public void testResource() {
        assertEquals(new Resource(ResourceType.CLUSTER, Resource.CLUSTER_NAME),
                LITERAL_ACLS.get(0).resource());
        assertEquals(new Resource(ResourceType.TOPIC, "foo_"),
                PREFIXED_ACLS.get(0).resource());
    }

    @Test
    public void testToBindingRoundTrips() {
        ALL.forEach(acl -> {
            AclBinding binding = acl.toBinding();
            StandardAcl acl2 = StandardAcl.fromAclBinding(binding);
            assertEquals(acl2, acl);
        });
    }

    @Test
    public void testEquals() {
        ALL.forEach(acl1 -> {
            ALL.forEach(acl2 -> {
                if (acl1 == acl2) {
                    assertEquals(acl1, acl2);
                } else {
                    assertNotEquals(acl1, acl2);
                }
            });
        });
    }

    @Test
    public void testIsWildcard() {
        WILDCARD_ACLS.forEach(acl -> assertTrue(acl.isWildcard()));
        LITERAL_ACLS.forEach(acl -> assertFalse(acl.isWildcard()));
        PREFIXED_ACLS.forEach(acl -> assertFalse(acl.isWildcard()));
    }

    @Test
    public void testIsWildcardOrPrefix() {
        WILDCARD_ACLS.forEach(acl -> assertTrue(acl.isWildcardOrPrefix()));
        PREFIXED_ACLS.forEach(acl -> assertTrue(acl.isWildcardOrPrefix()));
        LITERAL_ACLS.forEach(acl -> assertFalse(acl.isWildcardOrPrefix()));
    }

    @Test
    public void testResourceNameForPrefixNode() {
        WILDCARD_ACLS.forEach(acl -> assertEquals("", acl.resourceNameForPrefixNode()));
        LITERAL_ACLS.forEach(acl ->
                assertEquals(acl.resourceName(), acl.resourceNameForPrefixNode()));
        PREFIXED_ACLS.forEach(acl ->
                assertEquals(acl.resourceName(), acl.resourceNameForPrefixNode()));
    }
}
