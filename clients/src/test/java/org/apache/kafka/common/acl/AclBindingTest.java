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
package org.apache.kafka.common.acl;

import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AclBindingTest {
    private static final AclBinding ACL1 = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "mytopic", ResourceNameType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "", AclOperation.ALL, AclPermissionType.ALLOW));

    private static final AclBinding ACL2 = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "mytopic", ResourceNameType.LITERAL),
        new AccessControlEntry("User:*", "", AclOperation.READ, AclPermissionType.ALLOW));

    private static final AclBinding ACL3 = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "mytopic2", ResourceNameType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBinding UNKNOWN_ACL = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "mytopic2", ResourceNameType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.UNKNOWN, AclPermissionType.DENY));

    private static final AclBindingFilter ANY_ANONYMOUS = new AclBindingFilter(
        ResourcePatternFilter.ANY,
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));

    private static final AclBindingFilter ANY_DENY = new AclBindingFilter(
        ResourcePatternFilter.ANY,
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.DENY));

    private static final AclBindingFilter ANY_MYTOPIC = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", ResourceNameType.LITERAL),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    @Test
    public void testMatching() {
        assertEquals(ACL1, ACL1);
        final AclBinding acl1Copy = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "mytopic", ResourceNameType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "", AclOperation.ALL, AclPermissionType.ALLOW));
        assertEquals(ACL1, acl1Copy);
        assertEquals(acl1Copy, ACL1);
        assertEquals(ACL2, ACL2);
        assertNotEquals(ACL1, ACL2);
        assertNotEquals(ACL2, ACL1);
        assertTrue(AclBindingFilter.ANY.matches(ACL1));
        assertNotEquals(AclBindingFilter.ANY, ACL1);
        assertTrue(AclBindingFilter.ANY.matches(ACL2));
        assertNotEquals(AclBindingFilter.ANY, ACL2);
        assertTrue(AclBindingFilter.ANY.matches(ACL3));
        assertNotEquals(AclBindingFilter.ANY, ACL3);
        assertEquals(AclBindingFilter.ANY, AclBindingFilter.ANY);
        assertTrue(ANY_ANONYMOUS.matches(ACL1));
        assertNotEquals(ANY_ANONYMOUS, ACL1);
        assertFalse(ANY_ANONYMOUS.matches(ACL2));
        assertNotEquals(ANY_ANONYMOUS, ACL2);
        assertTrue(ANY_ANONYMOUS.matches(ACL3));
        assertNotEquals(ANY_ANONYMOUS, ACL3);
        assertFalse(ANY_DENY.matches(ACL1));
        assertFalse(ANY_DENY.matches(ACL2));
        assertTrue(ANY_DENY.matches(ACL3));
        assertTrue(ANY_MYTOPIC.matches(ACL1));
        assertTrue(ANY_MYTOPIC.matches(ACL2));
        assertFalse(ANY_MYTOPIC.matches(ACL3));
        assertTrue(ANY_ANONYMOUS.matches(UNKNOWN_ACL));
        assertTrue(ANY_DENY.matches(UNKNOWN_ACL));
        assertEquals(UNKNOWN_ACL, UNKNOWN_ACL);
        assertFalse(ANY_MYTOPIC.matches(UNKNOWN_ACL));
    }

    @Test
    public void testUnknowns() {
        assertFalse(ACL1.isUnknown());
        assertFalse(ACL2.isUnknown());
        assertFalse(ACL3.isUnknown());
        assertFalse(ANY_ANONYMOUS.isUnknown());
        assertFalse(ANY_DENY.isUnknown());
        assertFalse(ANY_MYTOPIC.isUnknown());
        assertTrue(UNKNOWN_ACL.isUnknown());
    }

    @Test
    public void testMatchesAtMostOne() {
        assertNull(ACL1.toFilter().findIndefiniteField());
        assertNull(ACL2.toFilter().findIndefiniteField());
        assertNull(ACL3.toFilter().findIndefiniteField());
        assertFalse(ANY_ANONYMOUS.matchesAtMostOne());
        assertFalse(ANY_DENY.matchesAtMostOne());
        assertFalse(ANY_MYTOPIC.matchesAtMostOne());
    }

    @Test
    public void shouldNotThrowOnUnknownResourceNameType() {
        new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", ResourceNameType.UNKNOWN), ACL1.entry());
    }

    @Test
    public void shouldNotThrowOnUnknownResourceType() {
        new AclBinding(new ResourcePattern(ResourceType.UNKNOWN, "foo", ResourceNameType.LITERAL), ACL1.entry());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnAnyResourceNameType() {
        new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", ResourceNameType.ANY), ACL1.entry());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnAnyResourceType() {
        new AclBinding(new ResourcePattern(ResourceType.ANY, "foo", ResourceNameType.LITERAL), ACL1.entry());
    }
}
