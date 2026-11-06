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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.immutable.ImmutableNavigableSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class AclCacheTest {

    private static StandardAcl newAcl(String principal) {
        return new StandardAcl(
            ResourceType.TOPIC, "foo", PatternType.LITERAL,
            principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    }

    private static StandardAcl newAcl(String principal, ResourceType resourceType, String resourceName) {
        return new StandardAcl(
            resourceType, resourceName, PatternType.LITERAL,
            principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    }

    @Test
    public void testEmptyCache() {
        AclCache cache = new AclCache();
        assertEquals(0, cache.count());
        assertTrue(cache.aclsForPrincipal("User:alice").isEmpty());
    }

    @Test
    public void testAclsForPrincipalReturnsEmptyForUnknownPrincipal() {
        AclCache cache = new AclCache();
        cache = cache.addAcl(Uuid.randomUuid(), newAcl("User:alice"));
        assertTrue(cache.aclsForPrincipal("User:bob").isEmpty());
    }

    @Test
    public void testAddUpdatesPrincipalIndex() {
        AclCache cache = new AclCache();
        Uuid id = Uuid.randomUuid();
        StandardAcl acl = newAcl("User:alice");
        cache = cache.addAcl(id, acl);

        ImmutableNavigableSet<StandardAcl> aliceAcls = cache.aclsForPrincipal("User:alice");
        assertEquals(1, aliceAcls.size());
        assertSame(acl, aliceAcls.first());
    }

    @Test
    public void testRemoveUpdatesPrincipalIndex() {
        AclCache cache = new AclCache();
        Uuid id = Uuid.randomUuid();
        StandardAcl acl = newAcl("User:alice");
        cache = cache.addAcl(id, acl);
        assertEquals(1, cache.aclsForPrincipal("User:alice").size());

        cache = cache.removeAcl(id);
        assertTrue(cache.aclsForPrincipal("User:alice").isEmpty());
        assertEquals(0, cache.count());
    }

    @Test
    public void testMultiplePrincipals() {
        AclCache cache = new AclCache();
        StandardAcl aliceAcl = newAcl("User:alice");
        StandardAcl bobAcl = newAcl("User:bob");
        cache = cache.addAcl(Uuid.randomUuid(), aliceAcl);
        cache = cache.addAcl(Uuid.randomUuid(), bobAcl);

        assertEquals(1, cache.aclsForPrincipal("User:alice").size());
        assertEquals(1, cache.aclsForPrincipal("User:bob").size());
        assertSame(aliceAcl, cache.aclsForPrincipal("User:alice").first());
        assertSame(bobAcl, cache.aclsForPrincipal("User:bob").first());
        assertTrue(cache.aclsForPrincipal("User:charlie").isEmpty());
    }

    @Test
    public void testCleanupOnLastRemoval() {
        AclCache cache = new AclCache();
        Uuid id = Uuid.randomUuid();
        cache = cache.addAcl(id, newAcl("User:alice"));
        cache = cache.removeAcl(id);

        assertTrue(cache.aclsForPrincipal("User:alice").isEmpty());
    }

    @Test
    public void testConsistencyAcrossIndices() {
        AclCache cache = new AclCache();
        Uuid id1 = Uuid.randomUuid();
        Uuid id2 = Uuid.randomUuid();
        StandardAcl aliceAcl = newAcl("User:alice", ResourceType.TOPIC, "foo");
        StandardAcl bobAcl = newAcl("User:bob", ResourceType.TOPIC, "bar");

        cache = cache.addAcl(id1, aliceAcl);
        cache = cache.addAcl(id2, bobAcl);
        assertEquals(2, cache.count());
        assertEquals(1, cache.aclsForPrincipal("User:alice").size());
        assertEquals(1, cache.aclsForPrincipal("User:bob").size());
        assertEquals(2, cache.aclsByResource().size());
        assertSame(aliceAcl, cache.getAcl(id1));
        assertSame(bobAcl, cache.getAcl(id2));

        cache = cache.removeAcl(id1);
        assertEquals(1, cache.count());
        assertEquals(0, cache.aclsForPrincipal("User:alice").size());
        assertEquals(1, cache.aclsForPrincipal("User:bob").size());
        assertEquals(1, cache.aclsByResource().size());
    }
}
