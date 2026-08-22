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
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class StandardAclTest {
    public static final List<StandardAcl> TEST_ACLS = StandardAclFixtures.TEST_ACLS;

    private static int signum(int input) {
        return Integer.compare(input, 0);
    }

    @Test
    public void testCompareTo() {
        assertEquals(1, signum(TEST_ACLS.get(0).compareTo(TEST_ACLS.get(1))));
        assertEquals(-1, signum(TEST_ACLS.get(1).compareTo(TEST_ACLS.get(0))));
        assertEquals(-1, signum(TEST_ACLS.get(2).compareTo(TEST_ACLS.get(3))));
        assertEquals(1, signum(TEST_ACLS.get(4).compareTo(TEST_ACLS.get(3))));
        assertEquals(-1, signum(TEST_ACLS.get(3).compareTo(TEST_ACLS.get(4))));
    }

    @Test
    public void testToBindingRoundTrips() {
        for (StandardAcl acl : TEST_ACLS) {
            AclBinding binding = acl.toBinding();
            StandardAcl acl2 = StandardAcl.fromAclBinding(binding);
            assertEquals(acl2, acl);
        }
    }

    @Test
    public void testEquals() {
        for (int i = 0; i != TEST_ACLS.size(); i++) {
            for (int j = 0; j != TEST_ACLS.size(); j++) {
                if (i == j) {
                    assertEquals(TEST_ACLS.get(i), TEST_ACLS.get(j));
                } else {
                    assertNotEquals(TEST_ACLS.get(i), TEST_ACLS.get(j));
                }
            }
        }
    }

    @Test
    public void testKafkaPrincipalIsCached() {
        StandardAcl acl = new StandardAcl(
            ResourceType.TOPIC, "foo", PatternType.LITERAL,
            "User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);
        assertSame(acl.kafkaPrincipal(), acl.kafkaPrincipal());
    }

    @Test
    public void testKafkaPrincipalParsing() {
        StandardAcl acl = new StandardAcl(
            ResourceType.TOPIC, "foo", PatternType.LITERAL,
            "User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);
        assertEquals("User", acl.kafkaPrincipal().getPrincipalType());
        assertEquals("alice", acl.kafkaPrincipal().getName());
    }

    @Test
    public void testKafkaPrincipalWildcard() {
        StandardAcl acl = new StandardAcl(
            ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL,
            "User:*", "*", AclOperation.ALTER, AclPermissionType.ALLOW);
        assertEquals("User", acl.kafkaPrincipal().getPrincipalType());
        assertEquals("*", acl.kafkaPrincipal().getName());
    }

    /**
     * The static kafkaPrincipal() cache is shared by every StandardAcl in the JVM and is never
     * explicitly cleared, so it must stay bounded even when a long-running cluster churns through
     * many distinct principal strings over time. Verify that parsing stays correct once the cache
     * is full and that its size never exceeds the documented bound.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testKafkaPrincipalCacheIsBounded() throws Exception {
        Field cacheField = StandardAcl.class.getDeclaredField("PRINCIPAL_CACHE");
        cacheField.setAccessible(true);
        Map<String, KafkaPrincipal> cache = (Map<String, KafkaPrincipal>) cacheField.get(null);

        Field maxField = StandardAcl.class.getDeclaredField("MAX_CACHED_PRINCIPALS");
        maxField.setAccessible(true);
        int maxCachedPrincipals = maxField.getInt(null);

        Map<String, KafkaPrincipal> previousEntries = new HashMap<>(cache);
        cache.clear();
        try {
            int principalsBeforeConcurrentAdmission = maxCachedPrincipals - 32;
            for (int i = 0; i < principalsBeforeConcurrentAdmission; i++) {
                assertPrincipalParsed(i);
            }

            IntStream.range(principalsBeforeConcurrentAdmission, maxCachedPrincipals + 500)
                .parallel()
                .forEach(StandardAclTest::assertPrincipalParsed);

            assertTrue(cache.size() <= maxCachedPrincipals,
                "Principal cache grew past its bound: size=" + cache.size() + " max=" + maxCachedPrincipals);
        } finally {
            cache.clear();
            cache.putAll(previousEntries);
        }
    }

    private static void assertPrincipalParsed(int id) {
        StandardAcl acl = new StandardAcl(
            ResourceType.TOPIC, "foo", PatternType.LITERAL,
            "User:cache-bound-test-" + id, "*", AclOperation.READ, AclPermissionType.ALLOW);
        KafkaPrincipal principal = acl.kafkaPrincipal();
        assertEquals("User", principal.getPrincipalType());
        assertEquals("cache-bound-test-" + id, principal.getName());
    }
}
