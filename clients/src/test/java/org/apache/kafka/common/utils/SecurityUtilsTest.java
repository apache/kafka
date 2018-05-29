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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SecurityUtilsTest {

    @Test
    public void testPrincipalNameCanContainSeparator() {
        String name = "name:with:separator:in:it";
        KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + name);
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
        assertEquals(name, principal.getName());
    }

    @Test
    public void testParseKafkaPrincipalWithNonUserPrincipalType() {
        String name = "foo";
        String principalType = "ResourceType.GROUP";
        KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(principalType + ":" + name);
        assertEquals(principalType, principal.getPrincipalType());
        assertEquals(name, principal.getName());
    }

    @Test
    public void testmatchWildcardSuffixedString() {
        // everything should match wildcard string
        assertTrue(SecurityUtils.matchWildcardSuffixedString(SecurityUtils.WILDCARD_MARKER, SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString(SecurityUtils.WILDCARD_MARKER, "f"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString(SecurityUtils.WILDCARD_MARKER, "foo"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString(SecurityUtils.WILDCARD_MARKER, "fo" + SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString(SecurityUtils.WILDCARD_MARKER, "f" + SecurityUtils.WILDCARD_MARKER));

        assertTrue(SecurityUtils.matchWildcardSuffixedString("f", SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("f", "f"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("f", "f" + SecurityUtils.WILDCARD_MARKER));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("f", "foo"));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("f", "fo" + SecurityUtils.WILDCARD_MARKER));

        assertTrue(SecurityUtils.matchWildcardSuffixedString("foo", SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("foo", "foo"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("foo", "fo" + SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("foo", "f" + SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("foo", "foo" + SecurityUtils.WILDCARD_MARKER));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("foo", "f"));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("foo", "foot" + SecurityUtils.WILDCARD_MARKER));

        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "fo"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "foo"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "foot"));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "fo" + SecurityUtils.WILDCARD_MARKER));

        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "foo" + SecurityUtils.WILDCARD_MARKER));
        assertTrue(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "foot" + SecurityUtils.WILDCARD_MARKER));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "f"));
        assertFalse(SecurityUtils.matchWildcardSuffixedString("fo" + SecurityUtils.WILDCARD_MARKER, "f" + SecurityUtils.WILDCARD_MARKER));
    }

    @Test
    public void testMatchResource() {
        // same resource should match
        assertTrue(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );

        // different resource shouldn't match
        assertFalse(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICB")
                )
        );
        assertFalse(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.GROUP, "ResourceType.TOPICA")
                )
        );

        // wildcard resource should match
        assertTrue(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, SecurityUtils.WILDCARD_MARKER),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );

        // wildcard-suffix resource should match
        assertTrue(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPIC" + SecurityUtils.WILDCARD_MARKER),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );
        assertFalse(
                SecurityUtils.matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPIC" + SecurityUtils.WILDCARD_MARKER),
                        new Resource(ResourceType.TOPIC, "topiA")
                )
        );
    }

}
