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
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceNameType;
import org.junit.Test;

import static org.apache.kafka.common.resource.ResourceNameType.LITERAL;
import static org.apache.kafka.common.resource.ResourceNameType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.ANY;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.UNKNOWN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AclUtilsTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsAny() {
        AclUtils.matchResource(
            new Resource(ANY, "Name", PREFIXED),
            new ResourceFilter(ANY, null, ResourceNameType.ANY)
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsUnknown() {
        AclUtils.matchResource(
            new Resource(UNKNOWN, "Name", LITERAL),
            new ResourceFilter(ANY, null, ResourceNameType.ANY)
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceNameTypeIsAny() {
        AclUtils.matchResource(
            new Resource(GROUP, "Name", ResourceNameType.ANY),
            new ResourceFilter(ANY, null, ResourceNameType.ANY)
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfAclResourceNameTypeIsUnknown() {
        AclUtils.matchResource(
            new Resource(GROUP, "Name", ResourceNameType.UNKNOWN),
            new ResourceFilter(ANY, null, ResourceNameType.ANY)
        );
    }

    @Test
    public void shouldNotMatchIfDifferentResourceType() {
        assertFalse(AclUtils.matchResource(
            new Resource(GROUP, "Name", LITERAL),
            new ResourceFilter(TOPIC, "Name", LITERAL)
        ));
    }

    @Test
    public void shouldNotMatchIfDifferentName() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, "Different", PREFIXED)
        ));
    }

    @Test
    public void shouldNotMatchIfDifferentNameCase() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", LITERAL),
            new ResourceFilter(TOPIC, "NAME", LITERAL)
        ));
    }

    @Test
    public void shouldNotMatchIfDifferentNameType() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, "Name", LITERAL)
        ));
    }

    @Test
    public void shouldMatchWhereResourceTypeIsAny() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(ANY, "Name", PREFIXED)
        ));
    }

    @Test
    public void shouldMatchWhereResourceNameIsAny() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, null, PREFIXED)
        ));
    }

    @Test
    public void shouldMatchWhereResourceNameTypeIsAny() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, null, ResourceNameType.ANY)
        ));
    }

    @Test
    public void shouldMatchLiteralIfExactMatch() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", LITERAL),
            new ResourceFilter(TOPIC, "Name", LITERAL)
        ));
    }

    @Test
    public void shouldMatchLiteralIfNameMatchesAndFilterIsOnAnyNameType() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", LITERAL),
            new ResourceFilter(TOPIC, "Name", ResourceNameType.ANY)
        ));
    }

    @Test
    public void shouldNotMatchLiteralIfNamePrefixed() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", LITERAL),
            new ResourceFilter(TOPIC, "Name-something", ResourceNameType.ANY)
        ));
    }

    @Test
    public void shouldMatchLiteralWildcardIfExactMatch() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "*", LITERAL),
            new ResourceFilter(TOPIC, "*", LITERAL)
        ));
    }

    @Test
    public void shouldNotMatchLiteralWildcardAgainstOtherName() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "*", LITERAL),
            new ResourceFilter(TOPIC, "Name", LITERAL)
        ));
    }

    @Test
    public void shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", LITERAL),
            new ResourceFilter(TOPIC, "*", LITERAL)
        ));
    }

    @Test
    public void shouldMatchLiteralWildcardIfFilterHasNameTypeOfAny() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "*", LITERAL),
            new ResourceFilter(TOPIC, "Name", ResourceNameType.ANY)
        ));
    }

    @Test
    public void shouldMatchPrefixedIfExactMatch() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, "Name", PREFIXED)
        ));
    }

    @Test
    public void shouldNotMatchPrefixedIfNamePrefixedAndFilterTypeIsPrefixed() {
        assertFalse(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, "Name-something", PREFIXED)
        ));
    }

    @Test
    public void shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() {
        assertTrue(AclUtils.matchResource(
            new Resource(TOPIC, "Name", PREFIXED),
            new ResourceFilter(TOPIC, "Name-something", ResourceNameType.ANY)
        ));
    }
}