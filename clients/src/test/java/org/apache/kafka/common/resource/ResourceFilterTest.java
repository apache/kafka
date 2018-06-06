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

package org.apache.kafka.common.resource;

import org.junit.Test;

import static org.apache.kafka.common.resource.ResourceNameType.LITERAL;
import static org.apache.kafka.common.resource.ResourceNameType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.ANY;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.UNKNOWN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceFilterTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsAny() {
        new ResourceFilter(ANY, null, ResourceNameType.ANY)
            .matches(new Resource(ANY, "Name", PREFIXED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsUnknown() {
        new ResourceFilter(ANY, null, ResourceNameType.ANY)
            .matches(new Resource(UNKNOWN, "Name", LITERAL));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceNameTypeIsAny() {
        new ResourceFilter(ANY, null, ResourceNameType.ANY)
            .matches(new Resource(GROUP, "Name", ResourceNameType.ANY));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfAclResourceNameTypeIsUnknown() {
        new ResourceFilter(ANY, null, ResourceNameType.ANY)
            .matches(new Resource(GROUP, "Name", ResourceNameType.UNKNOWN));
    }

    @Test
    public void shouldNotMatchIfDifferentResourceType() {
        assertFalse(new ResourceFilter(TOPIC, "Name", LITERAL)
            .matches(new Resource(GROUP, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentName() {
        assertFalse(new ResourceFilter(TOPIC, "Different", PREFIXED)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfDifferentNameCase() {
        assertFalse(new ResourceFilter(TOPIC, "NAME", LITERAL)
            .matches(new Resource(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentNameType() {
        assertFalse(new ResourceFilter(TOPIC, "Name", LITERAL)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceTypeIsAny() {
        assertTrue(new ResourceFilter(ANY, "Name", PREFIXED)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceNameIsAny() {
        assertTrue(new ResourceFilter(TOPIC, null, PREFIXED)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceNameTypeIsAny() {
        assertTrue(new ResourceFilter(TOPIC, null, ResourceNameType.ANY)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchLiteralIfExactMatch() {
        assertTrue(new ResourceFilter(TOPIC, "Name", LITERAL)
            .matches(new Resource(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralIfNameMatchesAndFilterIsOnAnyNameType() {
        assertTrue(new ResourceFilter(TOPIC, "Name", ResourceNameType.ANY)
            .matches(new Resource(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralIfNamePrefixed() {
        assertFalse(new ResourceFilter(TOPIC, "Name-something", ResourceNameType.ANY)
            .matches(new Resource(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfExactMatch() {
        assertTrue(new ResourceFilter(TOPIC, "*", LITERAL)
            .matches(new Resource(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardAgainstOtherName() {
        assertFalse(new ResourceFilter(TOPIC, "Name", LITERAL)
            .matches(new Resource(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(new ResourceFilter(TOPIC, "*", LITERAL)
            .matches(new Resource(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfFilterHasNameTypeOfAny() {
        assertTrue(new ResourceFilter(TOPIC, "Name", ResourceNameType.ANY)
            .matches(new Resource(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldMatchPrefixedIfExactMatch() {
        assertTrue(new ResourceFilter(TOPIC, "Name", PREFIXED)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource() {
        assertFalse(new ResourceFilter(TOPIC, "Name", PREFIXED)
            .matches(new Resource(TOPIC, "Name-something", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter() {
        assertFalse(new ResourceFilter(TOPIC, "Name-something", PREFIXED)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() {
        assertTrue(new ResourceFilter(TOPIC, "Name-something", ResourceNameType.ANY)
            .matches(new Resource(TOPIC, "Name", PREFIXED)));
    }
}