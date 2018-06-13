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
import org.junit.Test;

import static org.apache.kafka.common.resource.ResourceNameType.LITERAL;
import static org.apache.kafka.common.resource.ResourceNameType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.ANY;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.UNKNOWN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourcePatternFilterTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsAny() {
        new ResourcePatternFilter(ANY, null, ResourceNameType.ANY)
            .matches(new ResourcePattern(ANY, "Name", PREFIXED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceNameTypeIsAny() {
        new ResourcePatternFilter(ANY, null, ResourceNameType.ANY)
            .matches(new ResourcePattern(GROUP, "Name", ResourceNameType.ANY));
    }

    @Test
    public void shouldBeUnknownIfResourceTypeUnknown() {
        assertTrue(new ResourcePatternFilter(UNKNOWN, null, ResourceNameType.LITERAL).isUnknown());
    }

    @Test
    public void shouldBeUnknownIfResourceNameTypeUnknown() {
        assertTrue(new ResourcePatternFilter(GROUP, null, ResourceNameType.UNKNOWN).isUnknown());
    }

    @Test
    public void shouldNotMatchIfDifferentResourceType() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(GROUP, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentName() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Different", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfDifferentNameCase() {
        assertFalse(new ResourcePatternFilter(TOPIC, "NAME", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentNameType() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceTypeIsAny() {
        assertTrue(new ResourcePatternFilter(ANY, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceNameIsAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, null, PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceNameTypeIsAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, null, ResourceNameType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchLiteralIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralIfNameMatchesAndFilterIsOnAnyNameType() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", ResourceNameType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralIfNamePrefixed() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name-something", ResourceNameType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "*", LITERAL)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardAgainstOtherName() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(new ResourcePatternFilter(TOPIC, "*", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfFilterHasNameTypeOfAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", ResourceNameType.ANY)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldMatchPrefixedIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name-something", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name-something", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name-something", ResourceNameType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }
}