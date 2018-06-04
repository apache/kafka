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

import static org.apache.kafka.common.resource.ResourceUtils.WILDCARD_MARKER;
import static org.apache.kafka.common.resource.ResourceUtils.matchResource;
import static org.apache.kafka.common.resource.ResourceUtils.matchWildcardSuffixedString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceUtilsTest {

    @Test
    public void testmatchWildcardSuffixedString() {
        // everything should match wildcard string
        assertTrue(matchWildcardSuffixedString(WILDCARD_MARKER, WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString(WILDCARD_MARKER, "f"));
        assertTrue(matchWildcardSuffixedString(WILDCARD_MARKER, "foo"));
        assertTrue(matchWildcardSuffixedString(WILDCARD_MARKER, "fo" + WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString(WILDCARD_MARKER, "f" + WILDCARD_MARKER));

        assertTrue(matchWildcardSuffixedString("f", WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("f", "f"));
        assertTrue(matchWildcardSuffixedString("f", "f" + WILDCARD_MARKER));
        assertFalse(matchWildcardSuffixedString("f", "foo"));
        assertFalse(matchWildcardSuffixedString("f", "fo" + WILDCARD_MARKER));

        assertTrue(matchWildcardSuffixedString("foo", WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("foo", "foo"));
        assertTrue(matchWildcardSuffixedString("foo", "fo" + WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("foo", "f" + WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("foo", "foo" + WILDCARD_MARKER));
        assertFalse(matchWildcardSuffixedString("foo", "f"));
        assertFalse(matchWildcardSuffixedString("foo", "foot" + WILDCARD_MARKER));

        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "fo"));
        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "foo"));
        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "foot"));
        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "fo" + WILDCARD_MARKER));

        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "foo" + WILDCARD_MARKER));
        assertTrue(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "foot" + WILDCARD_MARKER));
        assertFalse(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "f"));
        assertFalse(matchWildcardSuffixedString("fo" + WILDCARD_MARKER, "f" + WILDCARD_MARKER));
    }

    @Test
    public void testMatchResource() {
        // same resource should match
        assertTrue(
                matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );

        // different resource shouldn't match
        assertFalse(
                matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICB")
                )
        );
        assertFalse(
                matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA"),
                        new Resource(ResourceType.GROUP, "ResourceType.TOPICA")
                )
        );

        // wildcard resource should match
        assertTrue(
                matchResource(
                        new Resource(ResourceType.TOPIC, WILDCARD_MARKER),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );

        // wildcard-suffix resource should match
        assertTrue(
                matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPIC", ResourceNameType.WILDCARD_SUFFIXED),
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPICA")
                )
        );
        assertFalse(
                matchResource(
                        new Resource(ResourceType.TOPIC, "ResourceType.TOPIC", ResourceNameType.WILDCARD_SUFFIXED),
                        new Resource(ResourceType.TOPIC, "topiA")
                )
        );
    }
}
