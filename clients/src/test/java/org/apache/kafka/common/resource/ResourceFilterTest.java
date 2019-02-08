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

import static org.apache.kafka.common.resource.ResourceType.ANY;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceFilterTest {
    @Test
    public void shouldNotMatchIfDifferentResourceType() {
        assertFalse(new ResourceFilter(TOPIC, "Name")
            .matches(new Resource(GROUP, "Name")));
    }

    @Test
    public void shouldNotMatchIfDifferentName() {
        assertFalse(new ResourceFilter(TOPIC, "Different")
            .matches(new Resource(TOPIC, "Name")));
    }

    @Test
    public void shouldNotMatchIfDifferentNameCase() {
        assertFalse(new ResourceFilter(TOPIC, "NAME")
            .matches(new Resource(TOPIC, "Name")));
    }

    @Test
    public void shouldMatchWhereResourceTypeIsAny() {
        assertTrue(new ResourceFilter(ANY, "Name")
            .matches(new Resource(TOPIC, "Name")));
    }

    @Test
    public void shouldMatchWhereResourceNameIsAny() {
        assertTrue(new ResourceFilter(TOPIC, null)
            .matches(new Resource(TOPIC, "Name")));
    }

    @Test
    public void shouldMatchIfExactMatch() {
        assertTrue(new ResourceFilter(TOPIC, "Name")
            .matches(new Resource(TOPIC, "Name")));
    }

    @Test
    public void shouldMatchWildcardIfExactMatch() {
        assertTrue(new ResourceFilter(TOPIC, "*")
            .matches(new Resource(TOPIC, "*")));
    }

    @Test
    public void shouldNotMatchWildcardAgainstOtherName() {
        assertFalse(new ResourceFilter(TOPIC, "Name")
            .matches(new Resource(TOPIC, "*")));
    }

    @Test
    public void shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(new ResourceFilter(TOPIC, "*")
            .matches(new Resource(TOPIC, "Name")));
    }
}