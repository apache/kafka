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

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ResourcePatternTest {

    @Test
    public void shouldThrowIfResourceTypeIsAny() {
        assertThrows(IllegalArgumentException.class,
            () -> new ResourcePattern(ResourceType.ANY, "name", PatternType.LITERAL));
    }

    @Test
    public void shouldThrowIfPatternTypeIsMatch() {
        assertThrows(IllegalArgumentException.class, () -> new ResourcePattern(ResourceType.TOPIC, "name", PatternType.MATCH));
    }

    @Test
    public void shouldThrowIfPatternTypeIsAny() {
        assertThrows(IllegalArgumentException.class, () -> new ResourcePattern(ResourceType.TOPIC, "name", PatternType.ANY));
    }

    @Test
    public void shouldThrowIfResourceNameIsNull() {
        assertThrows(NullPointerException.class, () -> new ResourcePattern(ResourceType.TOPIC, null, PatternType.ANY));
    }
}
