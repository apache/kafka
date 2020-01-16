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
import org.junit.Test;

public class ResourcePatternTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfResourceTypeIsAny() {
        new ResourcePattern(ResourceType.ANY, "name", PatternType.LITERAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfPatternTypeIsMatch() {
        new ResourcePattern(ResourceType.TOPIC, "name", PatternType.MATCH);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfPatternTypeIsAny() {
        new ResourcePattern(ResourceType.TOPIC, "name", PatternType.ANY);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfResourceNameIsNull() {
        new ResourcePattern(ResourceType.TOPIC, null, PatternType.ANY);
    }
}