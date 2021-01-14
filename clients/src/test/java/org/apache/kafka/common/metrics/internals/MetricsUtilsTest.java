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
package org.apache.kafka.common.metrics.internals;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetricsUtilsTest {

    @Test
    public void testCreatingTags() {
        Map<String, String> tags = MetricsUtils.getTags("k1", "v1", "k2", "v2");
        assertEquals("v1", tags.get("k1"));
        assertEquals("v2", tags.get("k2"));
        assertEquals(2, tags.size());
    }

    @Test
    public void testCreatingTagsWithOddNumberOfTags() {
        assertThrows(IllegalArgumentException.class, () -> MetricsUtils.getTags("k1", "v1", "k2", "v2", "extra"));
    }
}
