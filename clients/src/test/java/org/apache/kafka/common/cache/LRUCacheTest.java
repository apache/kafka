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
package org.apache.kafka.common.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LRUCacheTest {

    @Test
    public void testPutGet() {
        Cache<String, String> cache = new LRUCache<>(4);

        cache.put("a", "b");
        cache.put("c", "d");
        cache.put("e", "f");
        cache.put("g", "h");

        assertEquals(4, cache.size());

        assertEquals("b", cache.get("a"));
        assertEquals("d", cache.get("c"));
        assertEquals("f", cache.get("e"));
        assertEquals("h", cache.get("g"));
    }

    @Test
    public void testRemove() {
        Cache<String, String> cache = new LRUCache<>(4);

        cache.put("a", "b");
        cache.put("c", "d");
        cache.put("e", "f");
        assertEquals(3, cache.size());

        assertEquals(true, cache.remove("a"));
        assertEquals(2, cache.size());
        assertNull(cache.get("a"));
        assertEquals("d", cache.get("c"));
        assertEquals("f", cache.get("e"));

        assertEquals(false, cache.remove("key-does-not-exist"));

        assertEquals(true, cache.remove("c"));
        assertEquals(1, cache.size());
        assertNull(cache.get("c"));
        assertEquals("f", cache.get("e"));

        assertEquals(true, cache.remove("e"));
        assertEquals(0, cache.size());
        assertNull(cache.get("e"));
    }

    @Test
    public void testEviction() {
        Cache<String, String> cache = new LRUCache<>(2);

        cache.put("a", "b");
        cache.put("c", "d");
        assertEquals(2, cache.size());

        cache.put("e", "f");
        assertEquals(2, cache.size());
        assertNull(cache.get("a"));
        assertEquals("d", cache.get("c"));
        assertEquals("f", cache.get("e"));

        // Validate correct access order eviction
        cache.get("c");
        cache.put("g", "h");
        assertEquals(2, cache.size());
        assertNull(cache.get("e"));
        assertEquals("d", cache.get("c"));
        assertEquals("h", cache.get("g"));
    }
}
