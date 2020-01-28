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
package org.apache.kafka.rsm.hdfs;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LRUCacheTest {
    @Test
    public void testLRUCache() {
        LRUCache cache = new LRUCache(1000);
        for (int i=0;i<100;i++)
            cache.put("a", i * 10, String.format("a%09d", i).getBytes(StandardCharsets.UTF_8));

        assertEquals(String.format("a%09d", 0), new String(cache.get("a", 0), StandardCharsets.UTF_8));
        cache.put("b",  123, String.format("b%09d", 0).getBytes(StandardCharsets.UTF_8));
        assertNull(cache.get("a", 10));
        assertEquals(String.format("b%09d", 0), new String(cache.get("b", 123), StandardCharsets.UTF_8));
        cache.put("b",  456, String.format("b%09d", 1).getBytes(StandardCharsets.UTF_8));
        assertNull(cache.get("a", 20));
        assertEquals(String.format("b%09d", 1), new String(cache.get("b", 456), StandardCharsets.UTF_8));
        cache.put("a", 30, "test".getBytes());
        cache.put("b",  3333, String.format("b%09d", 2).getBytes(StandardCharsets.UTF_8));
        assertNull(cache.get("a", 40));
        assertEquals("test", new String(cache.get("a", 30)));
    }
}
