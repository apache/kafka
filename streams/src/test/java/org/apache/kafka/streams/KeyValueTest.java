/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyValueTest {

    private KeyValue<String, Long> kv1a = new KeyValue<>("key1", 1L);
    private KeyValue<String, Long> kv1b = new KeyValue<>("key1", 1L);
    private KeyValue<String, Long> kv1c = new KeyValue<>("key1", 2L);
    private KeyValue<String, Long> kv2 = new KeyValue<>("key2", 2L);
    private KeyValue<String, Long> kv3 = new KeyValue<>("key3", 3L);

    @Test
    public void testEquals() {
        assertTrue(kv1a.equals(kv1a));
        assertTrue(kv1a.equals(kv1b));
        assertTrue(kv1b.equals(kv1a));
        assertFalse(kv1a.equals(kv1c));
        assertFalse(kv1c.equals(kv2));
        assertFalse(kv1a.equals(kv2));
        assertFalse(kv1a.equals(kv3));
        assertFalse(kv2.equals(kv3));
        assertFalse(kv1a.equals(null));
    }



    @Test
    public void testHashcode() {
        assertTrue(kv1a.hashCode() == kv1b.hashCode());
        assertFalse(kv1a.hashCode() == kv2.hashCode());
        assertFalse(kv1a.hashCode() == kv3.hashCode());
        assertFalse(kv2.hashCode() == kv3.hashCode());
    }
}
