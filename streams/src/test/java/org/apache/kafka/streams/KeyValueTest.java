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
package org.apache.kafka.streams;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyValueTest {

    @Test
    public void shouldHaveSameEqualsAndHashCode() {
        final KeyValue<String, Long> kv = KeyValue.pair("key1", 1L);
        final KeyValue<String, Long> copyOfKV = KeyValue.pair(kv.key, kv.value);

        // Reflexive
        assertTrue(kv.equals(kv));
        assertTrue(kv.hashCode() == kv.hashCode());

        // Symmetric
        assertTrue(kv.equals(copyOfKV));
        assertTrue(kv.hashCode() == copyOfKV.hashCode());
        assertTrue(copyOfKV.hashCode() == kv.hashCode());

        // Transitive
        final KeyValue<String, Long> copyOfCopyOfKV = KeyValue.pair(copyOfKV.key, copyOfKV.value);
        assertTrue(copyOfKV.equals(copyOfCopyOfKV));
        assertTrue(copyOfKV.hashCode() == copyOfCopyOfKV.hashCode());
        assertTrue(kv.equals(copyOfCopyOfKV));
        assertTrue(kv.hashCode() == copyOfCopyOfKV.hashCode());

        // Inequality scenarios
        assertFalse("must be false for null", kv.equals(null));
        assertFalse("must be false if key is non-null and other key is null", kv.equals(KeyValue.pair(null, kv.value)));
        assertFalse("must be false if value is non-null and other value is null", kv.equals(KeyValue.pair(kv.key, null)));
        final KeyValue<Long, Long> differentKeyType = KeyValue.pair(1L, kv.value);
        assertFalse("must be false for different key types", kv.equals(differentKeyType));
        final KeyValue<String, String> differentValueType = KeyValue.pair(kv.key, "anyString");
        assertFalse("must be false for different value types", kv.equals(differentValueType));
        final KeyValue<Long, String> differentKeyValueTypes = KeyValue.pair(1L, "anyString");
        assertFalse("must be false for different key and value types", kv.equals(differentKeyValueTypes));
        assertFalse("must be false for different types of objects", kv.equals(new Object()));

        final KeyValue<String, Long> differentKey = KeyValue.pair(kv.key + "suffix", kv.value);
        assertFalse("must be false if key is different", kv.equals(differentKey));
        assertFalse("must be false if key is different", differentKey.equals(kv));

        final KeyValue<String, Long> differentValue = KeyValue.pair(kv.key, kv.value + 1L);
        assertFalse("must be false if value is different", kv.equals(differentValue));
        assertFalse("must be false if value is different", differentValue.equals(kv));

        final KeyValue<String, Long> differentKeyAndValue = KeyValue.pair(kv.key + "suffix", kv.value + 1L);
        assertFalse("must be false if key and value are different", kv.equals(differentKeyAndValue));
        assertFalse("must be false if key and value are different", differentKeyAndValue.equals(kv));
    }

}