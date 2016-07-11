/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class MockKeyValueMapper {

    private static class NoOpKeyValueMapper<K, V> implements KeyValueMapper<K, V, KeyValue<K, V>> {
        @Override
        public KeyValue<K, V> apply(K key, V value) {
            return KeyValue.pair(key, value);
        }
    }

    private static class SelectValueKeyValueMapper<K, V> implements KeyValueMapper<K, V, KeyValue<V, V>> {
        @Override
        public KeyValue<V, V> apply(K key, V value) {
            return KeyValue.pair(value, value);
        }
    }

    private static class SelectValueMapper<K, V> implements KeyValueMapper<K, V, V> {
        @Override
        public V apply(K key, V value) {
            return value;
        }
    }

    public static <K, V> KeyValueMapper<K, V, KeyValue<K, V>> NoOpKeyValueMapper() {
        return new NoOpKeyValueMapper<>();
    }

    public static <K, V> KeyValueMapper<K, V, KeyValue<V, V>> SelectValueKeyValueMapper() {
        return new SelectValueKeyValueMapper<>();
    }

    public static <K, V> KeyValueMapper<K, V, V> SelectValueMapper() {
        return new SelectValueMapper<>();
    }
}