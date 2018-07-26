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
package org.apache.kafka.test;

import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.HashMap;
import java.util.Map;

public class KTableValueGetterStub<K, V> implements KTableValueGetter<K, V> {

    private final Map<K, V> data = new HashMap<>();

    @Override
    public void init(final ProcessorContext context) {

    }

    @Override
    public V get(final K key) {
        return data.get(key);
    }

    public void put(final K key, V value) {
        data.put(key, value);
    }

    public void remove(final K key) {
        data.remove(key);
    }

    @Override
    public void close() {
    }
}
