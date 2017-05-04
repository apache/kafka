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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.Change;

public class MockCacheFlushListener<K, V> implements CacheFlushListener<K, V> {

    private List<KeyValue<K, Change<V>>> applied = new ArrayList<>();

    @Override
    public void apply(K key, V newValue, V oldValue) {
        applied.add(KeyValue.pair(key, new Change<>(newValue, oldValue)));
    }

    public void checkAndClearProcessResult(List<KeyValue<K, Change<V>>> expected) {
        assertEquals("the number of outputs:" + applied, expected.size(), applied.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("output[" + i + "]:", expected.get(i), applied.get(i));
        }
        applied.clear();
    }
}
