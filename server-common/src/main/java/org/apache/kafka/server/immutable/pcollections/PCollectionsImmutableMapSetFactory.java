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

package org.apache.kafka.server.immutable.pcollections;

import org.apache.kafka.server.immutable.ImmutableMap;
import org.apache.kafka.server.immutable.ImmutableMapSetFactory;
import org.apache.kafka.server.immutable.ImmutableSet;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;

public class PCollectionsImmutableMapSetFactory implements ImmutableMapSetFactory {
    @Override
    public <K, V> ImmutableMap<K, V> emptyMap() {
        return new PCollectionsImmutableMap<>(HashTreePMap.empty());
    }

    @Override
    public <K, V> ImmutableMap<K, V> singletonMap(K key, V value) {
        return new PCollectionsImmutableMap<>(HashTreePMap.singleton(key, value));
    }

    @Override
    public <E> ImmutableSet<E> emptySet() {
        return new PCollectionsImmutableSet<>(HashTreePSet.empty());
    }

    @Override
    public <E> ImmutableSet<E> singletonSet(E e) {
        return new PCollectionsImmutableSet<>(HashTreePSet.singleton(e));
    }
}
