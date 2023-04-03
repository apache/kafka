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

package org.apache.kafka.pcoll.pcollections;

import org.apache.kafka.pcoll.PHashMapWrapper;
import org.apache.kafka.pcoll.PHashMapSetWrapperFactory;
import org.apache.kafka.pcoll.PHashSetWrapper;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;

public class PCollectionsHashMapSetWrapperFactory implements PHashMapSetWrapperFactory {
    @Override
    public <K, V> PHashMapWrapper<K, V> emptyMap() {
        return new PCollectionsHashMapWrapper<K, V>(HashTreePMap.empty());
    }

    @Override
    public <K, V> PHashMapWrapper<K, V> singletonMap(K key, V value) {
        return new PCollectionsHashMapWrapper<K, V>(HashTreePMap.singleton(key, value));
    }

    @Override
    public <E> PHashSetWrapper<E> emptySet() {
        return new PCollectionsHashSetWrapper<E>(HashTreePSet.empty()); // test
    }

    @Override
    public <E> PHashSetWrapper<E> singletonSet(E e) {
        return new PCollectionsHashSetWrapper<E>(HashTreePSet.singleton(e));
    }
}
