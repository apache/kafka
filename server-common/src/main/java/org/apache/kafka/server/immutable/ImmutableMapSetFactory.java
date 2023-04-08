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

package org.apache.kafka.server.immutable;

import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableMapSetFactory;

/**
 * A factory for instantiating persistent Hash-based Maps/Sets
 */
public interface ImmutableMapSetFactory {
    ImmutableMapSetFactory PCOLLECTIONS_FACTORY = new PCollectionsImmutableMapSetFactory();

    /**
     * @return a wrapped hash-based persistent map that is empty
     * @param <K> the key type
     * @param <V> the value type
     */
    <K, V> ImmutableMap<K, V> emptyMap();

    /**
     * @param key the key
     * @param value the value
     * @return a wrapped hash-based persistent map that has a single mapping
     * @param <K> the key type
     * @param <V> the value type
     */
    <K, V> ImmutableMap<K, V> singletonMap(K key, V value);

    /**
     * @return a wrapped hash-based persistent set that is empty
     * @param <E> the element type
     */
    <E> ImmutableSet<E> emptySet();

    /**
     * @param e the element
     * @return a wrapped hash-based persistent set that has a single element
     * @param <E> the element type
     */
    <E> ImmutableSet<E> singletonSet(E e);
}
