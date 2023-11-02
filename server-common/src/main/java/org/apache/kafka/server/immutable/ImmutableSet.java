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

import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableSet;

import java.util.Set;

/**
 * A persistent Hash-based Set wrapper
 * java.util.Set methods that mutate in-place will throw UnsupportedOperationException
 *
 * @param <E> the element type
 */
public interface ImmutableSet<E> extends Set<E> {

    /**
     * @return a wrapped hash-based persistent set that is empty
     * @param <E> the element type
     */
    static <E> ImmutableSet<E> empty() {
        return PCollectionsImmutableSet.empty();
    }

    /**
     * @param e the element
     * @return a wrapped hash-based persistent set that has a single element
     * @param <E> the element type
     */
    static <E> ImmutableSet<E> singleton(E e) {
        return PCollectionsImmutableSet.singleton(e);
    }

    /**
     * @param e the element
     * @return a wrapped persistent set that differs from this one in that the given element is added (if necessary)
     */
    ImmutableSet<E> added(E e);

    /**
     * @param e the element
     * @return a wrapped persistent set that differs from this one in that the given element is added (if necessary)
     */
    ImmutableSet<E> removed(E e);
}
