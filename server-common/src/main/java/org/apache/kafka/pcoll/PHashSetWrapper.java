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

package org.apache.kafka.pcoll;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A persistent Hash-based Set wrapper
 *
 * @param <E> the element type
 */
public interface PHashSetWrapper<E> extends Iterable<E> {
    /**
     * @return the underlying persistent set
     */
    Object underlying();

    int size();

    /**
     * @return true iff the set is empty
     */
    boolean isEmpty();

    /**
     * @return the persistent set as a standard Java {@link Set}
     */
    Set<E> asJava();

    /**
     * @param e the element
     * @return a wrapped persistent set that differs from this one in that the given element is added (if necessary)
     */
    PHashSetWrapper<E> afterAdding(E e);

    /**
     * @param e the element
     * @return a wrapped persistent set that differs from this one in that the given element is added (if necessary)
     */
    PHashSetWrapper<E> afterRemoving(E e);

    /**
     * @return a sequential Stream with this collection as its source.
     */
    Stream<E> stream();

    /**
     * @return a possibly parallel Stream with this collection as its source.
     */
    Stream<E> parallelStream();

    /**
     * @param c the collection
     * @return true iff this collection contains all of the elements in the specified collection.
     */
    boolean containsAll(Collection<?> c);
}
