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

import org.apache.kafka.server.immutable.ImmutableSet;
import org.pcollections.HashTreePSet;
import org.pcollections.MapPSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

@SuppressWarnings("deprecation")
public class PCollectionsImmutableSet<E> implements ImmutableSet<E> {
    private final MapPSet<E> underlying;

    /**
     * @return a wrapped hash-based persistent set that is empty
     * @param <E> the element type
     */
    public static <E> PCollectionsImmutableSet<E> empty() {
        return new PCollectionsImmutableSet<>(HashTreePSet.empty());
    }

    /**
     * @param e the element
     * @return a wrapped hash-based persistent set that has a single element
     * @param <E> the element type
     */
    public static <E> PCollectionsImmutableSet<E> singleton(E e) {
        return new PCollectionsImmutableSet<>(HashTreePSet.singleton(e));
    }

    public PCollectionsImmutableSet(MapPSet<E> set) {
        this.underlying = Objects.requireNonNull(set);
    }

    @Override
    public ImmutableSet<E> added(E e) {
        return new PCollectionsImmutableSet<>(underlying().plus(e));
    }
    
    @Override
    public ImmutableSet<E> removed(E e) {
        return new PCollectionsImmutableSet<>(underlying().minus(e));
    }

    @Override
    public int size() {
        return underlying().size();
    }

    @Override
    public boolean isEmpty() {
        return underlying().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return underlying().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return underlying().iterator();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        underlying().forEach(action);
    }

    @Override
    public Object[] toArray() {
        return underlying().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return underlying().toArray(a);
    }

    @Override
    public boolean add(E e) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().add(e);
    }

    @Override
    public boolean remove(Object o) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return underlying().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        // will throw UnsupportedOperationException; delegate anyway for testability
        return underlying().removeIf(filter);
    }

    @Override
    public void clear() {
        // will throw UnsupportedOperationException; delegate anyway for testability
        underlying().clear();
    }

    @Override
    public Spliterator<E> spliterator() {
        return underlying().spliterator();
    }

    @Override
    public Stream<E> stream() {
        return underlying().stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return underlying().parallelStream();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PCollectionsImmutableSet<?> that = (PCollectionsImmutableSet<?>) o;
        return Objects.equals(underlying(), that.underlying());
    }

    @Override
    public int hashCode() {
        return underlying().hashCode();
    }

    @Override
    public String toString() {
        return "PCollectionsImmutableSet{" +
            "underlying=" + underlying() +
            '}';
    }

    // package-private for testing
    MapPSet<E> underlying() {
        return this.underlying;
    }
}
