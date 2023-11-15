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

import org.apache.kafka.server.immutable.ImmutableNavigableSet;
import org.pcollections.TreePSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

@SuppressWarnings("deprecation")
public class PCollectionsImmutableNavigableSet<E> implements ImmutableNavigableSet<E> {
    private final TreePSet<E> underlying;

    /**
     * @return a wrapped tree-based persistent navigable set that is empty
     * @param <E> the element type
     */
    public static <E extends Comparable<? super E>> PCollectionsImmutableNavigableSet<E> empty() {
        return new PCollectionsImmutableNavigableSet<>(TreePSet.<E>empty());
    }

    /**
     * @param e the element
     * @return a wrapped tree-based persistent set that is empty
     * @param <E> the element type
     */
    public static <E extends Comparable<? super E>> PCollectionsImmutableNavigableSet<E> singleton(E e) {
        return new PCollectionsImmutableNavigableSet<>(TreePSet.singleton(e));
    }

    public PCollectionsImmutableNavigableSet(TreePSet<E> underlying) {
        this.underlying = underlying;
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> added(E e) {
        return new PCollectionsImmutableNavigableSet<>(underlying().plus(e));
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> removed(E e) {
        return new PCollectionsImmutableNavigableSet<>(underlying().minus(e));
    }

    @Override
    public E lower(E e) {
        return underlying().lower(e);
    }

    @Override
    public E floor(E e) {
        return underlying().floor(e);
    }

    @Override
    public E ceiling(E e) {
        return underlying().ceiling(e);
    }

    @Override
    public E higher(E e) {
        return underlying().higher(e);
    }

    @Override
    public E pollFirst() {
        // will throw UnsupportedOperationException
        return underlying().pollFirst();
    }

    @Override
    public E pollLast() {
        // will throw UnsupportedOperationException
        return underlying().pollLast();
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> descendingSet() {
        return new PCollectionsImmutableNavigableSet<>(underlying().descendingSet());
    }

    @Override
    public Iterator<E> descendingIterator() {
        return underlying().descendingIterator();
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        return new PCollectionsImmutableNavigableSet<>(underlying().subSet(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> headSet(E toElement, boolean inclusive) {
        return new PCollectionsImmutableNavigableSet<>(underlying().headSet(toElement, inclusive));
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return new PCollectionsImmutableNavigableSet<>(underlying().tailSet(fromElement, inclusive));
    }

    @Override
    public Comparator<? super E> comparator() {
        return underlying().comparator();
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> subSet(E fromElement, E toElement) {
        return new PCollectionsImmutableNavigableSet<>(underlying().subSet(fromElement, toElement));
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> headSet(E toElement) {
        return new PCollectionsImmutableNavigableSet<>(underlying().headSet(toElement));
    }

    @Override
    public PCollectionsImmutableNavigableSet<E> tailSet(E fromElement) {
        return new PCollectionsImmutableNavigableSet<>(underlying().tailSet(fromElement));
    }

    @Override
    public E first() {
        return underlying().first();
    }

    @Override
    public E last() {
        return underlying().last();
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
        // will throw UnsupportedOperationException
        return underlying().add(e);
    }

    @Override
    public boolean remove(Object o) {
        // will throw UnsupportedOperationException
        return underlying().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return underlying().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        // will throw UnsupportedOperationException
        return underlying().addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // will throw UnsupportedOperationException
        return underlying().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        // will throw UnsupportedOperationException
        return underlying().removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        // will throw UnsupportedOperationException
        return underlying().removeIf(filter);
    }

    @Override
    public void clear() {
        // will throw UnsupportedOperationException
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
        PCollectionsImmutableNavigableSet<?> that = (PCollectionsImmutableNavigableSet<?>) o;
        return Objects.equals(underlying(), that.underlying());
    }

    @Override
    public int hashCode() {
        return underlying().hashCode();
    }

    @Override
    public String toString() {
        return "PCollectionsImmutableNavigableSet{" +
                "underlying=" + underlying() +
                '}';
    }

    // package-private for testing
    TreePSet<E> underlying() {
        return underlying;
    }
}
