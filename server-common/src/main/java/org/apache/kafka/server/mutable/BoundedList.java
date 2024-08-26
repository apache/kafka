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

package org.apache.kafka.server.mutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A list which cannot grow beyond a certain length. If the maximum length would be exceeded by an
 * operation, the operation throws a BoundedListTooLongException exception rather than completing.
 * For simplicity, mutation through iterators or sublists is not allowed.
 *
 * @param <E> the element type
 */
public class BoundedList<E> implements List<E> {
    private final int maxLength;
    private final List<E> underlying;

    public static <E> BoundedList<E> newArrayBacked(int maxLength) {
        return new BoundedList<>(maxLength, new ArrayList<>());
    }

    public static <E> BoundedList<E> newArrayBacked(int maxLength, int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Invalid non-positive initialCapacity of " + initialCapacity);
        }
        return new BoundedList<>(maxLength, new ArrayList<>(initialCapacity));
    }

    private BoundedList(int maxLength, List<E> underlying) {
        if (maxLength <= 0) {
            throw new IllegalArgumentException("Invalid non-positive maxLength of " + maxLength);
        }

        if (underlying.size() > maxLength) {
            throw new BoundedListTooLongException("Cannot wrap list, because it is longer than " +
                "the maximum length " + maxLength);
        }
        this.maxLength = maxLength;
        this.underlying = underlying;
    }

    @Override
    public int size() {
        return underlying.size();
    }

    @Override
    public boolean isEmpty() {
        return underlying.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return underlying.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return Collections.unmodifiableList(underlying).iterator();
    }

    @Override
    public Object[] toArray() {
        return underlying.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return underlying.toArray(a);
    }

    @Override
    public boolean add(E e) {
        if (underlying.size() >= maxLength) {
            throw new BoundedListTooLongException("Cannot add another element to the list " +
                "because it would exceed the maximum length of " + maxLength);
        }
        return underlying.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return underlying.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return underlying.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        int numToAdd = c.size();
        if (underlying.size() > maxLength - numToAdd) {
            throw new BoundedListTooLongException("Cannot add another " + numToAdd +
                " element(s) to the list because it would exceed the maximum length of " +
                maxLength);
        }
        return underlying.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        int numToAdd = c.size();
        if (underlying.size() > maxLength - numToAdd) {
            throw new BoundedListTooLongException("Cannot add another " + numToAdd +
                " element(s) to the list because it would exceed the maximum length of " +
                maxLength);
        }
        return underlying.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return underlying.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return underlying.retainAll(c);
    }

    @Override
    public void clear() {
        underlying.clear();
    }

    @Override
    public E get(int index) {
        return underlying.get(index);
    }

    @Override
    public E set(int index, E element) {
        return underlying.set(index, element);
    }

    @Override
    public void add(int index, E element) {
        if (underlying.size() >= maxLength) {
            throw new BoundedListTooLongException("Cannot add another element to the list " +
                    "because it would exceed the maximum length of " + maxLength);
        }
        underlying.add(index, element);
    }

    @Override
    public E remove(int index) {
        return underlying.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return underlying.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return underlying.lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator() {
        return Collections.unmodifiableList(underlying).listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return Collections.unmodifiableList(underlying).listIterator(index);
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return Collections.unmodifiableList(underlying).subList(fromIndex, toIndex);
    }

    @Override
    public boolean equals(Object o) {
        return underlying.equals(o);
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }
}
