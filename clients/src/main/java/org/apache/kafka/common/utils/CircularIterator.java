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

package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * An iterator that cycles through the {@code Iterator} of a {@code Collection}
 * indefinitely. Useful for tasks such as round-robin load balancing. This class
 * does not provide thread-safe access. This {@code Iterator} supports
 * {@code null} elements in the underlying {@code Collection}.
 */
public class CircularIterator<T> implements Iterator<T> {

    private final Iterable<T> iterable;
    private Iterator<T> iterator;
    private T peek;
    private boolean hasPeek;

    /**
     * Create a new instance of a CircularIterator. The ordering of this
     * Iterator will be dictated by the Iterator returned by Collection itself.
     *
     * @param col The collection to iterate indefinitely
     *
     * @throws NullPointerException if col is {@code null}
     * @throws IllegalArgumentException if col is empty.
     */
    public CircularIterator(final Collection<T> col) {
        this.iterable = Objects.requireNonNull(col);
        this.iterator = col.iterator();
        this.peek = null;
        this.hasPeek = false;
        if (col.isEmpty()) {
            throw new IllegalArgumentException("CircularIterator can only be used on non-empty lists");
        }
    }

    @Override
    public boolean hasNext() {
        if (this.hasPeek) {
            return true;
        }
        if (!this.iterator.hasNext()) {
            this.iterator = this.iterable.iterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
        final T nextValue;
        if (this.hasPeek) {
            nextValue = this.peek;
            this.peek = null;
            this.hasPeek = false;
        } else {
            nextValue = this.iterator.next();
        }
        return nextValue;
    }

    /**
     * Peek at the next value in the Iterator. Calling this method multiple
     * times will return the same element without advancing this Iterator. The
     * value returned by this method will be the next item returned by
     * {@code next()}.
     *
     * @return The next value in this {@code Iterator}
     */
    public T peek() {
        if (!this.hasPeek) {
            this.peek = next();
            this.hasPeek = true;
        }
        return this.peek;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
