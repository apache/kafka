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
 * An iterator that cycles through the Iterator of a Collection indefinitely.
 * Useful for tasks such as round-robin load balancing.
 */
public class CircularIterator<T> implements Iterator<T> {

    private final Iterable<T> iterable;
    private Iterator<T> iterator;
    private T peek;

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
        if (col.isEmpty()) {
            throw new IllegalArgumentException("CircularIterator can only be used on non-empty lists");
        }
    }

    @Override
    public boolean hasNext() {
        if (this.peek != null) {
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
        if (this.peek != null) {
            nextValue = this.peek;
            this.peek = null;
        } else {
            nextValue = this.iterator.next();
        }
        return nextValue;
    }

    public T peek() {
        if (this.peek == null) {
            this.peek = next();
        }
        return this.peek;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
