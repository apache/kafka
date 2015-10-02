/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import java.util.Iterator;

public abstract class FilteredIterator<T, S> implements Iterator<T> {

    private Iterator<S> inner;
    private T nextValue = null;

    public FilteredIterator(Iterator<S> inner) {
        this.inner = inner;

        findNext();
    }

    @Override
    public boolean hasNext() {
        return nextValue != null;
    }

    @Override
    public T next() {
        T value = nextValue;
        findNext();

        return value;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void findNext() {
        while (inner.hasNext()) {
            S item = inner.next();
            nextValue = filter(item);
            if (nextValue != null) {
                return;
            }
        }
        nextValue = null;
    }

    protected abstract T filter(S item);
}
