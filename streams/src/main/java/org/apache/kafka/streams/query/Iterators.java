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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

public final class Iterators {

    private Iterators() {
    }

    public static <E, I extends Iterator<E>> CloseableIterator<E> collate(final Collection<I> iterators) {
        return new CloseableIterator<E>() {
            private final Deque<I> iteratorQueue = new LinkedList<>(iterators);

            @Override
            public void close() {
                RuntimeException exception = null;
                for (final I iterator : iterators) {
                    if (iterator instanceof Closeable) {
                        try {
                            ((Closeable) iterator).close();
                        } catch (final IOException e) {
                            if (exception == null) {
                                exception = new RuntimeException(
                                    "Exception closing collated iterator", e);
                            } else {
                                exception.addSuppressed(e);
                            }
                        }
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }

            @Override
            public boolean hasNext() {
                for (int i = 0; i < iterators.size(); i++) {
                    final Iterator<E> iterator = Objects.requireNonNull(iteratorQueue.peek());
                    if (iterator.hasNext()) {
                        return true;
                    } else {
                        iteratorQueue.push(iteratorQueue.poll());
                    }
                }
                return false;
            }

            @Override
            public E next() {
                final I iterator = iteratorQueue.poll();
                final E next = Objects.requireNonNull(iterator).next();
                iteratorQueue.push(iterator);
                return next;
            }
        };
    }
}
