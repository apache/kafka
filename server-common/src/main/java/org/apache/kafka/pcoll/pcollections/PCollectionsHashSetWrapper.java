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

package org.apache.kafka.pcoll.pcollections;

import org.apache.kafka.pcoll.PHashSetWrapper;
import org.pcollections.MapPSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class PCollectionsHashSetWrapper<E> implements PHashSetWrapper<E> {
    private final MapPSet<E> underlying;


    public PCollectionsHashSetWrapper(MapPSet<E> set) {
        this.underlying = Objects.requireNonNull(set);
    }

    @Override
    public MapPSet<E> underlying() {
        return this.underlying;
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
    public Set<E> asJava() {
        return underlying();
    }

    @Override
    public PHashSetWrapper<E> afterAdding(E e) {
        return new PCollectionsHashSetWrapper<>(underlying().plus(e));
    }

    @Override
    public PHashSetWrapper<E> afterRemoving(E e) {
        return new PCollectionsHashSetWrapper<>(underlying().minus(e));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PCollectionsHashSetWrapper<?> that = (PCollectionsHashSetWrapper<?>) o;
        return Objects.equals(underlying(), that.underlying());
    }

    @Override
    public int hashCode() {
        return underlying().hashCode();
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
    public String toString() {
        return "PCollectionsHashSetWrapper{" +
            "underlying=" + underlying() +
            '}';
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
    public boolean containsAll(Collection<?> c) {
        return underlying().containsAll(c);
    }
}
