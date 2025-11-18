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

import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class TrieSet implements Set<String> {
    private final PatriciaTrie<String> trie;

    public TrieSet() {
        trie = new PatriciaTrie<>();
    }

    public Set<String> prefixSet(String key) {
        return trie.prefixMap(key).keySet();
    }

    @Override
    public int size() {
        return trie.size();
    }

    @Override
    public boolean isEmpty() {
        return trie.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return trie.containsKey(o);
    }

    @Override
    public Iterator<String> iterator() {
        return trie.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return trie.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return trie.keySet().toArray(a);
    }

    @Override
    public boolean add(String s) {
        Objects.requireNonNull(s, "Expect non-null element to add");
        return trie.putIfAbsent(s, s) == null;
    }

    @Override
    public boolean remove(Object o) {
        return trie.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (final Object k : c) {
            if (!trie.containsKey(k))
                return false;
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        boolean mutated = false;
        for (final String k : c)
            mutated |= add(k);
        return mutated;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean mutated = false;
        for (final Object k : c) {
            if (!contains(k))
                mutated |= remove(k);
        }
        return mutated;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean mutated = false;
        for (final Object k : c)
            mutated |= remove(k);
        return mutated;
    }

    @Override
    public void clear() {
        trie.clear();
    }
}
