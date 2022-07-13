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

package org.apache.kafka.server.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;


/**
 * A map which presents a lightweight view of another "underlying" map. Values in the
 * underlying map will be translated by a callback before they are returned.
 *
 * This class is not internally synchronized. (Typically the underlyingMap is treated as
 * immutable.)
 */
public final class TranslatedValueMapView<K, V, B> extends AbstractMap<K, V> {
    class TranslatedValueSetView extends AbstractSet<Entry<K, V>> {
        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new TranslatedValueEntryIterator(underlyingMap.entrySet().iterator());
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry)) return false;
            Entry other = (Entry) o;
            if (!underlyingMap.containsKey(other.getKey())) return false;
            B value = underlyingMap.get(other.getKey());
            V translatedValue = valueMapping.apply(value);
            return Objects.equals(translatedValue, other.getValue());
        }

        @Override
        public boolean isEmpty() {
            return underlyingMap.isEmpty();
        }

        @Override
        public int size() {
            return underlyingMap.size();
        }
    }

    class TranslatedValueEntryIterator implements Iterator<Entry<K, V>> {
        private final Iterator<Entry<K, B>> underlyingIterator;

        TranslatedValueEntryIterator(Iterator<Entry<K, B>> underlyingIterator) {
            this.underlyingIterator = underlyingIterator;
        }

        @Override
        public boolean hasNext() {
            return underlyingIterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            Entry<K, B> underlyingEntry = underlyingIterator.next();
            return new AbstractMap.SimpleImmutableEntry<>(underlyingEntry.getKey(),
                valueMapping.apply(underlyingEntry.getValue()));
        }
    }

    private final Map<K, B> underlyingMap;
    private final Function<B, V> valueMapping;
    private final TranslatedValueSetView set;

    public TranslatedValueMapView(Map<K, B> underlyingMap,
                                  Function<B, V> valueMapping) {
        this.underlyingMap = underlyingMap;
        this.valueMapping = valueMapping;
        this.set = new TranslatedValueSetView();
    }

    @Override
    public boolean containsKey(Object key) {
        return underlyingMap.containsKey(key);
    }

    @Override
    public V get(Object key) {
        if (!underlyingMap.containsKey(key)) return null;
        B value = underlyingMap.get(key);
        return valueMapping.apply(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return set;
    }

    @Override
    public boolean isEmpty() {
        return underlyingMap.isEmpty();
    }
}
