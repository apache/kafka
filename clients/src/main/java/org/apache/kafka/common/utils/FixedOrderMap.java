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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An ordered map (LinkedHashMap) implementation for which the order is immutable.
 * To accomplish this, all methods of removing mappings are disabled (they are marked
 * deprecated and throw an exception).
 *
 * This class is final to prevent subclasses from violating the desired property.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public final class FixedOrderMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = -6504110858733236170L;

    @Deprecated
    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return false;
    }

    @Deprecated
    @Override
    public V remove(final Object key) {
        throw new UnsupportedOperationException("Removing from registeredStores is not allowed");
    }

    @Deprecated
    @Override
    public boolean remove(final Object key, final Object value) {
        throw new UnsupportedOperationException("Removing from registeredStores is not allowed");
    }

    @Override
    public FixedOrderMap<K, V> clone() {
        throw new UnsupportedOperationException();
    }
}
