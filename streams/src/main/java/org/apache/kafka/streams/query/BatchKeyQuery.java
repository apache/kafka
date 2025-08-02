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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A query that retrieves multiple keys in a single operation, reducing the overhead
 * of individual key lookups and improving performance for bulk operations.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
@Evolving
public final class BatchKeyQuery<K, V> implements Query<Map<K, V>> {

    private final List<K> keys;

    /**
     * Creates a batch key query for the specified keys.
     *
     * @param keys the keys to retrieve
     */
    @SafeVarargs
    public BatchKeyQuery(final K... keys) {
        this.keys = Arrays.asList(keys);
    }

    /**
     * Creates a batch key query for the specified list of keys.
     *
     * @param keys the keys to retrieve
     */
    public BatchKeyQuery(final List<K> keys) {
        this.keys = List.copyOf(keys);
    }

    /**
     * Returns the keys to be retrieved.
     *
     * @return the list of keys
     */
    public List<K> getKeys() {
        return keys;
    }

    /**
     * The maximum number of keys that should be processed in a single batch.
     * This helps prevent memory issues with very large batch sizes.
     *
     * @return the maximum batch size
     */
    public int getMaxBatchSize() {
        return 1000; // Default batch size limit
    }

    @Override
    public String toString() {
        return "BatchKeyQuery{keys=" + keys.size() + " keys}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchKeyQuery<?, ?> that = (BatchKeyQuery<?, ?>) o;
        return keys.equals(that.keys);
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }
}