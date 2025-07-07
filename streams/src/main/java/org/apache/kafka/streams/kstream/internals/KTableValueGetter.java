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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public interface KTableValueGetter<K, V> {

    void init(ProcessorContext<?, ?> context);

    ValueAndTimestamp<V> get(K key);

    /**
     * Returns the latest record version, associated with the provided key, with timestamp
     * not exceeding the provided timestamp bound. This method may only be called if
     * {@link #isVersioned()} is true.
     */
    default ValueAndTimestamp<V> get(K key, long asOfTimestamp) {
        throw new UnsupportedOperationException("get(key, timestamp) is only supported for versioned stores");
    }

    /**
     * @return whether this value getter supports multiple record versions for the same key.
     *         If true, then {@link #get(Object, long)} must be implemented. If not, then
     *         {@link #get(Object, long)} must not be called.
     */
    boolean isVersioned();

    default void close() {}
}
