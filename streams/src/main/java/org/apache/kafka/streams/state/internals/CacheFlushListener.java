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
package org.apache.kafka.streams.state.internals;

/**
 * Listen to cache flush events
 * @param <K> key type
 * @param <V> value type
 */
public interface CacheFlushListener<K, V> {

    /**
     * Called when records are flushed from the {@link ThreadCache}
     * @param key         key of the entry
     * @param newValue    current value
     * @param oldValue    previous value
     * @param timestamp   timestamp of new value
     */
    void apply(final K key, final V newValue, final V oldValue, final long timestamp);
}
