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


import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A caching key-value store with headers.
 *
 * <p>It inherits {@link CachingKeyValueStore}'s IQv2 {@code query(...)} handling: point queries
 * ({@code KeyQuery}) consult the record cache (honoring {@code skipCache}) and fall back to the
 * wrapped store on a miss, while other query types are forwarded to the wrapped store. The cached
 * value bytes are the serialized {@code ValueTimestampHeaders}; the metered layer performs the
 * header-aware deserialization, so no override is needed here.
 */
public class CachingKeyValueStoreWithHeaders extends CachingKeyValueStore {

    CachingKeyValueStoreWithHeaders(final KeyValueStore<Bytes, byte[]> underlying) {
        super(underlying, CacheType.TIMESTAMPED_KEY_VALUE_STORE_WITH_HEADERS);
    }
}
