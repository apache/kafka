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

import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * RocksDB-backed time-ordered window store with support for record headers.
 * <p>
 * This store extends {@link RocksDBTimeOrderedWindowStore} and implements both
 * {@link TimestampedBytesStore} (for timestamp support) and {@link HeadersBytesStore}
 * (for header support) marker interfaces.
 * <p>
 * The storage format for values is: [headersSize(varint)][headersBytes][timestamp(8)][value]
 *
 * @see RocksDBTimeOrderedWindowStore
 * @see HeadersBytesStore
 * @see TimestampedBytesStore
 */
class RocksDBTimeOrderedWindowStoreWithHeaders extends RocksDBTimeOrderedWindowStore<WindowSegmentWithHeaders> implements TimestampedBytesStore, HeadersBytesStore {

    RocksDBTimeOrderedWindowStoreWithHeaders(final RocksDBTimeOrderedWindowSegmentedBytesStore<WindowSegmentWithHeaders> store,
                                             final boolean retainDuplicates,
                                             final long windowSize) {
        super(store, retainDuplicates, windowSize);
    }
}
