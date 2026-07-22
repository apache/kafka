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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.common.header.Headers;

/**
 * A read-only view of a record's {@code key}, {@code value}, {@code timestamp}, and {@code headers}.
 *
 * <p>This is the shared read surface of a record: the processing-layer {@link Record} extends it with
 * transform-and-forward builders ({@code withKey}/{@code withValue}/{@code withTimestamp}/{@code withHeaders}),
 * while an Interactive Query (IQv2) result is exactly this read-only snapshot and exposes nothing more.
 * It is the result type returned by the headers-aware IQv2 query types, which surface the record headers
 * persisted by header-aware state stores.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
@InterfaceAudience.Public
@Evolving
public interface ReadOnlyRecord<K, V> {

    /**
     * The key of the record. May be null.
     */
    K key();

    /**
     * The value of the record. May be null.
     */
    V value();

    /**
     * The timestamp of the record. Will never be negative.
     */
    long timestamp();

    /**
     * The headers of the record. Never null.
     *
     * <p>The returned {@link Headers} is part of a read-only view and must not be mutated by
     * callers.
     */
    // TODO (KIP-1356 follow-up): once the IQv2 query types land, records served as IQv2
    // results from a state store will have their headers frozen via RecordHeaders.setReadOnly(),
    // so that any attempt to mutate them throws IllegalStateException.
    Headers headers();
}
