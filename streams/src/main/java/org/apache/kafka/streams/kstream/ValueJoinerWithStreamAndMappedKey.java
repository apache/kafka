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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceAudience;

/**
 * The {@code ValueJoinerWithStreamAndMappedKey} interface for joining two values into a new value of
 * arbitrary type, with access to both the original {@link KStream} record and the mapped join key.
 * Used by {@link KStream}-{@link GlobalKTable} joins, where the join key is produced by a
 * {@link KeyValueMapper} and does not necessarily equal the {@link KStream} record's key.
 *
 *
 * @param <StreamKey> the type of the original {@link KStream} record key
 * @param <TableKey> the type of the mapped join key (the {@link GlobalKTable} lookup key)
 * @param <StreamValue> the type of the first (stream) value
 * @param <TableValue> the type of the second (table) value
 * @param <VOut> the type of the joined result value
 */
@FunctionalInterface
@InterfaceAudience.Public
public interface ValueJoinerWithStreamAndMappedKey<StreamKey, TableKey, StreamValue, TableValue, VOut> {

    /**
     * Return a joined value derived from {@code mappedKey}, {@code streamKey}, {@code value1} and {@code value2}.
     *
     * @param streamKey the {@link KStream} record's key. Read-only.
     * @param mappedKey the join key produced by the {@link KeyValueMapper} (i.e. the {@link GlobalKTable}
     *                  lookup key); may be {@code null} for a left-join when the mapper returns {@code null}.
     *                  Read-only.
     * @param value1    the {@link KStream} record's value
     * @param value2    the matching {@link GlobalKTable} value, or {@code null} for a left-join with no match
     * @return the joined value
     */
    VOut apply(final StreamKey streamKey, final TableKey mappedKey, final StreamValue value1, final TableValue value2);
}
