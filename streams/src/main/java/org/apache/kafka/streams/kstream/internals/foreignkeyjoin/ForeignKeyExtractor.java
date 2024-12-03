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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An interface for extracting foreign keys from input records during foreign key joins in Kafka Streams.
 * This extractor is used to determine the key of the foreign table to join with based on the primary
 * table's record key and value.
 * <p>
 * The interface provides two factory methods:
 * <ul>
 *   <li>{@link #fromFunction(Function)} - when the foreign key depends only on the value</li>
 *   <li>{@link #fromBiFunction(BiFunction)} - when the foreign key depends on both key and value</li>
 * </ul>
 *
 * @param <K>  Type of primary table's key
 * @param <V>  Type of primary table's value
 * @param <KO> Type of the foreign key to extract
 */
@FunctionalInterface
public interface ForeignKeyExtractor<K, V, KO> {
    KO extract(K key, V value);

    static <K, V, KO> ForeignKeyExtractor<K, V, KO> fromFunction(Function<V, KO> function) {
        return (key, value) -> function.apply(value);
    }

    static <K, V, KO> ForeignKeyExtractor<K, V, KO> fromBiFunction(BiFunction<K, V, KO> biFunction) {
        return biFunction::apply;
    }
}
