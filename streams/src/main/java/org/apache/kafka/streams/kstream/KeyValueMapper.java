/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.KeyValue;

/**
 * The {@code KeyValueMapper} interface for mapping a {@link KeyValue key-value pair} to a new value of arbitrary type.
 * For example, it can be used to
 * <ul>
 * <li>map from an input {@link KeyValue} pair to an output {@link KeyValue} pair with different key and/or value type
 *     (for this case output type {@code VR == }{@link KeyValue KeyValue&lt;NewKeyType,NewValueType&gt;})</li>
 * <li>map from an input record to a new key (with arbitrary key type as specified by {@code VR})</li>
 * </ul>
 * This is a stateless record-by-record operation, i.e, {@link #apply(Object, Object)} is invoked individually for each
 * record of a stream (cf. {@link Transformer} for stateful record transformation).
 * {@code KeyValueMapper} is a generalization of {@link ValueMapper}.
 *
 * @param <K>  key type
 * @param <V>  value type
 * @param <VR> mapped value type
 * @see ValueMapper
 * @see Transformer
 * @see KStream#map(KeyValueMapper)
 * @see KStream#flatMap(KeyValueMapper)
 * @see KStream#selectKey(KeyValueMapper)
 * @see KStream#groupBy(KeyValueMapper)
 * @see KStream#groupBy(KeyValueMapper, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
 * @see KTable#groupBy(KeyValueMapper)
 * @see KTable#groupBy(KeyValueMapper, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
 * @see KTable#toStream(KeyValueMapper)
 */
@InterfaceStability.Unstable
public interface KeyValueMapper<K, V, VR> {

    /**
     * Map a record with the given key and value to a new value.
     *
     * @param key   the key of the record
     * @param value the value of the record
     * @return the new value
     */
    VR apply(final K key, final V value);
}
