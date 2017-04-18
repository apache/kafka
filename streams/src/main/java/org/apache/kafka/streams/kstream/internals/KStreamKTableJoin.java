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

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamKTableJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {

    private final KeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>() {
        @Override
        public K apply(final K key, final V1 value) {
            return key;
        }
    };
    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier;
    private final ValueJoiner<? super V1, ? super V2, R> joiner;
    private final boolean leftJoin;

    KStreamKTableJoin(final KTableValueGetterSupplier<K, V2> valueGetterSupplier, final ValueJoiner<? super V1, ? super V2, R> joiner, final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKTableJoinProcessor<>(valueGetterSupplier.get(), keyValueMapper, joiner, leftJoin);
    }

}
