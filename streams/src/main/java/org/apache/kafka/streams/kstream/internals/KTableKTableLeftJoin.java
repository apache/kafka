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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.Processor;

class KTableKTableLeftJoin<K, R, V1, V2> extends KTableKTableAbstractJoin<K, R, V1, V2> {

    private final KeyValueMapper<K, V1, K> keyValueMapper = new KeyValueMapper<K, V1, K>() {
        @Override
        public K apply(final K key, final V1 value) {
            return key;
        }
    };

    KTableKTableLeftJoin(KTableImpl<K, ?, V1> table1, KTableImpl<K, ?, V2> table2, ValueJoiner<V1, V2, R> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>> get() {
        return new KTableKTableLeftJoinProcessor<>(valueGetterSupplier2.get(),
                                                 joiner,
                                                 keyValueMapper,
                                                 sendOldValues);
    }

    @Override
    public KTableValueGetterSupplier<K, R> view() {
        return new KTableKTableLeftJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableLeftJoinValueGetterSupplier extends AbstractKTableKTableJoinValueGetterSupplier<K, R, V1, V2> {

        public KTableKTableLeftJoinValueGetterSupplier(KTableValueGetterSupplier<K, V1> valueGetterSupplier1, KTableValueGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, R> get() {
            return new KTableKTableLeftJoinValueGetter<>(valueGetterSupplier1.get(),
                                                         valueGetterSupplier2.get(),
                                                         joiner,
                                                         keyValueMapper);
        }
    }


}
