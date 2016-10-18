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

class KTableGlobalKTableLeftJoin<K1, K2, R, V1, V2> implements KTableProcessorSupplier<K1, V1, R> {

    private final KTableValueGetterSupplier<K1, V1> valueGetterSupplier;
    private final KTableValueGetterSupplier<K2, V2> globalTableValueGetterSupplier;
    private final ValueJoiner<V1, V2, R> joiner;
    private final KeyValueMapper<K1, V1, K2> mapper;
    private boolean sendOldValues;

    KTableGlobalKTableLeftJoin(final KTableValueGetterSupplier<K1, V1> tableValueGetterSupplier,
                               final KTableValueGetterSupplier<K2, V2> globalTableValueGetterSupplier,
                               final ValueJoiner<V1, V2, R> joiner,
                               final KeyValueMapper<K1, V1, K2> mapper) {
        this.valueGetterSupplier = tableValueGetterSupplier;
        this.globalTableValueGetterSupplier = globalTableValueGetterSupplier;
        this.joiner = joiner;
        this.mapper = mapper;
    }

    @Override
    public Processor<K1, Change<V1>> get() {
        return new KTableKTableLeftJoinProcessor<>(globalTableValueGetterSupplier.get(),
                                                   joiner,
                                                   mapper,
                                                   sendOldValues);
    }

    @Override
    public KTableValueGetterSupplier<K1, R> view() {
        return new KTableValueGetterSupplier<K1, R>() {
            @Override
            public KTableValueGetter<K1, R> get() {
                return new KTableKTableLeftJoinValueGetter<>(
                        valueGetterSupplier.get(),
                        globalTableValueGetterSupplier.get(),
                        joiner,
                        mapper);
            }

            @Override
            public String[] storeNames() {
                return valueGetterSupplier.storeNames(); // shouldn't need global?
            }
        };
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

}
