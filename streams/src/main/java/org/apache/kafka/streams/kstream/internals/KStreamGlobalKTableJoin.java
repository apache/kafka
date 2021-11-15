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
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

class KStreamGlobalKTableJoin<K1, V1, K2, V2, VOut> implements ProcessorSupplier<K1, V1, K1, VOut> {

    private final KTableValueGetterSupplier<K2, V2> valueGetterSupplier;
    private final ValueJoinerWithKey<? super K1, ? super V1, ? super V2, ? extends VOut> joiner;
    private final KeyValueMapper<? super K1, ? super V1, ? extends K2> mapper;
    private final boolean leftJoin;

    KStreamGlobalKTableJoin(final KTableValueGetterSupplier<K2, V2> valueGetterSupplier,
                            final ValueJoinerWithKey<? super K1, ? super V1, ? super V2, ? extends VOut> joiner,
                            final KeyValueMapper<? super K1, ? super V1, ? extends K2> mapper,
                            final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.joiner = joiner;
        this.mapper = mapper;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K1, V1, K1, VOut> get() {
        return new KStreamKTableJoinProcessor<>(valueGetterSupplier.get(), mapper, joiner, leftJoin);
    }
}
