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

import java.time.Duration;
import java.util.Optional;

class KStreamKTableJoin<K, V1, V2, VOut> implements ProcessorSupplier<K, V1, K, VOut> {

    private final KeyValueMapper<K, V1, K> keyValueMapper = (key, value) -> key;
    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier;
    private final ValueJoinerWithKey<? super K, ? super V1, ? super V2, VOut> joiner;
    private final boolean leftJoin;
    private final Optional<Duration> gracePeriod;
    private final Optional<String> storeName;


    KStreamKTableJoin(final KTableValueGetterSupplier<K, V2> valueGetterSupplier,
                      final ValueJoinerWithKey<? super K, ? super V1, ? super V2, VOut> joiner,
                      final boolean leftJoin,
                      final Optional<Duration> gracePeriod,
                      final Optional<String> storeName) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
        this.gracePeriod = gracePeriod;
        this.storeName = storeName;
    }

    @Override
    public Processor<K, V1, K, VOut> get() {
        return new KStreamKTableJoinProcessor<>(valueGetterSupplier.get(), keyValueMapper, joiner, leftJoin, gracePeriod, storeName);
    }

}
