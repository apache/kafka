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
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

class KStreamKTableJoin<StreamKey, StreamValue, TableValue, VOut> implements ProcessorSupplier<StreamKey, StreamValue, StreamKey, VOut> {

    private final KeyValueMapper<StreamKey, StreamValue, StreamKey> keyValueMapper = (key, value) -> key;
    private final KTableValueGetterSupplier<StreamKey, TableValue> valueGetterSupplier;
    private final ValueJoinerWithKey<? super StreamKey, ? super StreamValue, ? super TableValue, ? extends VOut> joiner;
    private final boolean leftJoin;
    private final Optional<Duration> gracePeriod;
    private final Optional<String> storeName;
    private final Set<StoreBuilder<?>> stores;

    KStreamKTableJoin(final KTableValueGetterSupplier<StreamKey, TableValue> valueGetterSupplier,
                      final ValueJoinerWithKey<? super StreamKey, ? super StreamValue, ? super TableValue, ? extends VOut> joiner,
                      final boolean leftJoin,
                      final Optional<Duration> gracePeriod,
                      final Optional<StoreBuilder<?>> bufferStoreBuilder) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
        this.gracePeriod = gracePeriod;
        this.storeName = bufferStoreBuilder.map(StoreBuilder::name);

        this.stores = bufferStoreBuilder.<Set<StoreBuilder<?>>>map(Collections::singleton).orElse(null);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return stores;
    }

    @Override
    public Processor<StreamKey, StreamValue, StreamKey, VOut> get() {
        return new KStreamKTableJoinProcessor<>(valueGetterSupplier.get(), keyValueMapper, joiner, leftJoin, gracePeriod, storeName);
    }

}
