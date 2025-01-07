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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.StateSerdes;

public class StoreSerdeInitializer {
    static <K, V> StateSerdes<K, V> prepareStoreSerde(final StateStoreContext context,
                                                      final String storeName,
                                                      final String changelogTopic,
                                                      final Serde<K> keySerde,
                                                      final Serde<V> valueSerde,
                                                      final PrepareFunc<V> prepareValueSerdeFunc) {
        return new StateSerdes<>(
            changelogTopic,
            prepareSerde(WrappingNullableUtils::prepareKeySerde, storeName, keySerde, new SerdeGetter(context), true, context.taskId()),
            prepareSerde(prepareValueSerdeFunc, storeName, valueSerde, new SerdeGetter(context), false, context.taskId())
        );
    }

    private static <T> Serde<T> prepareSerde(final PrepareFunc<T> prepare,
                                             final String storeName,
                                             final Serde<T> serde,
                                             final SerdeGetter getter,
                                             final Boolean isKey,
                                             final TaskId taskId) {

        final String serdeType = isKey ? "key" : "value";
        try {
            return prepare.prepareSerde(serde, getter);
        } catch (final ConfigException | StreamsException e) {
            throw new StreamsException(String.format("Failed to initialize %s serdes for store %s", serdeType, storeName), e, taskId);
        }
    }
}

interface PrepareFunc<T> {
    Serde<T> prepareSerde(Serde<T> serde, SerdeGetter getter);
}
