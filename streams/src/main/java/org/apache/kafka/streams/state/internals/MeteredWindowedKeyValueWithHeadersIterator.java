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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;

final class MeteredWindowedKeyValueWithHeadersIterator<K, VInner, VOuter> extends MeteredWindowedKeyValueIterator<K, VOuter> {
    private final Function<byte[], VInner> deserializeValue;
    private final BiFunction<byte[], Headers, K> deserializeKey;
    private final Function<VInner, Headers> headersExtractor;
    private final Function<VInner, VOuter> valueConverter;

    MeteredWindowedKeyValueWithHeadersIterator(
        final KeyValueIterator<Windowed<Bytes>, byte[]> iter,
        final Sensor operationSensor,
        final Sensor iteratorSensor,
        final Function<byte[], VInner> deserializeValue,
        final BiFunction<byte[], Headers, K> deserializeKey,
        final Function<VInner, Headers> headersExtractor,
        final Function<VInner, VOuter> valueConverter,
        final Time time,
        final LongAdder numOpenIterators,
        final Set<MeteredIterator> openIterators
    ) {
        super(
            iter,
            operationSensor,
            iteratorSensor,
            null, // should not be used in super-class
            null, // should not be used in super-class
            time,
            numOpenIterators,
            openIterators
        );

        this.deserializeValue = deserializeValue;
        this.deserializeKey = deserializeKey;
        this.headersExtractor = headersExtractor;
        this.valueConverter = valueConverter;
    }

    @Override
    public KeyValue<Windowed<K>, VOuter> next() {
        final KeyValue<Windowed<Bytes>, byte[]> next = iter.next();
        final VInner valueTimestampHeaders = deserializeValue.apply(next.value);
        return KeyValue.pair(
            windowedKey(next.key, headersExtractor.apply(valueTimestampHeaders)),
            valueConverter.apply(valueTimestampHeaders)
        );
    }

    private Windowed<K> windowedKey(final Windowed<Bytes> bytesKey, final Headers headers) {
        final K key = deserializeKey.apply(bytesKey.key().get(), headers);
        return new Windowed<>(key, bytesKey.window());
    }
}
