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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Receives {@code SubscriptionResponseWrapper<VO>} events and filters out events which do not match the current hash
 * of the primary key. This eliminates race-condition results for rapidly-changing foreign-keys for a given primary key.
 * Applies the join and emits nulls according to LEFT/INNER rules.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of primary values
 * @param <VO> Type of foreign values
 * @param <VR> Type of joined result of primary and foreign values
 */
public class ResponseJoinProcessorSupplier<K, V, VO, VR> implements ProcessorSupplier<Bytes, SubscriptionResponseWrapper<byte[]>, K, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseJoinProcessorSupplier.class);
    private final KTableValueGetterSupplier<Bytes, byte[]> rawValueGetterSupplier;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> leftValueDeserializer;
    private final Deserializer<VO> rightValueDeserializer;
    private final Supplier<String> valueHashSerdePseudoTopicSupplier;
    private final ValueJoiner<V, VO, VR> joiner;
    private final boolean leftJoin;

    public ResponseJoinProcessorSupplier(final KTableValueGetterSupplier<Bytes, byte[]> rawValueGetterSupplier,
                                         final Deserializer<K> keyDeserializer,
                                         final Deserializer<V> leftValueDeserializer,
                                         final Deserializer<VO> rightValueDeserializer,
                                         final Supplier<String> valueHashSerdePseudoTopicSupplier,
                                         final ValueJoiner<V, VO, VR> joiner,
                                         final boolean leftJoin) {
        this.rawValueGetterSupplier = rawValueGetterSupplier;
        this.keyDeserializer = keyDeserializer;
        this.leftValueDeserializer = leftValueDeserializer;
        this.rightValueDeserializer = rightValueDeserializer;
        this.valueHashSerdePseudoTopicSupplier = valueHashSerdePseudoTopicSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<Bytes, SubscriptionResponseWrapper<byte[]>, K, VR> get() {
        return new ContextualProcessor<Bytes, SubscriptionResponseWrapper<byte[]>, K, VR>() {
            private String valueHashSerdePseudoTopic;
            private Deserializer<K> keyDeserializer = ResponseJoinProcessorSupplier.this.keyDeserializer;
            private Deserializer<V> leftValueDeserializer = ResponseJoinProcessorSupplier.this.leftValueDeserializer;
            private Deserializer<VO> rightValueDeserializer = ResponseJoinProcessorSupplier.this.rightValueDeserializer;
            private KTableValueGetter<Bytes, byte[]> rawValueGetter;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext<K, VR> context) {
                super.init(context);
                valueHashSerdePseudoTopic = valueHashSerdePseudoTopicSupplier.get();
                rawValueGetter = rawValueGetterSupplier.get();
                rawValueGetter.init(context);
                if (keyDeserializer == null) {
                    keyDeserializer = (Deserializer<K>) context.keySerde().deserializer();
                }
                if (leftValueDeserializer == null) {
                    leftValueDeserializer = (Deserializer<V>) context.valueSerde().deserializer();
                }
                if (rightValueDeserializer == null) {
                    rightValueDeserializer = (Deserializer<VO>) context.valueSerde().deserializer();
                }
            }

            @Override
            public void process(final Record<Bytes, SubscriptionResponseWrapper<byte[]>> record) {
                if (record.value().getVersion() != SubscriptionResponseWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionResponseWrapper. Need to ensure that there is
                    //compatibility with previous versions to enable rolling upgrades. Must develop a strategy for
                    //upgrading from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionResponseWrapper is of an incompatible version.");
                }
                final ValueAndTimestamp<byte[]> currentValueWithTimestamp = rawValueGetter.get(record.key());

                final long[] currentHash = currentValueWithTimestamp == null ?
                    null :
                    //Murmur3.hash128(runtimeValueSerializer.serialize(valueHashSerdePseudoTopic, currentValueWithTimestamp.value()));
                    Murmur3.hash128(currentValueWithTimestamp.value());

                final long[] messageHash = record.value().getOriginalValueHash();

                //If this value doesn't match the current value from the original table, it is stale and should be discarded.
                if (java.util.Arrays.equals(messageHash, currentHash)) {
                    final VR result;

                    final byte[] rightValue = record.value().getForeignValue();
                    if (rightValue == null && (!leftJoin || currentValueWithTimestamp == null)) {
                        result = null; //Emit tombstone
                    } else {
                        result = joiner.apply(
                                currentValueWithTimestamp == null ?
                                    null :
                                    leftValueDeserializer.deserialize(valueHashSerdePseudoTopic, currentValueWithTimestamp.value()),
                                rightValue == null ?
                                    null :
                                    rightValueDeserializer.deserialize(null, rightValue)
                        );
                    }
                    context().forward(new Record<>(
                        keyDeserializer.deserialize(null, record.key().get()),
                        result,
                        record.timestamp()
                    ));
                } else {
                    LOG.trace("Dropping FK-join response due to hash mismatch. Expected {}. Actual {}", messageHash, currentHash);
                }
            }
        };
    }
}
