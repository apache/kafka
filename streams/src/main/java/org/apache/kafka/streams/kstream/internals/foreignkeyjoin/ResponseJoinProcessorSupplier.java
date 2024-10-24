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
import org.apache.kafka.common.hash.Murmur3;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.ValueAndTimestamp;

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
public class ResponseJoinProcessorSupplier<K, V, VO, VR> implements ProcessorSupplier<K, SubscriptionResponseWrapper<VO>, K, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseJoinProcessorSupplier.class);
    private final KTableValueGetterSupplier<K, V> valueGetterSupplier;
    private final Serializer<V> constructionTimeValueSerializer;
    private final Supplier<String> valueHashSerdePseudoTopicSupplier;
    private final ValueJoiner<V, VO, VR> joiner;
    private final boolean leftJoin;

    public ResponseJoinProcessorSupplier(final KTableValueGetterSupplier<K, V> valueGetterSupplier,
                                         final Serializer<V> valueSerializer,
                                         final Supplier<String> valueHashSerdePseudoTopicSupplier,
                                         final ValueJoiner<V, VO, VR> joiner,
                                         final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        constructionTimeValueSerializer = valueSerializer;
        this.valueHashSerdePseudoTopicSupplier = valueHashSerdePseudoTopicSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, SubscriptionResponseWrapper<VO>, K, VR> get() {
        return new ContextualProcessor<K, SubscriptionResponseWrapper<VO>, K, VR>() {
            private String valueHashSerdePseudoTopic;
            private Serializer<V> runtimeValueSerializer = constructionTimeValueSerializer;

            private KTableValueGetter<K, V> valueGetter;
            private Sensor droppedRecordsSensor;


            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext<K, VR> context) {
                super.init(context);
                valueHashSerdePseudoTopic = valueHashSerdePseudoTopicSupplier.get();
                valueGetter = valueGetterSupplier.get();
                valueGetter.init(context);
                if (runtimeValueSerializer == null) {
                    runtimeValueSerializer = (Serializer<V>) context.valueSerde().serializer();
                }

                final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;
                droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                        Thread.currentThread().getName(),
                        internalProcessorContext.taskId().toString(),
                        internalProcessorContext.metrics()
                );
            }

            @Override
            public void process(final Record<K, SubscriptionResponseWrapper<VO>> record) {
                if (record.value().version() != SubscriptionResponseWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionResponseWrapper. Need to ensure that there is
                    //compatibility with previous versions to enable rolling upgrades. Must develop a strategy for
                    //upgrading from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionResponseWrapper is of an incompatible version.");
                }
                final ValueAndTimestamp<V> currentValueWithTimestamp = valueGetter.get(record.key());

                final long[] currentHash = currentValueWithTimestamp == null ?
                    null :
                    Murmur3.hash128(runtimeValueSerializer.serialize(valueHashSerdePseudoTopic, currentValueWithTimestamp.value()));

                final long[] messageHash = record.value().originalValueHash();

                //If this value doesn't match the current value from the original table, it is stale and should be discarded.
                if (java.util.Arrays.equals(messageHash, currentHash)) {
                    final VR result;

                    if (record.value().foreignValue() == null && (!leftJoin || currentValueWithTimestamp == null)) {
                        result = null; //Emit tombstone
                    } else {
                        result = joiner.apply(currentValueWithTimestamp == null ? null : currentValueWithTimestamp.value(), record.value().foreignValue());
                    }
                    context().forward(record.withValue(result));
                } else {
                    LOG.trace("Dropping FK-join response due to hash mismatch. Expected {}. Actual {}", messageHash, currentHash);
                    droppedRecordsSensor.record();
                }
            }
        };
    }
}
