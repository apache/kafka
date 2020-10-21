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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;

public class ForeignJoinSubscriptionSendProcessorSupplier<K, KO, V> implements ProcessorSupplier<K, Change<V>> {
    private static final Logger LOG = LoggerFactory.getLogger(ForeignJoinSubscriptionSendProcessorSupplier.class);

    private final Function<V, KO> foreignKeyExtractor;
    private final Supplier<String> foreignKeySerdeTopicSupplier;
    private final Supplier<String> valueSerdeTopicSupplier;
    private final boolean leftJoin;
    private Serializer<KO> foreignKeySerializer;
    private Serializer<V> valueSerializer;

    public ForeignJoinSubscriptionSendProcessorSupplier(final Function<V, KO> foreignKeyExtractor,
                                                        final Supplier<String> foreignKeySerdeTopicSupplier,
                                                        final Supplier<String> valueSerdeTopicSupplier,
                                                        final Serde<KO> foreignKeySerde,
                                                        final Serializer<V> valueSerializer,
                                                        final boolean leftJoin) {
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.foreignKeySerdeTopicSupplier = foreignKeySerdeTopicSupplier;
        this.valueSerdeTopicSupplier = valueSerdeTopicSupplier;
        this.valueSerializer = valueSerializer;
        this.leftJoin = leftJoin;
        foreignKeySerializer = foreignKeySerde == null ? null : foreignKeySerde.serializer();
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new UnbindChangeProcessor();
    }

    private class UnbindChangeProcessor extends AbstractProcessor<K, Change<V>> {

        private Sensor droppedRecordsSensor;
        private String foreignKeySerdeTopic;
        private String valueSerdeTopic;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            foreignKeySerdeTopic = foreignKeySerdeTopicSupplier.get();
            valueSerdeTopic = valueSerdeTopicSupplier.get();
            // get default key serde if it wasn't supplied directly at construction
            if (foreignKeySerializer == null) {
                foreignKeySerializer = (Serializer<KO>) context.keySerde().serializer();
            }
            if (valueSerializer == null) {
                valueSerializer = (Serializer<V>) context.valueSerde().serializer();
            }
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final long[] currentHash = change.newValue == null ?
                null :
                Murmur3.hash128(valueSerializer.serialize(valueSerdeTopic, change.newValue));

            if (change.oldValue != null) {
                final KO oldForeignKey = foreignKeyExtractor.apply(change.oldValue);
                if (oldForeignKey == null) {
                    LOG.warn(
                        "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        change.oldValue, context().topic(), context().partition(), context().offset()
                    );
                    droppedRecordsSensor.record();
                    return;
                }
                if (change.newValue != null) {
                    final KO newForeignKey = foreignKeyExtractor.apply(change.newValue);
                    if (newForeignKey == null) {
                        LOG.warn(
                            "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                            change.newValue, context().topic(), context().partition(), context().offset()
                        );
                        droppedRecordsSensor.record();
                        return;
                    }

                    final byte[] serialOldForeignKey =
                        foreignKeySerializer.serialize(foreignKeySerdeTopic, oldForeignKey);
                    final byte[] serialNewForeignKey =
                        foreignKeySerializer.serialize(foreignKeySerdeTopic, newForeignKey);
                    if (!Arrays.equals(serialNewForeignKey, serialOldForeignKey)) {
                        //Different Foreign Key - delete the old key value and propagate the new one.
                        //Delete it from the oldKey's state store
                        context().forward(oldForeignKey, new SubscriptionWrapper<>(currentHash, DELETE_KEY_NO_PROPAGATE, key));
                        //Add to the newKey's state store. Additionally, propagate null if no FK is found there,
                        //since we must "unset" any output set by the previous FK-join. This is true for both INNER
                        //and LEFT join.
                    }
                    context().forward(newForeignKey, new SubscriptionWrapper<>(currentHash, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, key));
                } else {
                    //A simple propagatable delete. Delete from the state store and propagate the delete onwards.
                    context().forward(oldForeignKey, new SubscriptionWrapper<>(currentHash, DELETE_KEY_AND_PROPAGATE, key));
                }
            } else if (change.newValue != null) {
                //change.oldValue is null, which means it was deleted at least once before, or it is brand new.
                //In either case, we only need to propagate if the FK_VAL is available, as the null from the delete would
                //have been propagated otherwise.

                final SubscriptionWrapper.Instruction instruction;
                if (leftJoin) {
                    //Want to send info even if RHS is null.
                    instruction = PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
                } else {
                    instruction = PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;
                }
                final KO newForeignKey = foreignKeyExtractor.apply(change.newValue);
                if (newForeignKey == null) {
                    LOG.warn(
                        "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        change.newValue, context().topic(), context().partition(), context().offset()
                    );
                    droppedRecordsSensor.record();
                } else {
                    context().forward(newForeignKey, new SubscriptionWrapper<>(currentHash, instruction, key));
                }
            }
        }
    }
}
