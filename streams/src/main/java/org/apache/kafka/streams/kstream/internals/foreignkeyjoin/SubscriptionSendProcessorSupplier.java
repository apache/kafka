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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.Murmur3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;

public class SubscriptionSendProcessorSupplier<K, KO, V> implements ProcessorSupplier<K, Change<V>, KO, SubscriptionWrapper<K>> {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSendProcessorSupplier.class);

    private final Function<V, KO> foreignKeyExtractor;
    private final Supplier<String> foreignKeySerdeTopicSupplier;
    private final Supplier<String> valueSerdeTopicSupplier;
    private final boolean leftJoin;
    private Serializer<KO> foreignKeySerializer;
    private Serializer<V> valueSerializer;
    private boolean useVersionedSemantics;

    public SubscriptionSendProcessorSupplier(final Function<V, KO> foreignKeyExtractor,
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
    public Processor<K, Change<V>, KO, SubscriptionWrapper<K>> get() {
        return new UnbindChangeProcessor();
    }

    public void setUseVersionedSemantics(final boolean useVersionedSemantics) {
        this.useVersionedSemantics = useVersionedSemantics;
    }

    // VisibleForTesting
    public boolean isUseVersionedSemantics() {
        return useVersionedSemantics;
    }

    private class UnbindChangeProcessor extends ContextualProcessor<K, Change<V>, KO, SubscriptionWrapper<K>> {

        private Sensor droppedRecordsSensor;
        private String foreignKeySerdeTopic;
        private String valueSerdeTopic;
        private long[] recordHash;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<KO, SubscriptionWrapper<K>> context) {
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
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            // clear cashed hash from previous record
            recordHash = null;
            // drop out-of-order records from versioned tables (cf. KIP-914)
            if (useVersionedSemantics && !record.value().isLatest) {
                LOG.info("Skipping out-of-order record from versioned table while performing table-table join.");
                droppedRecordsSensor.record();
                return;
            }
            if (leftJoin) {
                leftJoinInstructions(record);
            } else {
                defaultJoinInstructions(record);
            }
        }

        private void leftJoinInstructions(final Record<K, Change<V>> record) {
            if (record.value().oldValue != null) {
                final KO oldForeignKey = foreignKeyExtractor.apply(record.value().oldValue);
                final KO newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.apply(record.value().newValue);
                if (oldForeignKey != null && !Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
                }
                forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            } else if (record.value().newValue != null) {
                final KO newForeignKey = foreignKeyExtractor.apply(record.value().newValue);
                forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            }
        }

        private void defaultJoinInstructions(final Record<K, Change<V>> record) {
            if (record.value().oldValue != null) {
                final KO oldForeignKey = foreignKeyExtractor.apply(record.value().oldValue);
                final KO newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.apply(record.value().newValue);

                if (oldForeignKey == null && newForeignKey == null) {
                    logSkippedRecordDueToNullForeignKey();
                } else if (oldForeignKey == null) {
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                } else if (newForeignKey == null) {
                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
                } else if (!Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
                    //Different Foreign Key - delete the old key value and propagate the new one.
                    //Delete it from the oldKey's state store
                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
                    //Add to the newKey's state store. Additionally, propagate null if no FK is found there,
                    //since we must "unset" any output set by the previous FK-join. This is true for both INNER
                    //and LEFT join.
                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
                } else { // unchanged FK
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                }
            } else if (record.value().newValue != null) {
                final KO newForeignKey = foreignKeyExtractor.apply(record.value().newValue);
                if (newForeignKey == null) {
                    logSkippedRecordDueToNullForeignKey();
                } else {
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                }
            }
        }

        private byte[] serialize(final KO key) {
            return foreignKeySerializer.serialize(foreignKeySerdeTopic, key);
        }

        private void forward(final Record<K, Change<V>> record, final KO foreignKey, final Instruction deleteKeyNoPropagate) {
            final SubscriptionWrapper<K> wrapper = new SubscriptionWrapper<>(
                hash(record),
                deleteKeyNoPropagate,
                record.key(),
                context().recordMetadata().get().partition()
            );
            context().forward(record.withKey(foreignKey).withValue(wrapper));
        }

        private long[] hash(final Record<K, Change<V>> record) {
            if (recordHash == null) {
                recordHash = record.value().newValue == null
                    ? null
                    : Murmur3.hash128(valueSerializer.serialize(valueSerdeTopic, record.value().newValue));
            }
            return recordHash;
        }

        private void logSkippedRecordDueToNullForeignKey() {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn(
                    "Skipping record due to null foreign key. topic=[{}] partition=[{}] offset=[{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                LOG.warn("Skipping record due to null foreign key. Topic, partition, and offset not known.");
            }
            droppedRecordsSensor.record();
        }
    }
}
