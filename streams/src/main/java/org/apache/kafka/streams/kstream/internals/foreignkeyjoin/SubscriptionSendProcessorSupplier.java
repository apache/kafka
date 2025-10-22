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
import java.util.function.Supplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;

public class SubscriptionSendProcessorSupplier<KLeft, VLeft, KRight>
    implements ProcessorSupplier<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSendProcessorSupplier.class);

    private final ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> foreignKeyExtractor;
    private final Supplier<String> foreignKeySerdeTopicSupplier;
    private final Supplier<String> valueSerdeTopicSupplier;
    private final boolean leftJoin;
    private Serializer<KRight> foreignKeySerializer;
    private Serializer<VLeft> valueSerializer;
    private boolean useVersionedSemantics;

    public SubscriptionSendProcessorSupplier(final ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> foreignKeyExtractor,
                                             final Supplier<String> foreignKeySerdeTopicSupplier,
                                             final Supplier<String> valueSerdeTopicSupplier,
                                             final Serde<KRight> foreignKeySerde,
                                             final Serializer<VLeft> valueSerializer,
                                             final boolean leftJoin) {
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.foreignKeySerdeTopicSupplier = foreignKeySerdeTopicSupplier;
        this.valueSerdeTopicSupplier = valueSerdeTopicSupplier;
        this.valueSerializer = valueSerializer;
        this.leftJoin = leftJoin;
        foreignKeySerializer = foreignKeySerde == null ? null : foreignKeySerde.serializer();
    }

    @Override
    public Processor<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> get() {
        return new UnbindChangeProcessor();
    }

    public void setUseVersionedSemantics(final boolean useVersionedSemantics) {
        this.useVersionedSemantics = useVersionedSemantics;
    }

    // VisibleForTesting
    public boolean isUseVersionedSemantics() {
        return useVersionedSemantics;
    }

    private class UnbindChangeProcessor extends ContextualProcessor<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> {

        private Sensor droppedRecordsSensor;
        private String foreignKeySerdeTopic;
        private String valueSerdeTopic;
        private long[] recordHash;

        @SuppressWarnings({"unchecked", "resource"})
        @Override
        public void init(final ProcessorContext<KRight, SubscriptionWrapper<KLeft>> context) {
            super.init(context);
            foreignKeySerdeTopic = foreignKeySerdeTopicSupplier.get();
            valueSerdeTopic = valueSerdeTopicSupplier.get();
            // get default key serde if it wasn't supplied directly at construction
            if (foreignKeySerializer == null) {
                foreignKeySerializer = (Serializer<KRight>) context.keySerde().serializer();
            }
            if (valueSerializer == null) {
                valueSerializer = (Serializer<VLeft>) context.valueSerde().serializer();
            }
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
        }

        @Override
        public void process(final Record<KLeft, Change<VLeft>> record) {
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

        private void leftJoinInstructions(final Record<KLeft, Change<VLeft>> record) {
            final VLeft oldValue = record.value().oldValue;
            final VLeft newValue = record.value().newValue;

            if (oldValue == null && newValue == null) {
                // no output for idempotent left hand side deletes
                return;
            }

            final KRight oldForeignKey = oldValue == null ? null : foreignKeyExtractor.extract(record.key(), oldValue);
            final KRight newForeignKey = newValue == null ? null : foreignKeyExtractor.extract(record.key(), newValue);

            final boolean maybeUnsubscribe = oldForeignKey != null;
            if (maybeUnsubscribe) {
                // delete old subscription only if FK changed
                //
                // if FK did change, we need to explicitly delete the old subscription,
                // because the new subscription goes to a different partition
                if (foreignKeyChanged(newForeignKey, oldForeignKey)) {
                    // this may lead to unnecessary tombstones if the old FK did not join;
                    // however, we cannot avoid it as we have no means to know if the old FK joined or not
                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
                }
            }

            // for all cases (insert, update, and delete), we send a new subscription;
            // we need to get a response back for all cases to always produce a left-join result
            //
            // note: for delete, `newForeignKey` is null, what is a "hack"
            // no actual subscription will be added for null-FK on the right hand side, but we still get the response back we need
            //
            // this may lead to unnecessary tombstones if the old FK did not join;
            // however, we cannot avoid it as we have no means to know if the old FK joined or not
            forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
        }

        private void defaultJoinInstructions(final Record<KLeft, Change<VLeft>> record) {
            final VLeft oldValue = record.value().oldValue;
            final VLeft newValue = record.value().newValue;

            final KRight oldForeignKey = oldValue == null ? null : foreignKeyExtractor.extract(record.key(), oldValue);
            final boolean needToUnsubscribe = oldForeignKey != null;

            // if left row is inserted or updated, subscribe to new FK (if new FK is valid)
            if (newValue != null) {
                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), newValue);

                if (newForeignKey == null) { // invalid FK
                    logSkippedRecordDueToNullForeignKey();
                    if (needToUnsubscribe) {
                        // delete old subscription
                        //
                        // this may lead to unnecessary tombstones if the old FK did not join;
                        // however, we cannot avoid it as we have no means to know if the old FK joined or not
                        forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
                    }
                } else { // valid FK
                    // regular insert/update

                    if (needToUnsubscribe) {
                        // update case

                        if (foreignKeyChanged(newForeignKey, oldForeignKey)) {
                            // if FK did change, we need to explicitly delete the old subscription,
                            // because the new subscription goes to a different partition
                            //
                            // we don't need any response, as we only want a response from the new subscription
                            forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);

                            // subscribe for new FK (note, could be on a different task/node than the old FK)
                            // additionally, propagate null if no FK is found so we can delete the previous result (if any)
                            //
                            // this may lead to unnecessary tombstones if the old FK did not join and the new FK key does not join either;
                            // however, we cannot avoid it as we have no means to know if the old FK joined or not
                            forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
                        } else {
                            // if FK did not change, we only need a response from the new FK subscription, if there is a join
                            // if there is no join, we know that the old row did not join either (as it used the same FK)
                            // and thus we don't need to propagate an idempotent null result
                            forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                        }
                    } else {
                        // insert case

                        // subscribe to new key
                        // don't propagate null if no FK is found:
                        // for inserts, we know that there is no need to delete any previous result
                        forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                    }
                }
            } else {
                // left row is deleted
                if (needToUnsubscribe) {
                    // this may lead to unnecessary tombstones if the old FK did not join;
                    // however, we cannot avoid it as we have no means to know if the old FK joined or not
                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
                }
            }
        }

        private boolean foreignKeyChanged(final KRight newForeignKey, final KRight oldForeignKey) {
            return !Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey));
        }

        private byte[] serialize(final KRight key) {
            return foreignKeySerializer.serialize(foreignKeySerdeTopic, key);
        }

        private void forward(final Record<KLeft, Change<VLeft>> record, final KRight foreignKey, final Instruction deleteKeyNoPropagate) {
            final SubscriptionWrapper<KLeft> wrapper = new SubscriptionWrapper<>(
                hash(record),
                deleteKeyNoPropagate,
                record.key(),
                context().recordMetadata().get().partition()
            );
            context().forward(record.withKey(foreignKey).withValue(wrapper));
        }

        private long[] hash(final Record<KLeft, Change<VLeft>> record) {
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
