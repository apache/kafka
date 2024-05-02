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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionReceiveProcessorSupplier<K, KO>
    implements ProcessorSupplier<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionReceiveProcessorSupplier.class);

    private final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder;
    private final CombinedKeySchema<KO, K> keySchema;

    public SubscriptionReceiveProcessorSupplier(
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder,
        final CombinedKeySchema<KO, K> keySchema) {

        this.storeBuilder = storeBuilder;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> get() {

        return new ContextualProcessor<KO, SubscriptionWrapper<K>, CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>>() {

            private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> store;
            private Sensor droppedRecordsSensor;

            @Override
            public void init(final ProcessorContext<CombinedKey<KO, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> context) {
                super.init(context);
                final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;

                droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                    Thread.currentThread().getName(),
                    internalProcessorContext.taskId().toString(),
                    internalProcessorContext.metrics()
                );
                store = internalProcessorContext.getStateStore(storeBuilder);

                keySchema.init(context);
            }

            @Override
            public void process(final Record<KO, SubscriptionWrapper<K>> record) {
                if (record.key() == null && !SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE.equals(record.value().getInstruction())) {
                    dropRecord();
                    return;
                }
                if (record.value().getVersion() > SubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionWrapper is of an incompatible version.");
                }
                context().forward(
                    record.withKey(new CombinedKey<>(record.key(), record.value().getPrimaryKey()))
                        .withValue(inferChange(record))
                        .withTimestamp(record.timestamp())
                );
            }

            private Change<ValueAndTimestamp<SubscriptionWrapper<K>>> inferChange(final Record<KO, SubscriptionWrapper<K>> record) {
                if (record.key() == null) {
                    return new Change<>(ValueAndTimestamp.make(record.value(), record.timestamp()), null);
                } else {
                    return inferBasedOnState(record);
                }
            }

            private Change<ValueAndTimestamp<SubscriptionWrapper<K>>> inferBasedOnState(final Record<KO, SubscriptionWrapper<K>> record) {
                final Bytes subscriptionKey = keySchema.toBytes(record.key(), record.value().getPrimaryKey());

                final ValueAndTimestamp<SubscriptionWrapper<K>> newValue = ValueAndTimestamp.make(record.value(), record.timestamp());
                final ValueAndTimestamp<SubscriptionWrapper<K>> oldValue = store.get(subscriptionKey);

                //This store is used by the prefix scanner in ForeignTableJoinProcessorSupplier
                if (record.value().getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE) ||
                    record.value().getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE)) {
                    store.delete(subscriptionKey);
                } else {
                    store.put(subscriptionKey, newValue);
                }
                return new Change<>(newValue, oldValue);
            }

            private void dropRecord() {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null foreign key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null foreign key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
            }
        };
    }
}