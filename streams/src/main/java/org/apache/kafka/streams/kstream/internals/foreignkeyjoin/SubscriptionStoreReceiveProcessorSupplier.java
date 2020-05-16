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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionStoreReceiveProcessorSupplier<K, KO>
    implements ProcessorSupplier<KO, SubscriptionWrapper<K>> {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStoreReceiveProcessorSupplier.class);

    private final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder;
    private final CombinedKeySchema<KO, K> keySchema;

    public SubscriptionStoreReceiveProcessorSupplier(
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> storeBuilder,
        final CombinedKeySchema<KO, K> keySchema) {

        this.storeBuilder = storeBuilder;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<KO, SubscriptionWrapper<K>> get() {

        return new AbstractProcessor<KO, SubscriptionWrapper<K>>() {

            private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> store;
            private Sensor droppedRecordsSensor;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                final InternalProcessorContext internalProcessorContext = (InternalProcessorContext) context;

                droppedRecordsSensor = TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(
                    Thread.currentThread().getName(),
                    internalProcessorContext.taskId().toString(),
                    internalProcessorContext.metrics()
                );
                store = internalProcessorContext.getStateStore(storeBuilder);

                keySchema.init(context);
            }

            @Override
            public void process(final KO key, final SubscriptionWrapper<K> value) {
                if (key == null) {
                    LOG.warn(
                        "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        value, context().topic(), context().partition(), context().offset()
                    );
                    droppedRecordsSensor.record();
                    return;
                }
                if (value.getVersion() != SubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionWrapper is of an incompatible version.");
                }

                final Bytes subscriptionKey = keySchema.toBytes(key, value.getPrimaryKey());

                final ValueAndTimestamp<SubscriptionWrapper<K>> newValue = ValueAndTimestamp.make(value, context().timestamp());
                final ValueAndTimestamp<SubscriptionWrapper<K>> oldValue = store.get(subscriptionKey);

                //This store is used by the prefix scanner in ForeignJoinSubscriptionProcessorSupplier
                if (value.getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE) ||
                    value.getInstruction().equals(SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE)) {
                    store.delete(subscriptionKey);
                } else {
                    store.put(subscriptionKey, newValue);
                }
                final Change<ValueAndTimestamp<SubscriptionWrapper<K>>> change = new Change<>(newValue, oldValue);
                // note: key is non-nullable
                // note: newValue is non-nullable
                context().forward(
                    new CombinedKey<>(key, value.getPrimaryKey()),
                    change,
                    To.all().withTimestamp(newValue.timestamp())
                );
            }
        };
    }
}