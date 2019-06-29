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
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;

public class ForeignKeySingleLookupProcessorSupplier<K, KO, VO>
        implements ProcessorSupplier<KO, SubscriptionWrapper<K>> {
    private static final Logger LOG = LoggerFactory.getLogger(ForeignKeySingleLookupProcessorSupplier.class);

    private final String stateStoreName;
    private final KTableValueGetterSupplier<KO, VO> foreignValueGetterSupplier;

    public ForeignKeySingleLookupProcessorSupplier(final String stateStoreName,
                                                   final KTableValueGetterSupplier<KO, VO> foreignValueGetter) {
        this.stateStoreName = stateStoreName;
        this.foreignValueGetterSupplier = foreignValueGetter;
    }

    @Override
    public Processor<KO, SubscriptionWrapper<K>> get() {

        return new AbstractProcessor<KO, SubscriptionWrapper<K>>() {

            private TimestampedKeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper> store;
            private KTableValueGetter<KO, VO> foreignValues;
            private StreamsMetricsImpl metrics;
            private Sensor skippedRecordsSensor;

            @Override@SuppressWarnings("unchecked")
            public void init(final ProcessorContext context) {
                super.init(context);
                metrics = (StreamsMetricsImpl) context.metrics();
                skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
                foreignValues = foreignValueGetterSupplier.get();
                foreignValues.init(context);
                store = (TimestampedKeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(final KO key, final SubscriptionWrapper<K> value) {
                if (key == null) {
                    LOG.warn(
                            "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                            value, context().topic(), context().partition(), context().offset()
                    );
                    skippedRecordsSensor.record();
                    return;
                }
                if (value.getVersion() != SubscriptionWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionWrapper. Need to ensure that there is compatibility
                    //with previous versions to enable rolling upgrades. Must develop a strategy for upgrading
                    //from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionWrapper is of an incompatible version.");
                }

                final CombinedKey<KO, K> combinedKey = new CombinedKey<KO, K>(key, value.getPrimaryKey());

                //If the subscriptionWrapper hash indicates a null, must delete from statestore.
                //This store is used by the prefix scanner in KTableKTablePrefixScanProcessorSupplier
                if (value.getHash() == null) {
                    store.delete(combinedKey);
                } else {
                    store.put(combinedKey, ValueAndTimestamp.make(value, context().timestamp()));
                }

                final ValueAndTimestamp<VO> foreignValueAndTime = foreignValues.get(key);

                //Do nothing with DELETE_KEY_NO_PROPAGATE, so it's not checked in the instruction list below.
                if (value.getInstruction() == DELETE_KEY_AND_PROPAGATE) {
                    final SubscriptionResponseWrapper<VO> newValue = new SubscriptionResponseWrapper<>(value.getHash(), null);
                    context().forward(combinedKey.getPrimaryKey(), newValue, To.all().withTimestamp(context().timestamp()));
                } else if (value.getInstruction() == PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE) {
                    VO valueToSend = null;
                    //This one needs to go through regardless of LEFT or INNER join, since the extracted FK was
                    //changed and there is no match for it. We must propagate the (key, null) to ensure that the
                    //downstream consumers are alerted to this fact.
                    if (foreignValueAndTime != null) {
                        //Get the value if it's available, as per instruction.
                        valueToSend = foreignValueAndTime.value();
                    }
                    final SubscriptionResponseWrapper<VO> newValue = new SubscriptionResponseWrapper<>(value.getHash(), valueToSend);
                    context().forward(combinedKey.getPrimaryKey(), newValue, To.all().withTimestamp(context().timestamp()));
                } else if (value.getInstruction() == PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE && foreignValueAndTime != null) {
                    final SubscriptionResponseWrapper<VO> newValue = new SubscriptionResponseWrapper<>(value.getHash(), foreignValueAndTime.value());
                    context().forward(combinedKey.getPrimaryKey(), newValue, To.all().withTimestamp(context().timestamp()));
                }
            }
        };
    }

    public KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> valueGetterSupplier() {
        return new KTablePrefixValueGetterSupplier<>(stateStoreName);
    }
}