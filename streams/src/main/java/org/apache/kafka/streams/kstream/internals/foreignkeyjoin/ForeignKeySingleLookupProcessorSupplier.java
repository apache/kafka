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
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForeignKeySingleLookupProcessorSupplier<K, KO, VO>
        implements ProcessorSupplier<CombinedKey<KO, K>, SubscriptionWrapper> {
    private static final Logger LOG = LoggerFactory.getLogger(ForeignKeySingleLookupProcessorSupplier.class);

    private final String stateStoreName;
    private final KTableValueGetterSupplier<KO, VO> foreignValueGetterSupplier;

    public ForeignKeySingleLookupProcessorSupplier(final String stateStoreName,
                                                   final KTableValueGetterSupplier<KO, VO> foreignValueGetter) {
        this.stateStoreName = stateStoreName;
        this.foreignValueGetterSupplier = foreignValueGetter;
    }

    @Override
    public Processor<CombinedKey<KO, K>, SubscriptionWrapper> get() {

        return new AbstractProcessor<CombinedKey<KO, K>, SubscriptionWrapper>() {

            private KeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper> store;
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
                store = (KeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(final CombinedKey<KO, K> key, final SubscriptionWrapper value) {
                KO foreignKey = key.getForeignKey();
                if (foreignKey == null) {
                    LOG.warn(
                            "Skipping record due to null foreign key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                            value, context().topic(), context().partition(), context().offset()
                    );
                    skippedRecordsSensor.record();
                    return;
                }

                //If the subscriptionWrapper hash indicates a null, must delete from statestore.
                //This store is used by the prefix scanner in KTableKTablePrefixScanProcessorSupplier
                if (value.getHash() == null) {
                    store.delete(key);
                } else {
                    store.put(key, value);
                }

                VO foreignValue = foreignValues.get(foreignKey).value();
                //Propagate valid requests for the foreign event data as well as deletions.
                if ((value.getHash() != null && foreignValue != null) ||
                    (value.getHash() == null && value.isPropagate())) {
                    final SubscriptionResponseWrapper<VO> newValue = new SubscriptionResponseWrapper<>(value.getHash(), foreignValue);
                    context().forward(key.getPrimaryKey(), newValue);
                }
            }
        };
    }

    public KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> valueGetterSupplier() {
        return new KTablePrefixValueGetterSupplier<>(stateStoreName);
    }
}