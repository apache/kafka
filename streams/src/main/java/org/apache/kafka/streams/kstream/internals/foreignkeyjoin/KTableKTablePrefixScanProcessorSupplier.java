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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetter;
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KTableKTablePrefixScanProcessorSupplier<K, KO, VO> implements ProcessorSupplier<KO, Change<VO>> {
    private final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> primary;
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTablePrefixScanProcessorSupplier.class);

    public KTableKTablePrefixScanProcessorSupplier(final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> primary) {
        this.primary = primary;
    }

    @Override
    public Processor<KO, Change<VO>> get() {
        return new KTableKTableJoinProcessor(primary);
    }


    private class KTableKTableJoinProcessor extends AbstractProcessor<KO, Change<VO>> {

        private final KTablePrefixValueGetter<CombinedKey<KO, K>, SubscriptionWrapper> prefixValueGetter;

        public KTableKTableJoinProcessor(final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> valueGetter) {
            prefixValueGetter = valueGetter.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            prefixValueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final KO key, final Change<VO> value) {
            if (key == null)
                throw new StreamsException("Record key for foreignKeyJoin operator should not be null.");

            //Don't do any work if the value hasn't changed.
            //It can be expensive to update all the records returned from prefixScan.
            if (value.oldValue == value.newValue)
                return;

            final CombinedKey<KO, K> prefixKey = new CombinedKey<>(key);
            //Perform the prefixScan and propagate the results
            final KeyValueIterator<CombinedKey<KO, K>, ValueAndTimestamp<SubscriptionWrapper>> prefixScanResults = prefixValueGetter.prefixScan(prefixKey);
            while (prefixScanResults.hasNext()) {
                final KeyValue<CombinedKey<KO, K>, ValueAndTimestamp<SubscriptionWrapper>> scanResult = prefixScanResults.next();
                context().forward(scanResult.key.getPrimaryKey(), new SubscriptionResponseWrapper<>(scanResult.value.value().getHash(), value.newValue));
            }
        }
    }
}
