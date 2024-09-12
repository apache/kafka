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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ForeignTableJoinProcessorSupplier<K, KO, VO> implements
    ProcessorSupplier<KO, Change<VO>, K, SubscriptionResponseWrapper<VO>> {
    private static final Logger LOG = LoggerFactory.getLogger(ForeignTableJoinProcessorSupplier.class);
    private final String storeName;
    private final CombinedKeySchema<KO, K> keySchema;
    private boolean useVersionedSemantics = false;

    public ForeignTableJoinProcessorSupplier(
        final String storeName,
        final CombinedKeySchema<KO, K> keySchema) {

        this.storeName = storeName;
        this.keySchema = keySchema;
    }

    @Override
    public Processor<KO, Change<VO>, K, SubscriptionResponseWrapper<VO>> get() {
        return new KTableKTableJoinProcessor();
    }

    public void setUseVersionedSemantics(final boolean useVersionedSemantics) {
        this.useVersionedSemantics = useVersionedSemantics;
    }

    // VisibleForTesting
    public boolean isUseVersionedSemantics() {
        return useVersionedSemantics;
    }

    private final class KTableKTableJoinProcessor extends ContextualProcessor<KO, Change<VO>, K, SubscriptionResponseWrapper<VO>> {
        private Sensor droppedRecordsSensor;
        private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> subscriptionStore;

        @Override
        public void init(final ProcessorContext<K, SubscriptionResponseWrapper<VO>> context) {
            super.init(context);
            final InternalProcessorContext<?, ?> internalProcessorContext = (InternalProcessorContext<?, ?>) context;
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                Thread.currentThread().getName(),
                internalProcessorContext.taskId().toString(),
                internalProcessorContext.metrics()
            );
            subscriptionStore = internalProcessorContext.getStateStore(storeName);
        }

        @Override
        public void process(final Record<KO, Change<VO>> record) {
            // if the key is null, we do not need to proceed aggregating
            // the record with the table
            if (record.key() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            // drop out-of-order records from versioned tables (cf. KIP-914)
            if (useVersionedSemantics && !record.value().isLatest) {
                LOG.info("Skipping out-of-order record from versioned table while performing table-table join.");
                droppedRecordsSensor.record();
                return;
            }

            final Bytes prefixBytes = keySchema.prefixBytes(record.key());

            //Perform the prefixScan and propagate the results
            try (final KeyValueIterator<Bytes, ValueAndTimestamp<SubscriptionWrapper<K>>> prefixScanResults =
                     subscriptionStore.range(prefixBytes, Bytes.increment(prefixBytes))) {

                while (prefixScanResults.hasNext()) {
                    final KeyValue<Bytes, ValueAndTimestamp<SubscriptionWrapper<K>>> next = prefixScanResults.next();
                    // have to check the prefix because the range end is inclusive :(
                    if (prefixEquals(next.key.get(), prefixBytes.get())) {
                        final CombinedKey<KO, K> combinedKey = keySchema.fromBytes(next.key);
                        context().forward(
                            record.withKey(combinedKey.primaryKey())
                                .withValue(new SubscriptionResponseWrapper<>(
                                    next.value.value().hash(),
                                    record.value().newValue,
                                    next.value.value().primaryPartition()))
                        );
                    }
                }
            }
        }

        private boolean prefixEquals(final byte[] x, final byte[] y) {
            final int min = Math.min(x.length, y.length);
            final ByteBuffer xSlice = ByteBuffer.wrap(x, 0, min);
            final ByteBuffer ySlice = ByteBuffer.wrap(y, 0, min);
            return xSlice.equals(ySlice);
        }
    }
}
