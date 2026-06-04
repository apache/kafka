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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoinerWithKeys;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueTimestampHeaders.getValueOrNull;

class KStreamGlobalKTableJoinProcessor<StreamKey, StreamValue, TableKey, TableValue, VOut>
    extends ContextualProcessor<StreamKey, StreamValue, StreamKey, VOut> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamGlobalKTableJoinProcessor.class);

    private final KTableValueGetter<TableKey, TableValue> valueGetter;
    private final KeyValueMapper<? super StreamKey, ? super StreamValue, ? extends TableKey> keyMapper;
    private final ValueJoinerWithKeys<? super TableKey, ? super StreamKey, ? super StreamValue, ? super TableValue, ? extends VOut> joiner;
    private final boolean leftJoin;
    private Sensor droppedRecordsSensor;

    KStreamGlobalKTableJoinProcessor(final KTableValueGetter<TableKey, TableValue> valueGetter,
                                     final KeyValueMapper<? super StreamKey, ? super StreamValue, ? extends TableKey> keyMapper,
                                     final ValueJoinerWithKeys<? super TableKey, ? super StreamKey, ? super StreamValue, ? super TableValue, ? extends VOut> joiner,
                                     final boolean leftJoin) {
        this.valueGetter = valueGetter;
        this.keyMapper = keyMapper;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public void init(final ProcessorContext<StreamKey, VOut> context) {
        super.init(context);
        final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
        droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
        valueGetter.init(context);
    }

    @Override
    public void process(final Record<StreamKey, StreamValue> record) {
        final TableKey mappedKey = keyMapper.apply(record.key(), record.value());
        if (shouldDrop(record, mappedKey)) {
            return;
        }
        final TableValue tableValue = lookup(record, mappedKey);
        if (leftJoin || tableValue != null) {
            context().forward(record.withValue(joiner.apply(mappedKey, record.key(), record.value(), tableValue)));
        }
    }

    private TableValue lookup(final Record<StreamKey, StreamValue> record, final TableKey mappedKey) {
        if (mappedKey == null) {
            return null;
        }
        final ValueTimestampHeaders<TableValue> valueTimestampHeaders = valueGetter.isVersioned()
            ? valueGetter.get(mappedKey, record.timestamp())
            : valueGetter.get(mappedKey);
        return getValueOrNull(valueTimestampHeaders);
    }

    private boolean shouldDrop(final Record<StreamKey, StreamValue> record, final TableKey mappedKey) {
        // mirror KStreamKTableJoinProcessor#maybeDropRecord: left-join with null mappedKey but non-null
        // value is allowed (produces a null-table-side join result). Drop otherwise.
        if (leftJoin && mappedKey == null && record.value() != null) {
            return false;
        }
        if (mappedKey == null || record.value() == null) {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn(
                    "Skipping record due to null join key or value. "
                        + "topic=[{}] partition=[{}] offset=[{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                LOG.warn("Skipping record due to null join key or value. Topic, partition, and offset not known.");
            }
            droppedRecordsSensor.record();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        valueGetter.close();
    }
}
