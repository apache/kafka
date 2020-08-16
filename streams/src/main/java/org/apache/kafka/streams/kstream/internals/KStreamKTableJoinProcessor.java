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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KStreamKTableJoinProcessor<K1, K2, V1, V2, R> extends AbstractProcessor<K1, V1> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKTableJoin.class);

    private final KTableValueGetter<K2, V2> valueGetter;
    private final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyMapper;
    private final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;
    private final boolean leftJoin;
    private StreamsMetricsImpl metrics;
    private Sensor droppedRecordsSensor;

    KStreamKTableJoinProcessor(final KTableValueGetter<K2, V2> valueGetter,
                               final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyMapper,
                               final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                               final boolean leftJoin) {
        this.valueGetter = valueGetter;
        this.keyMapper = keyMapper;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public void init(final ProcessorContext context) {
        super.init(context);
        metrics = (StreamsMetricsImpl) context.metrics();
        droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
        valueGetter.init(context);
    }

    @Override
    public void process(final K1 key, final V1 value) {
        // we do join iff the joining keys are equal, thus, if the mappedKey is null we cannot join
        // and just ignore the record.
        //
        // we also ignore the record if value is null, because in a key-value data model a null-value indicates
        // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
        // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
        // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
        final Optional<K2> maybeMappedKey = maybeExtractMappedKey(key, value);
        if (!maybeMappedKey.isPresent()) {
            LOG.warn(
                "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                key, value, context().topic(), context().partition(), context().offset()
            );
            droppedRecordsSensor.record();
        } else {
            final K2 mappedKey = maybeMappedKey.get();
            final V2 value2 = getValueOrNull(valueGetter.get(mappedKey));
            if (leftJoin || value2 != null) {
                context().forward(key, joiner.apply(value, value2));
            }
        }
    }

    private Optional<K2> maybeExtractMappedKey(final K1 key, final V1 value) {
        if (value == null) {
            return Optional.empty();
        }

        // we allow the case where the key is null but mappedKey is not null and thus
        // we need to guard against nullPointerExceptions. This may happen for GlobalKTables.
        // For KTables, the keyMapper simply returns the key, so this will never happen
        Optional<K2> maybeMappedKey;
        try {
            maybeMappedKey = Optional.ofNullable(keyMapper.apply(key, value));
        } catch (final NullPointerException e) {
            maybeMappedKey = Optional.empty();
        }
        return maybeMappedKey;
    }

    @Override
    public void close() {
        valueGetter.close();
    }
}
