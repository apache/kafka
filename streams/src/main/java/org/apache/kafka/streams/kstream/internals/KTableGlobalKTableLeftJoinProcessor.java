/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableGlobalKTableLeftJoinProcessor<K1, K2, V1, V2, R> extends AbstractProcessor<K1, Change<V1>> {

    private final KTableValueGetter<K2, V2> valueGetter;
    private final ValueJoiner<V1, V2, R> joiner;
    private final KeyValueMapper<K1, V1, K2> keyValueMapper;
    private final boolean sendOldValues;

    KTableGlobalKTableLeftJoinProcessor(final KTableValueGetter<K2, V2> valueGetter,
                                        final ValueJoiner<V1, V2, R> joiner,
                                        final KeyValueMapper<K1, V1, K2> keyValueMapper,
                                        final boolean sendOldValues) {
        this.valueGetter = valueGetter;
        this.joiner = joiner;
        this.keyValueMapper = keyValueMapper;
        this.sendOldValues = sendOldValues;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        valueGetter.init(context);
    }

    /**
     * @throws StreamsException if key is null
     */
    @Override
    public void process(final K1 key, final Change<V1> change) {
        // the keys should never be null
        if (key == null) {
            throw new StreamsException("Record key for KTable left-join operator should not be null.");
        }

        final V2 newOtherValue = valueGetter.get(keyValueMapper.apply(key, change.newValue));
        final V2 oldOtherValue = valueGetter.get(keyValueMapper.apply(key, change.oldValue));

        if (newOtherValue != null
                || oldOtherValue != null
                || change.newValue != null
                || change.oldValue != null) {

            final R newValue = applyJoin(change.newValue, newOtherValue, true);
            final R oldValue = applyJoin(change.oldValue, oldOtherValue, sendOldValues);
            context().forward(key, new Change<>(newValue, oldValue));
        }

    }

    private R applyJoin(final V1 value, final V2 otherValue, final boolean shouldJoin) {
        if (shouldJoin && value != null) {
            return joiner.apply(value, otherValue);
        }
        return null;
    }

}
