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

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KStreamKTableJoinProcessor<K1, K2, V1, V2, R> extends AbstractProcessor<K1, V1> {

    private final KTableValueGetter<K2, V2> valueGetter;
    private final KeyValueMapper<? super K1, ? super V1, ? extends K2> keyMapper;
    private final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;
    private final boolean leftJoin;

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
        valueGetter.init(context);
    }

    @Override
    public void process(final K1 key, final V1 value) {
        // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
        //
        // we also ignore the record if value is null, because in a key-value data model a null-value indicates
        // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
        // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
        // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
        if (key != null && value != null) {
            final V2 value2 = valueGetter.get(keyMapper.apply(key, value));
            if (leftJoin || value2 != null) {
                context().forward(key, joiner.apply(value, value2));
            }
        }
    }
}
