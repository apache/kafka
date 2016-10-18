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

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamKTableJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {

    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier;
    private final ValueJoiner<V1, V2, R> joiner;
    private final boolean leftJoin;

    KStreamKTableJoin(final KTableValueGetterSupplier<K, V2> valueGetterSupplier, final ValueJoiner<V1, V2, R> joiner, final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKTableJoinProcessor(valueGetterSupplier.get(), leftJoin);
    }

    private class KStreamKTableJoinProcessor extends AbstractProcessor<K, V1> {

        private final KTableValueGetter<K, V2> valueGetter;
        private final boolean leftJoin;

        KStreamKTableJoinProcessor(final KTableValueGetter<K, V2> valueGetter, final boolean leftJoin) {
            this.valueGetter = valueGetter;
            this.leftJoin = leftJoin;
        }

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        @Override
        public void process(final K key, final V1 value) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key != null && value != null) {
                final V2 value2 = valueGetter.get(key);
                if (leftJoin || value2 != null) {
                    context().forward(key, joiner.apply(value, value2));
                }
            }
        }
    }
}
