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

class KTableKTableLeftJoin<K, R, V1, V2> extends KTableKTableAbstractJoin<K, R, V1, V2> {

    KTableKTableLeftJoin(KTableImpl<K, ?, V1> table1, KTableImpl<K, ?, V2> table2, ValueJoiner<? super V1, ? super V2, ? extends R> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>> get() {
        return new KTableKTableLeftJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, R> view() {
        return new KTableKTableLeftJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableLeftJoinValueGetterSupplier extends AbstractKTableKTableJoinValueGetterSupplier<K, R, V1, V2> {

        public KTableKTableLeftJoinValueGetterSupplier(KTableValueGetterSupplier<K, V1> valueGetterSupplier1, KTableValueGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, R> get() {
            return new KTableKTableLeftJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }


    private class KTableKTableLeftJoinProcessor extends AbstractProcessor<K, Change<V1>> {

        private final KTableValueGetter<K, V2> valueGetter;

        public KTableKTableLeftJoinProcessor(KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        @Override
        public void process(final K key, final Change<V1> change) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (key == null) {
                return;
            }

            R newValue = null;
            R oldValue = null;

            final V2 value2 = valueGetter.get(key);
            if (value2 == null && change.newValue == null && change.oldValue == null) {
                return;
            }

            if (change.newValue != null) {
                newValue = joiner.apply(change.newValue, value2);
            }

            if (sendOldValues && change.oldValue != null)
                oldValue = joiner.apply(change.oldValue, value2);

            context().forward(key, new Change<>(newValue, oldValue));
        }
    }

    private class KTableKTableLeftJoinValueGetter implements KTableValueGetter<K, R> {

        private final KTableValueGetter<K, V1> valueGetter1;
        private final KTableValueGetter<K, V2> valueGetter2;

        public KTableKTableLeftJoinValueGetter(KTableValueGetter<K, V1> valueGetter1, KTableValueGetter<K, V2> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public void init(ProcessorContext context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public R get(K key) {
            V1 value1 = valueGetter1.get(key);

            if (value1 != null) {
                V2 value2 = valueGetter2.get(key);
                return joiner.apply(value1, value2);
            } else {
                return null;
            }
        }

    }

}
