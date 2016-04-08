/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamKTableLeftJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {

    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier;
    private final ValueJoiner<V1, V2, R> joiner;

    KStreamKTableLeftJoin(KTableImpl<K, ?, V2> table, ValueJoiner<V1, V2, R> joiner) {
        this.valueGetterSupplier = table.valueGetterSupplier();
        this.joiner = joiner;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKTableLeftJoinProcessor(valueGetterSupplier.get());
    }

    private class KStreamKTableLeftJoinProcessor extends AbstractProcessor<K, V1> {

        private final KTableValueGetter<K, V2> valueGetter;

        public KStreamKTableLeftJoinProcessor(KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        @Override
        public void process(K key, V1 value) {
            // if the key is null, we do not need proceed joining
            // the record with the table
            if (key != null) {
                context().forward(key, joiner.apply(value, valueGetter.get(key)));
            }
        }
    }

}
