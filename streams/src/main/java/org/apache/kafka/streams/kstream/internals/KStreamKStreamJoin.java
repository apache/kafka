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
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;


class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {

    private final String otherWindowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;

    private final ValueJoiner<V1, V2, R> joiner;
    private final boolean outer;

    KStreamKStreamJoin(String otherWindowName, long joinBeforeMs, long joinAfterMs, ValueJoiner<V1, V2, R> joiner, boolean outer) {
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joiner = joiner;
        this.outer = outer;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor extends AbstractProcessor<K, V1> {

        private WindowStore<K, V2> otherWindow;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            otherWindow = (WindowStore<K, V2>) context.getStateStore(otherWindowName);
        }


        @Override
        public void process(K key, V1 value) {
            if (key == null)
                return;

            boolean needOuterJoin = KStreamKStreamJoin.this.outer;

            long timeFrom = Math.max(0L, context().timestamp() - joinBeforeMs);
            long timeTo = Math.max(0L, context().timestamp() + joinAfterMs);

            try (WindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    context().forward(key, joiner.apply(value, iter.next().value));
                }

                if (needOuterJoin)
                    context().forward(key, joiner.apply(value, null));
            }
        }
    }
}
