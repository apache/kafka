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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.streaming.kstream.ValueJoiner;
import org.apache.kafka.streaming.kstream.Window;
import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.ProcessorDef;

import java.util.Iterator;

class KStreamJoin<K, V, V1, V2> implements ProcessorDef {

    private static abstract class Finder<K, T> {
        abstract Iterator<T> find(K key, long timestamp);
    }

    private final String windowName1;
    private final String windowName2;
    private final ValueJoiner<V1, V2, V> joiner;

    private Processor processorForOtherStream = null;
    public final ProcessorDef processorDefForOtherStream = new ProcessorDef() {
        @Override
        public Processor instance() {
            return processorForOtherStream;
        }
    };

    KStreamJoin(String windowName1, String windowName2, ValueJoiner<V1, V2, V> joiner) {
        this.windowName1 = windowName1;
        this.windowName2 = windowName2;
        this.joiner = joiner;
    }

    @Override
    public Processor instance() {
        // create a processor instance for the other stream
        processorForOtherStream = new KStreamJoinProcessor<K, V2, V1>(windowName1) {
            @Override
            protected void doJoin(K key, V2 value2, V1 value1) {
                context.forward(key, joiner.apply(value1, value2));
            }
        };

        // create a processor instance for the primary stream
        return new KStreamJoinProcessor<K, V1, V2>(windowName2) {
            @Override
            protected void doJoin(K key, V1 value1, V2 value2) {
                context.forward(key, joiner.apply(value1, value2));
            }
        };
    }

    private abstract class KStreamJoinProcessor<K, T1, T2> extends KStreamProcessor<K, T1> {

        private final String windowName;
        protected Finder<K, T2> finder;

        public KStreamJoinProcessor(String windowName) {
            this.windowName = windowName;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            // check if these two streams are joinable
            if (!context.joinable())
                throw new IllegalStateException("Streams are not joinable.");

            final Window<K, T2> window = (Window<K, T2>) context.getStateStore(windowName);

            this.finder = new Finder<K, T2>() {
                Iterator<T2> find(K key, long timestamp) {
                    return window.find(key, timestamp);
                }
            };
        }

        @Override
        public void process(K key, T1 value) {
            long timestamp = context.timestamp();
            Iterator<T2> iter = finder.find(key, timestamp);
            if (iter != null) {
                while (iter.hasNext()) {
                    doJoin(key, value, iter.next());
                }
            }
        }

        abstract protected void doJoin(K key, T1 value1, T2 value2);
    }

}
