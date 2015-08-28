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

    private final String windowName;
    private final ValueJoiner<V1, V2, V> joiner;

    KStreamJoin(String windowName, ValueJoiner<V1, V2, V> joiner) {
        this.windowName = windowName;
        this.joiner = joiner;
    }

    @Override
    public Processor instance() {
        return new KStreamJoinProcessor(windowName);
    }

    private class KStreamJoinProcessor extends KStreamProcessor<K, V1> {

        private final String windowName;
        protected Finder<K, V2> finder;

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

            final Window<K, V2> window = (Window<K, V2>) context.getStateStore(windowName);

            this.finder = new Finder<K, V2>() {
                Iterator<V2> find(K key, long timestamp) {
                    return window.find(key, timestamp);
                }
            };
        }

        @Override
        public void process(K key, V1 value) {
            long timestamp = context.timestamp();
            Iterator<V2> iter = finder.find(key, timestamp);
            if (iter != null) {
                while (iter.hasNext()) {
                    context.forward(key, joiner.apply(value, iter.next()));
                }
            }
        }
    }

    public static <T2, T1, R> ValueJoiner<T2, T1, R> reserveJoiner(final ValueJoiner<T1, T2, R> joiner) {
        return new ValueJoiner<T2, T1, R>() {
            @Override
            public R apply(T2 value2, T1 value1) {
                return joiner.apply(value1, value2);
            }
        };
    }

}
