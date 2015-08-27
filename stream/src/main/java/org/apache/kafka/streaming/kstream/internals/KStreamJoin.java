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

import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.kstream.ValueJoiner;
import org.apache.kafka.streaming.kstream.Window.WindowInstance;
import org.apache.kafka.streaming.processor.ProcessorFactory;

import java.util.Iterator;

class KStreamJoin<K, V, V1, V2> implements ProcessorFactory {

    private static abstract class Finder<K, T> {
        abstract Iterator<T> find(K key, long timestamp);
    }

    private final String windowName1;
    private final String windowName2;
    private final ValueJoiner<V1, V2, V> joiner;
    private final boolean prior;

    private Processor processorForOtherStream = null;
    public final ProcessorFactory processorFactoryForOtherStream = new ProcessorFactory() {
        @Override
        public Processor build() {
            return processorForOtherStream;
        }
    };

    KStreamJoin(String windowName1, String windowName2, boolean prior, ValueJoiner<V1, V2, V> joiner) {
        this.windowName1 = windowName1;
        this.windowName2 = windowName2;
        this.joiner = joiner;
        this.prior = prior;
    }

    @Override
    public Processor build() {
        return new KStreamJoinProcessor();
    }

    private class KStreamJoinProcessor extends KStreamProcessor<K, V1> {

        private Finder<K, V1> finder1;
        private Finder<K, V2> finder2;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            // check if these two streams are joinable
            if (!context.joinable())
                throw new IllegalStateException("Streams are not joinable.");

            final WindowInstance<K, V1> window1 = (WindowInstance<K, V1>) context.getStateStore(windowName1);
            final WindowInstance<K, V2> window2 = (WindowInstance<K, V2>) context.getStateStore(windowName2);

            if (prior) {
                this.finder1 = new Finder<K, V1>() {
                    Iterator<V1> find(K key, long timestamp) {
                        return window1.findAfter(key, timestamp);
                    }
                };
                this.finder2 = new Finder<K, V2>() {
                    Iterator<V2> find(K key, long timestamp) {
                        return window2.findBefore(key, timestamp);
                    }
                };
            } else {
                this.finder1 = new Finder<K, V1>() {
                    Iterator<V1> find(K key, long timestamp) {
                        return window1.find(key, timestamp);
                    }
                };
                this.finder2 = new Finder<K, V2>() {
                    Iterator<V2> find(K key, long timestamp) {
                        return window2.find(key, timestamp);
                    }
                };
            }

            processorForOtherStream = new KStreamProcessor<K, V2>() {
                @Override
                public void process(K key, V2 value) {
                    long timestamp = context.timestamp();
                    Iterator<V1> iter = finder1.find(key, timestamp);
                    if (iter != null) {
                        while (iter.hasNext()) {
                            doJoin(key, iter.next(), value);
                        }
                    }
                }
            };
        }

        @Override
        public void process(K key, V1 value) {
            long timestamp = context.timestamp();
            Iterator<V2> iter = finder2.find(key, timestamp);
            if (iter != null) {
                while (iter.hasNext()) {
                    doJoin(key, value, iter.next());
                }
            }
        }

        // TODO: use the "outer-stream" topic as the resulted join stream topic
        private void doJoin(K key, V1 value1, V2 value2) {
            context.forward(key, joiner.apply(value1, value2));
        }
    }

}
