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
import org.apache.kafka.streaming.kstream.Window;

import java.util.Iterator;

class KStreamJoin<K, V, V1, V2> extends Processor<K, V1, K, V> {

    private static final String JOIN_NAME = "KAFKA-JOIN";
    private static final String JOIN_OTHER_NAME = "KAFKA-JOIN-OTHER";

    private static abstract class Finder<K, T> {
        abstract Iterator<T> find(K key, long timestamp);
    }

    private final KStreamWindow<K, V1> stream1;
    private final KStreamWindow<K, V2> stream2;
    private final Finder<K, V1> finder1;
    private final Finder<K, V2> finder2;
    private final ValueJoiner<V1, V2, V> joiner;
    final Processor<K, V2, K, V> processorForOtherStream;

    private ProcessorContext context;

    KStreamJoin(KStreamWindow<K, V1> stream1, KStreamWindow<K, V2> stream2, boolean prior, ValueJoiner<V1, V2, V> joiner) {
        super(JOIN_NAME);

        this.stream1 = stream1;
        this.stream2 = stream2;
        final Window<K, V1> window1 = stream1.window();
        final Window<K, V2> window2 = stream2.window();

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

        this.joiner = joiner;

        this.processorForOtherStream = processorForOther();
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        // check if these two streams are joinable
        if (!stream1.context().joinable(stream2.context()))
            throw new IllegalStateException("Stream " + stream1.name() + " and stream " +
                stream2.name() + " are not joinable.");
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

    private Processor<K, V2, K, V> processorForOther() {
        return new Processor<K, V2, K, V>(JOIN_OTHER_NAME) {

            @SuppressWarnings("unchecked")
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

            @Override
            public void close() {
                // down stream instances are close when the primary stream is closed
            }
        };
    }

    // TODO: use the "outer-stream" topic as the resulted join stream topic
    private void doJoin(K key, V1 value1, V2 value2) {
        forward(key, joiner.apply(value1, value2));
    }
}
