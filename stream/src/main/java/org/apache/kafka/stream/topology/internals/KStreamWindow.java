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

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.topology.KStreamWindowed;
import org.apache.kafka.stream.topology.ValueJoiner;
import org.apache.kafka.stream.topology.Window;

public class KStreamWindow<K, V> extends KafkaProcessor<K, V, K, V> {

    private static final class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V> implements KStreamWindowed<K, V> {

        public KStreamWindowedImpl(PTopology topology, KafkaProcessor<?, ?, K, V> processor) {
            super(topology, processor);
        }

        @Override
        public <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
            return join(other, false, processor);
        }

        @Override
        public <V1, V2> KStream<K, V2> joinPrior(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
            return join(other, true, processor);
        }

        private <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, boolean prior, ValueJoiner<V2, V, V1> processor) {

            KStreamWindowedImpl<K, V1> otherImpl = (KStreamWindowedImpl<K, V1>) other;

            KStreamJoin<K, V2, V, V1> stream = new KStreamJoin<>(this, otherImpl, prior, processor, topology);
            otherImpl.registerReceiver(stream.receiverForOtherStream);

            return chain(stream);
        }
    }

    private static final String WINDOW_NAME = "KAFKA-WINDOW";

    private final Window<K, V> window;
    private ProcessorContext context;

    KStreamWindow(Window<K, V> window) {
        super(WINDOW_NAME);
        this.window = window;
    }

    public Window<K, V> window() {
        return window;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(K key, V value) {
        synchronized (this) {
            window.put(key, value, context.timestamp());
            forward(key, value);
        }
    }

    @Override
    public void close() {
        window.close();
    }
}
