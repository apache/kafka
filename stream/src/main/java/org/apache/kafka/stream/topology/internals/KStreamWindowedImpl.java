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

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KStreamWindowed;
import org.apache.kafka.stream.topology.ValueJoiner;
import org.apache.kafka.stream.topology.Window;

public class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V> implements KStreamWindowed<K, V> {

    final Window<K, V> window;

    KStreamWindowedImpl(Window<K, V> window, KStreamTopology initializer) {
        super(initializer);
        this.window = window;
    }

    @Override
    public void bind(KStreamContext context, KStreamMetadata metadata) {
        super.bind(context, metadata);
        window.init(context);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void receive(Object key, Object value, long timestamp) {
        synchronized (this) {
            window.put((K) key, (V) value, timestamp);
            // KStreamWindowed needs to forward the topic name since it may receive directly from KStreamSource
            forward(key, value, timestamp);
        }
    }

    @Override
    public void close() {
        window.close();
        super.close();
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
