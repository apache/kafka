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
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Transformer;

class KStreamTransform<K, V, K1, V1> extends KStreamImpl<K, V> implements Transformer.Forwarder<K, V> {

    private final Transformer<K, V, K1, V1> transformer;

    KStreamTransform(Transformer<K, V, K1, V1> transformer, KStreamTopology topology) {
        super(topology);
        this.transformer = transformer;
    }

    @Override
    public void bind(KStreamContext context, KStreamMetadata metadata) {
        transformer.init(context);
        transformer.forwarder(this);

        super.bind(context, KStreamMetadata.unjoinable());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void receive(Object key, Object value, long timestamp) {
        synchronized (this) {
            transformer.process((K1) key, (V1) value);
        }
    }

    @Override
    public void send(K key, V value, long timestamp) {
        forward(key, value, timestamp);
    }
}
