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

package org.apache.kafka.stream.internals;

import org.apache.kafka.stream.topology.Processor;
import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.topology.internals.KStreamMetadata;

public class ProcessorNode<K, V> implements Receiver {

    private final Processor<K, V> processor;
    private KStreamContext context;

    public ProcessorNode(Processor<K, V> processor) {
        this.processor = processor;
    }

    @Override
    public void bind(KStreamContext context, KStreamMetadata metadata) {
        if (this.context != null) throw new IllegalStateException("kstream topology is already bound");

        this.context = context;
        processor.init(context);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void receive(Object key, Object value, long timestamp) {
        processor.process((K) key, (V) value);
    }

    @Override
    public void close() {
        processor.close();
    }

}
