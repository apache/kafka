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

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streaming.processor.ProcessorContext;

public class SourceNode<K, V> extends ProcessorNode<K, V> {

    public Deserializer<K> keyDeserializer;
    public Deserializer<V> valDeserializer;

    private ProcessorContext context;

    public SourceNode(String name, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer) {
        super(name);

        this.keyDeserializer = keyDeserializer;
        this.valDeserializer = valDeserializer;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(K key, V value) {
        context.forward(key, value);
    }

    @Override
    public void close() {
        // do nothing
    }

}
