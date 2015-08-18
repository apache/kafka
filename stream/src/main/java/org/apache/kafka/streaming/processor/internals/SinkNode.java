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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.RecordCollector;

import java.util.ArrayList;
import java.util.List;

public class SinkNode<K, V> extends ProcessorNode<K, V, K, V> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final List<String> topics;

    private ProcessorContext context;

    public SinkNode(String name, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        super(name);

        this.topics = new ArrayList<>();
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
    }

    public void addTopic(String topic) {
        this.topics.add(topic);
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(K key, V value) {
        // send to all the registered topics
        RecordCollector collector = context.recordCollector();
        for (String topic : topics) {
            collector.send(new ProducerRecord<>(topic, key, value), keySerializer, valSerializer);
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
