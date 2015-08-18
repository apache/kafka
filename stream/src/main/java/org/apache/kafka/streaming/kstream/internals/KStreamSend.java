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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.processor.ProcessorMetadata;
import org.apache.kafka.streaming.processor.ProcessorContext;

class KStreamSend<K, V> extends KafkaProcessor<K, V, Object, Object> {

    private ProcessorContext context;

    private TopicSer topicSerializers;

    public static final class TopicSer {
        public String topic;
        public Serializer<Object> keySerializer;
        public Serializer<Object> valSerializer;

        public TopicSer(String topic, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
            this.topic = topic;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
        }
    }

    @SuppressWarnings("unchecked")
    public KStreamSend(String name, ProcessorMetadata config) {
        super(name, config);

        if (this.metadata() == null)
            throw new IllegalStateException("ProcessorMetadata should be specified.");

        this.topicSerializers = (TopicSer) config.value();
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(K key, V value) {
        this.context.send(topicSerializers.topic, key, value, topicSerializers.keySerializer, topicSerializers.valSerializer);
    }
}