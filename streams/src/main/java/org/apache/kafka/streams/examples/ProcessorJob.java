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

package org.apache.kafka.streams.examples;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorDef;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.state.Entry;
import org.apache.kafka.streams.state.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

public class ProcessorJob {

    private static class MyProcessorDef implements ProcessorDef {

        @Override
        public Processor<String, String> instance() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(this, 1000);
                    this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
                }

                @Override
                public void process(String key, String value) {
                    Integer oldValue = this.kvStore.get(key);
                    Integer newValue = Integer.parseInt(value);
                    if (oldValue == null) {
                        this.kvStore.put(key, newValue);
                    } else {
                        this.kvStore.put(key, oldValue + newValue);
                    }

                    context.commit();
                }

                @Override
                public void punctuate(long streamTime) {
                    KeyValueIterator<String, Integer> iter = this.kvStore.all();

                    while (iter.hasNext()) {
                        Entry<String, Integer> entry = iter.next();

                        System.out.println("[" + entry.key() + ", " + entry.value() + "]");

                        context.forward(entry.key(), entry.value());
                    }
                }

                @Override
                public void close() {
                    this.kvStore.close();
                }
            };
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamingConfig config = new StreamingConfig(props);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", new StringDeserializer(), new StringDeserializer(), "topic-source");

        builder.addProcessor("PROCESS", new MyProcessorDef(), "SOURCE");

        builder.addSink("SINK", "topic-sink", new StringSerializer(), new IntegerSerializer(), "PROCESS");

        KafkaStreaming streaming = new KafkaStreaming(builder, config);
        streaming.start();
    }
}
