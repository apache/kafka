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

package org.apache.kafka.streaming.examples;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streaming.KafkaStreaming;
import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.ProcessorDef;
import org.apache.kafka.streaming.processor.TopologyBuilder;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.StreamingConfig;
import org.apache.kafka.streaming.state.Entry;
import org.apache.kafka.streaming.state.InMemoryKeyValueStore;
import org.apache.kafka.streaming.state.KeyValueIterator;
import org.apache.kafka.streaming.state.KeyValueStore;

import java.util.Properties;

public class ProcessorJob {

    private static class MyProcessorDef implements ProcessorDef {

        @Override
        public Processor<String, Integer> instance() {
            return new Processor<String, Integer>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(this, 1000);
                    this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
                }

                @Override
                public void process(String key, Integer value) {
                    Integer oldValue = this.kvStore.get(key);
                    if (oldValue == null) {
                        this.kvStore.put(key, value);
                    } else {
                        int newValue = oldValue + value;
                        this.kvStore.put(key, newValue);
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
        StreamingConfig config = new StreamingConfig(new Properties());
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", new StringDeserializer(), new IntegerDeserializer(), "topic-source");

        builder.addProcessor("PROCESS", new MyProcessorDef(), null, "SOURCE");

        builder.addSink("SINK", "topic-sink", new StringSerializer(), new IntegerSerializer(), "PROCESS");

        KafkaStreaming streaming = new KafkaStreaming(builder, config);
        streaming.run();
    }
}
