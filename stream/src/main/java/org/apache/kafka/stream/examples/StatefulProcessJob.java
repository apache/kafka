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

package org.apache.kafka.stream.examples;

import org.apache.kafka.stream.KStreamProcess;
import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.internals.KafkaSource;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.internals.KStreamConfig;
import org.apache.kafka.stream.state.Entry;
import org.apache.kafka.stream.state.InMemoryKeyValueStore;
import org.apache.kafka.stream.state.KeyValueIterator;
import org.apache.kafka.stream.state.KeyValueStore;

import java.util.Properties;

public class StatefulProcessJob {

    private static class MyProcessor extends KafkaProcessor<String, Integer, Object, Object> {
        private ProcessorContext context;
        private KeyValueStore<String, Integer> kvStore;

        public MyProcessor(String name) {
            super(name);
        }

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
            }
        }

        @Override
        public void close() {
            this.kvStore.close();
        }
    }

    private static class MyPTopology extends PTopology {

        @Override
        public void build() {
            KafkaSource<String, Integer> source = addSource(new StringDeserializer(), new IntegerDeserializer(), "topic-source");

            addProcessor(new MyProcessor("processor"), source);
        }
    }

    public static void main(String[] args) throws Exception {
        KStreamProcess streaming = new KStreamProcess(MyPTopology.class, new KStreamConfig(new Properties()));
        streaming.run();
    }
}
