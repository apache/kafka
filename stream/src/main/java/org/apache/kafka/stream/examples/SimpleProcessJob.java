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

import org.apache.kafka.stream.processor.KafkaProcessor;
import org.apache.kafka.stream.processor.PTopologyBuilder;
import org.apache.kafka.stream.processor.ProcessorContext;
import org.apache.kafka.stream.processor.ProcessorProperties;
import org.apache.kafka.stream.processor.KafkaSource;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.KStreamProcess;

import java.util.Properties;

public class SimpleProcessJob {

    private static class MyProcessor extends KafkaProcessor<String, Integer, Object, Object> {
        private ProcessorContext context;

        public MyProcessor(String name) {
            super(name);
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(String key, Integer value) {
            System.out.println("[" + key + ", " + value + "]");

            context.commit();

            context.send("topic-dest", key, value);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    public static void main(String[] args) throws Exception {
        ProcessorProperties properties = new ProcessorProperties(new Properties());
        PTopologyBuilder builder = new PTopologyBuilder();

        builder.addSource("SOURCE", new StringDeserializer(), new IntegerDeserializer(), "topic-source");
        builder.addProcessor("PROCESS", MyProcessor.class, null, "SOURCE");

        KStreamProcess streaming = new KStreamProcess(builder, properties);
        streaming.run();
    }
}
