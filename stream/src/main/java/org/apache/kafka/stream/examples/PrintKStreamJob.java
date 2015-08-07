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

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;

import java.util.Properties;

public class PrintKStreamJob {

    private static class MyProcessor extends KafkaProcessor<String, Integer, Object, Object> {
        ProcessorContext context;

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

<<<<<<< HEAD
    @SuppressWarnings("unchecked")
    @Override
    public void topology() { from("topic").process(new MyProcessor()); }

<<<<<<< HEAD
  public static void main(String[] args) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fix examples
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(PrintKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
<<<<<<< HEAD
=======
    KafkaStreaming kstream = new KafkaStreaming(new PrintKStreamJob(), new StreamingConfig(new Properties()));
=======
    KafkaStreaming kstream = new KafkaStreaming(new PrintKStreamJob(args), new StreamingConfig(new Properties()));
>>>>>>> fix examples
    kstream.run();
>>>>>>> wip
=======
>>>>>>> fix examples
  }
=======
=======
>>>>>>> wip
    public static void main(String[] args) {
        PTopology topology = new PTopology();
        topology.addProcessor(new MyProcessor("processor"), new StringDeserializer(), new IntegerDeserializer(), "topic-source");
        topology.build();

        KafkaStreaming streaming = new KafkaStreaming(
            new PrintKStreamJob(),
            new StreamingConfig(new Properties())
        );
        streaming.run();
    }
>>>>>>> compile and test passed
}
