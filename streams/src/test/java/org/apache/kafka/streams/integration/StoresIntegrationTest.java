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
package org.apache.kafka.streams.integration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;


public class StoresIntegrationTest {
    private static EmbeddedSingleNodeKafkaCluster cluster = null;
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static int foundStores = 0;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        cluster = new EmbeddedSingleNodeKafkaCluster();
        cluster.createTopic(INPUT_TOPIC);
        cluster.createTopic(OUTPUT_TOPIC);
    }

    @AfterClass
    public static void stopKafkaCluster() throws IOException {
        if (cluster != null) {
            cluster.stop();
        }
    }

    private static class MyProcessorSupplier implements ProcessorSupplier<String, String> {
        private final int numStores;
        MyProcessorSupplier(int numStores) {
            this.numStores = numStores;
        }
        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000);
                    for (int i = 0; i < numStores; i++) {
                        String storeName = "Counts" + i;
                        if (context.getStateStore(storeName) != null) {
                            foundStores++;
                        }
                    }
                }

                @Override
                public void process(String dummy, String line) {
                    // do nothing
                }

                @Override
                public void punctuate(long timestamp) {
                    // do noting
                }

                @Override
                public void close() {
                    // do nothing
                }
            };
        }
    }


    /**
     * This tests the problem described in https://issues.apache.org/jira/browse/KAFKA-3559
     */
    @Test
    public void testCreateLargeNumberOfStores() throws Exception {
        int numStores = 10;
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stores-integration-test-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zKConnectString());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", "streams-input");
        builder.addProcessor("Process", new MyProcessorSupplier(numStores), "Source");

        for (int i = 0; i < numStores; i++) {
            String storeName = "Counts" + i;
            builder.addStateStore(Stores.create(storeName).withStringKeys().withIntegerValues().inMemory().build(), "Process");
        }
        builder.addSink("Sink", "streams-output", "Process");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
        assertEquals(foundStores, numStores);
    }
}
