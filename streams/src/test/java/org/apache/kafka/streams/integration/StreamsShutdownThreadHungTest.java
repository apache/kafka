/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Category({IntegrationTest.class})
public class StreamsShutdownThreadHungTest {

    private final Properties streamsConfig = new Properties();
    private final Properties producerProperties = new Properties();
    private boolean isShutdown = false;
    private boolean didProcessARecord = false;
    private final Object someCollaborationObject = new Object();

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Before
    public void setUp() throws InterruptedException {
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        CLUSTER.createTopics("input");
    }

    @After
    public void tearDown() throws InterruptedException {
        CLUSTER.deleteAllTopicsAndWait(30000);
    }

    @Test
    public void shouldShutDownEvenIfThreadIsStuck() throws InterruptedException, ExecutionException {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("input").process(new TestProcessor());

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

        IntegrationTestUtils.produceKeyValuesSynchronously("input",
            Collections.singletonList(KeyValue.pair("key", "value")),
            producerProperties,
            Time.SYSTEM);

        streams.start();
        TestUtils.waitForCondition(() -> didProcessARecord, "Didn't process record in time");
        streams.close(Duration.ofSeconds(5));
        TestUtils.waitForCondition(() -> isShutdown, "Streams didn't exit in time");

    }

    private final class TestProcessor extends AbstractProcessor<String, String> implements ProcessorSupplier<String, String> {
        @Override
        public Processor<String, String> get() {
            return this;
        }

        @Override
        public void process(final String key, final String value) {
            didProcessARecord = true;
        }

        @Override
        public void close() {
            try {
                Thread.sleep(300000);
            } catch (final InterruptedException e) {
                isShutdown = true;
                Thread.currentThread().interrupt();
            }
        }
    }
}
