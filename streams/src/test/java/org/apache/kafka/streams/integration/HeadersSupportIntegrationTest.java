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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class HeadersSupportIntegrationTest {
    private static final int NUM_BROKERS = 3;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, new Properties() {
        {
            put("auto.create.topics.enable", false);
        }
    });

    private static String applicationId;
    private final static String CONSUMER_GROUP_ID = "headersConsumer";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String THROUGH_TOPIC = "throughTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";


    private int testNumber = 0;

    @Before
    public void createTopics() throws InterruptedException {
        applicationId = "appId-" + ++testNumber;
        CLUSTER.deleteTopicsAndWait(
                INPUT_TOPIC,
                THROUGH_TOPIC,
                OUTPUT_TOPIC);

        CLUSTER.createTopics(INPUT_TOPIC, THROUGH_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void shouldCopyHeadersToIntermediateAndSinkTopic() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(INPUT_TOPIC)
                .through(THROUGH_TOPIC)
                .to(OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(
                builder.build(),
                StreamsTestUtils.getStreamsConfig(
                        applicationId,
                        CLUSTER.bootstrapServers(),
                        Serdes.LongSerde.class.getName(),
                        Serdes.LongSerde.class.getName(),
                        new Properties()));

        try {
            streams.start();

            final List<KeyValue<Long, Long>> inputData = prepareData(0, 10L, 0L, 1L);

            final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("hkey", "hvalue".getBytes())});

            IntegrationTestUtils.produceKeyValuesSynchronously(
                    INPUT_TOPIC,
                    inputData,
                    TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                    headers,
                    CLUSTER.time);

            final List<ConsumerRecord<Long, Long>> intermediateRecords =
                    IntegrationTestUtils.waitUntilMinRecordsReceived(
                            TestUtils.consumerConfig(
                                    CLUSTER.bootstrapServers(),
                                    CONSUMER_GROUP_ID,
                                    LongDeserializer.class,
                                    LongDeserializer.class,
                                    new Properties()),
                            THROUGH_TOPIC,
                            inputData.size());

            checkResultRecordsHasHeaders(headers, intermediateRecords);

            final List<ConsumerRecord<Long, Long>> outputRecords =
                    IntegrationTestUtils.waitUntilMinRecordsReceived(
                            TestUtils.consumerConfig(
                                    CLUSTER.bootstrapServers(),
                                    CONSUMER_GROUP_ID,
                                    LongDeserializer.class,
                                    LongDeserializer.class,
                                    new Properties()),
                            OUTPUT_TOPIC,
                            inputData.size());

            checkResultRecordsHasHeaders(headers, outputRecords);
        } finally {
            streams.close();
        }

    }

    @Test
    public void shouldAddHeadersToInputRecord() throws Exception {
        final Topology topology = new Topology();

        final Headers[] headers = new Headers[1];

        topology
                .addSource("INPUT", INPUT_TOPIC)
                .addProcessor("HEADER_PROCESSOR", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new AbstractProcessor() {
                            @Override
                            public void process(final Object key, final Object value) {
                                context().headers().add("hkey", "hvalue".getBytes());
                                headers[0] = context().headers();
                                context().forward(key, value);
                            }
                        };
                    }
                }, "INPUT")
                .addSink("OUTPUT", OUTPUT_TOPIC, "HEADER_PROCESSOR");

        final KafkaStreams streams = new KafkaStreams(
                topology,
                StreamsTestUtils.getStreamsConfig(
                        applicationId,
                        CLUSTER.bootstrapServers(),
                        Serdes.LongSerde.class.getName(),
                        Serdes.LongSerde.class.getName(),
                        new Properties()));

        try {
            streams.start();

            final List<KeyValue<Long, Long>> inputData = prepareData(0, 10L, 0L, 1L);

            IntegrationTestUtils.produceKeyValuesSynchronously(
                    INPUT_TOPIC,
                    inputData,
                    TestUtils.producerConfig(CLUSTER.bootstrapServers(), LongSerializer.class, LongSerializer.class),
                    CLUSTER.time);

            final List<ConsumerRecord<Long, Long>> outputRecords =
                    IntegrationTestUtils.waitUntilMinRecordsReceived(
                            TestUtils.consumerConfig(
                                    CLUSTER.bootstrapServers(),
                                    CONSUMER_GROUP_ID,
                                    LongDeserializer.class,
                                    LongDeserializer.class,
                                    new Properties()),
                            OUTPUT_TOPIC,
                            inputData.size());

            checkResultRecordsHasHeaders(headers[0], outputRecords);
        } finally {
            streams.close();
        }

    }

    private void checkResultRecordsHasHeaders(final Headers headers, final List<ConsumerRecord<Long, Long>> committedRecords) {
        for (ConsumerRecord<Long, Long> record : committedRecords) {
            assertThat(headers, equalTo(record.headers()));
        }
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive, final long toExclusive, final Long... keys) {
        final List<KeyValue<Long, Long>> data = new ArrayList<>();

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }

}
