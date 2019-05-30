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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category({IntegrationTest.class})
public class KStreamRestorationIntegrationTest {
    private StreamsBuilder builder = new StreamsBuilder();

    private static final String APPLICATION_ID = "restoration-test-app";
    private static final String STATE_STORE_NAME = "stateStore";
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private Properties streamsConfiguration;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private final MockTime mockTime = CLUSTER.time;

    @Before
    public void setUp() throws Exception {
        final Properties props = new Properties();

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                APPLICATION_ID,
                CLUSTER.bootstrapServers(),
                Serdes.Integer().getClass().getName(),
                Serdes.ByteArray().getClass().getName(),
                props);

        CLUSTER.createTopics(INPUT_TOPIC);
        CLUSTER.createTopics(OUTPUT_TOPIC);

        final StoreBuilder<KeyValueStore<Integer, byte[]>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore(STATE_STORE_NAME),
                        Serdes.Integer(),
                        Serdes.ByteArray());
        builder.addStateStore(keyValueStoreBuilder);
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldRestoreNullRecord() throws InterruptedException, ExecutionException {
        final KStream<Integer, byte[]> sourceStream = builder.stream(INPUT_TOPIC);
        final KStream<Integer, byte[]> transformedStream =
                sourceStream.transform(() -> new Transformer<Integer, byte[], KeyValue<Integer, byte[]>>() {
                    private KeyValueStore<Integer, byte[]> state;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext context) {
                        state = (KeyValueStore<Integer, byte[]>) context.getStateStore(STATE_STORE_NAME);
                    }

                    @Override
                    public KeyValue<Integer, byte[]> transform(final Integer key, final byte[] value) {
                        state.put(key, value);
                        return new KeyValue<>(key, value);
                    }

                    @Override
                    public void close() {
                    }
                }, STATE_STORE_NAME);
        transformedStream.to(OUTPUT_TOPIC);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, ByteArraySerializer.class);

        final List<KeyValue<Integer, byte[]>> initialKeyValues = Arrays.asList(KeyValue.pair(3, null), KeyValue.pair(1, new byte[]{5}));

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, initialKeyValues, producerConfig, mockTime);

        KafkaStreams streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        streams.start();

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), IntegerDeserializer.class, ByteArrayDeserializer.class);
        // We couldn't use a #waitUntilFinalKeyValueRecordsReceived here because the byte array comparison is not triggered correctly.
        final List<KeyValue<Integer, byte[]>> outputs = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, initialKeyValues.size());
        for (int i = 0; i < outputs.size(); i++) {
            assertEquals(outputs.get(i).key, initialKeyValues.get(i).key);
            assertArrayEquals(outputs.get(i).value, initialKeyValues.get(i).value);
        }

        streams.close();
        streams.cleanUp();

        // Restart the stream instance. There should not be exception handling the null value within changelog topic.
        final List<KeyValue<Integer, byte[]>> expectedNewKeyValues = Collections.singletonList(KeyValue.pair(1, null));
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, expectedNewKeyValues, producerConfig, mockTime);
        streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        streams.start();
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, expectedNewKeyValues);
        streams.close();
    }
}
