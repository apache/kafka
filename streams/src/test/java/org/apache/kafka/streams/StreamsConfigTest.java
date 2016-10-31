/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamsConfigTest {

    private Properties props = new Properties();
    private StreamsConfig streamsConfig;

    @Before
    public void setUp() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-config-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("DUMMY", "dummy");
        props.put("key.deserializer.encoding", "UTF8");
        props.put("value.deserializer.encoding", "UTF-16");
        streamsConfig = new StreamsConfig(props);
    }

    @Test
    public void testGetProducerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamsConfig.getProducerConfigs("client");
        assertEquals(returnedProps.get(ProducerConfig.CLIENT_ID_CONFIG), "client-producer");
        assertEquals(returnedProps.get(ProducerConfig.LINGER_MS_CONFIG), "100");
        assertNull(returnedProps.get("DUMMY"));
    }

    @Test
    public void testGetConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamsConfig.getConsumerConfigs(null, "example-application", "client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-consumer");
        assertEquals(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), "example-application");
        assertEquals(returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "1000");
        assertNull(returnedProps.get("DUMMY"));
    }

    @Test
    public void testGetRestoreConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamsConfig.getRestoreConsumerConfigs("client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-restore-consumer");
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertNull(returnedProps.get("DUMMY"));
    }

    @Test
    public void defaultSerdeShouldBeConfigured() {
        Map<String, Object> serializerConfigs = new HashMap<String, Object>();
        serializerConfigs.put("key.serializer.encoding", "UTF8");
        serializerConfigs.put("value.serializer.encoding", "UTF-16");
        Serializer<String> serializer = Serdes.String().serializer();

        String str = "my string for testing";
        String topic = "my topic";

        serializer.configure(serializerConfigs, true);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.keySerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));

        serializer.configure(serializerConfigs, false);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.valueSerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));
    }

    @Test
    public void shouldSupportMultipleBootstrapServers() {
        List<String> expectedBootstrapServers = Arrays.asList("broker1:9092", "broker2:9092");
        String bootstrapServersString = Utils.mkString(expectedBootstrapServers, ",").toString();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "irrelevant");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
        StreamsConfig config = new StreamsConfig(props);

        List<String> actualBootstrapServers = config.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertEquals(expectedBootstrapServers, actualBootstrapServers);
    }

    @Test
    public void shouldSupportPrefixedConsumerConfigs() throws Exception {
        props.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedRestoreConsumerConfigs() throws Exception {
        props.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfConsumerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfRestoreConsumerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("clientId");
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfProducerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(producerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertEquals("host", producerConfigs.get("interceptor.statsd.host"));
    }


    @Test
    public void shouldSupportPrefixedProducerConfigs() throws Exception {
        props.put(producerPrefix(ProducerConfig.BUFFER_MEMORY_CONFIG), 10);
        props.put(producerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs("client");
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedConsumerConfigs() throws Exception {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedRestoreConsumerConfigs() throws Exception {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("groupId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportNonPrefixedProducerConfigs() throws Exception {
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 10);
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs("client");
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }



    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfKeySerdeConfigFails() throws Exception {
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        streamsConfig.keySerde();
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfValueSerdeConfigFails() throws Exception {
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        streamsConfig.valueSerde();
    }

    static class MisconfiguredSerde implements Serde {
        @Override
        public void configure(final Map configs, final boolean isKey) {
            throw new RuntimeException("boom");
        }

        @Override
        public void close() {

        }

        @Override
        public Serializer serializer() {
            return null;
        }

        @Override
        public Deserializer deserializer() {
            return null;
        }
    }
}
