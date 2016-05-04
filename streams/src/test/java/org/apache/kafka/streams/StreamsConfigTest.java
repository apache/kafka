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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
        props.put("key.deserializer.encoding", "UTF8");
        props.put("value.deserializer.encoding", "UTF-16");
        streamsConfig = new StreamsConfig(props);

    @Test
    public void testGetProducerConfigs() throws Exception {
        Map<String, Object> returnedProps = customStreamsConfig.getProducerConfigs("client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-producer");
        assertEquals(returnedProps.get(ProducerConfig.LINGER_MS_CONFIG), "50");
    }

    @Test
    public void testGetDefaultProducerConfigs() throws Exception {
        Map<String, Object> returnedProps = defaultStreamsConfig.getProducerConfigs("client");
        assertEquals(returnedProps.get(ProducerConfig.LINGER_MS_CONFIG), "100");
    }

    @Test
    public void testGetConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamsConfig.getConsumerConfigs(null, "example-application", "client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-consumer");
        assertEquals(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), "example-application");
        assertEquals(returnedProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
    }

    @Test
    public void testGetDefaultConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps =
                defaultStreamsConfig.getConsumerConfigs(streamThreadPlaceHolder, "example-application", "client");

        assertEquals(returnedProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false");
    }

    @Test
    public void testGetRestoreConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = customStreamsConfig.getRestoreConsumerConfigs("client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-restore-consumer");
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
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
}
