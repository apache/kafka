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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.examples.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;



public class StreamConfigTest {

    private Properties props = new Properties();
    private StreamConfig streamConfig;
    private StreamThread streamThreadPlaceHolder;


    @Before
    public void setUp() {
        props.put(StreamConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(StreamConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamConfig = new StreamConfig(props);
    }

    @Test
    public void testGetProducerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamConfig.getProducerConfigs("client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-producer");
    }

    @Test
    public void testGetConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamConfig.getConsumerConfigs(streamThreadPlaceHolder, "example-job", "client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-consumer");
        assertEquals(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), "example-job");

    }

    @Test
    public void testGetRestoreConsumerConfigs() throws Exception {
        Map<String, Object> returnedProps = streamConfig.getRestoreConsumerConfigs("client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-restore-consumer");
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
    }
}
