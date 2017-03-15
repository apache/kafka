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

package org.apache.kafka.streams.kstream.internals;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.KStreamTestDriver;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KeyValuePrinterProcessorTest {

    private final String topicName = "topic";
    private final Serde<String> stringSerde = Serdes.String();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final KStreamBuilder builder = new KStreamBuilder();
    private final PrintStream printStream = new PrintStream(baos);

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testPrintKeyValueDefaultSerde() throws Exception {

        KeyValuePrinter<String, String> keyValuePrinter = new KeyValuePrinter<>(printStream, null);
        String[] suppliedKeys = {"foo", "bar", null};
        String[] suppliedValues = {"value1", "value2", "value3"};
        String[] expectedValues = {"[null]: foo , value1", "[null]: bar , value2", "[null]: null , value3"};


        KStream<String, String> stream = builder.stream(stringSerde, stringSerde, topicName);
        stream.process(keyValuePrinter);

        driver = new KStreamTestDriver(builder);
        for (int i = 0; i < suppliedKeys.length; i++) {
            driver.process(topicName, suppliedKeys[i], suppliedValues[i]);
        }

        String[] capturedValues = new String(baos.toByteArray(), Charset.forName("UTF-8")).split("\n");

        for (int i = 0; i < capturedValues.length; i++) {
            assertEquals(capturedValues[i], expectedValues[i]);
        }
    }

    @Test
    public void testPrintKeyValuesWithName() throws Exception {

        KeyValuePrinter<String, String> keyValuePrinter = new KeyValuePrinter<>(printStream, "test-stream");
        String[] suppliedKeys = {"foo", "bar", null};
        String[] suppliedValues = {"value1", "value2", "value3"};
        String[] expectedValues = {"[test-stream]: foo , value1", "[test-stream]: bar , value2", "[test-stream]: null , value3"};


        KStream<String, String> stream = builder.stream(stringSerde, stringSerde, topicName);
        stream.process(keyValuePrinter);

        driver = new KStreamTestDriver(builder);
        for (int i = 0; i < suppliedKeys.length; i++) {
            driver.process(topicName, suppliedKeys[i], suppliedValues[i]);
        }

        String[] capturedValues = new String(baos.toByteArray(), Charset.forName("UTF-8")).split("\n");

        for (int i = 0; i < capturedValues.length; i++) {
            assertEquals(capturedValues[i], expectedValues[i]);
        }
    }


    @Test
    public void testPrintKeyValueWithProvidedSerde() throws Exception {

        Serde<MockObject> mockObjectSerde = Serdes.serdeFrom(new MockSerializer(), new MockDeserializer());
        KeyValuePrinter<String, MockObject> keyValuePrinter = new KeyValuePrinter<>(printStream, stringSerde, mockObjectSerde, null);
        KStream<String, MockObject> stream = builder.stream(stringSerde, mockObjectSerde, topicName);

        stream.process(keyValuePrinter);

        driver = new KStreamTestDriver(builder);

        String suppliedKey = null;
        byte[] suppliedValue = "{\"name\":\"print\", \"label\":\"test\"}".getBytes(Charset.forName("UTF-8"));

        driver.process(topicName, suppliedKey, suppliedValue);
        String expectedPrintedValue = "[null]: null , name:print label:test";
        String capturedValue = new String(baos.toByteArray(), Charset.forName("UTF-8")).trim();

        assertEquals(capturedValue, expectedPrintedValue);

    }

    private static class MockObject {
        public String name;
        public String label;

        public MockObject() {
        }

        MockObject(String name, String label) {
            this.name = name;
            this.label = label;
        }

        @Override
        public String toString() {
            return "name:" + name + " label:" + label;
        }
    }


    private static class MockDeserializer implements Deserializer<MockObject> {

        private com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public MockObject deserialize(String topic, byte[] data) {
            MockObject mockObject;
            try {
                mockObject = objectMapper.readValue(data, MockObject.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
            return mockObject;
        }

        @Override
        public void close() {

        }
    }


    private static class MockSerializer implements Serializer<MockObject> {
        private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, MockObject data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {

        }
    }


}