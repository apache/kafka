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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TimeWindowedDeserializerTest {
    @Test
    public void testWindowedDeserializerNoArgConstructors() {
        Map<String, String> props = new HashMap<>();
        // test key[value].deserializer.inner.class takes precedence over serializer.inner.class
        TimeWindowedDeserializer<StringSerializer> timeWindowedDeserializer = new TimeWindowedDeserializer<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put("key.deserializer.inner.class", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("deserializer.inner.class", "org.apache.kafka.common.serialization.StringDeserializer");
        timeWindowedDeserializer.configure(props, true);
        Deserializer<?> inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
        // test deserializer.inner.class
        props.put("deserializer.inner.class", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.remove("key.deserializer.inner.class");
        props.remove("value.deserializer.inner.class");
        TimeWindowedDeserializer<?> timeWindowedDeserializer1 = new TimeWindowedDeserializer<>();
        timeWindowedDeserializer1.configure(props, false);
        Deserializer<?> inner1 = timeWindowedDeserializer1.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner1);
        assertTrue("Inner deserializer type should be ByteArrayDeserializer", inner1 instanceof ByteArrayDeserializer);
    }
}
