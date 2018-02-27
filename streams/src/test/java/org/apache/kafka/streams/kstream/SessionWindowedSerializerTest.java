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

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SessionWindowedSerializerTest {
    @Test
    public void testWindowedSerializerNoArgConstructors() {
        Map<String, String> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArraySerde.class.getName());

        SessionWindowedSerializer<?> sessionWindowedDeserializer = new SessionWindowedSerializer<>();
        sessionWindowedDeserializer.configure(props, true);
        Serializer<?> inner = sessionWindowedDeserializer.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner);
        assertTrue("Inner serializer type should be StringDeserializer", inner instanceof StringSerializer);

        sessionWindowedDeserializer = new SessionWindowedSerializer<>();
        sessionWindowedDeserializer.configure(props, false);
        inner = sessionWindowedDeserializer.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner);
        assertTrue("Inner serializer type should be ByteArrayDeserializer", inner instanceof ByteArraySerializer);
    }
}