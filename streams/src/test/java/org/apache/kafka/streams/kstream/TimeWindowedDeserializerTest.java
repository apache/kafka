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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TimeWindowedDeserializerTest {
    @Test
    public void testWindowedDeserializerNoArgConstructors() {
        final Map<String, String> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArraySerde.class.getName());

        TimeWindowedDeserializer<?> timeWindowedDeserializer = new TimeWindowedDeserializer<>();
        timeWindowedDeserializer.configure(props, true);
        Deserializer<?> inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);

        timeWindowedDeserializer = new TimeWindowedDeserializer<>();
        timeWindowedDeserializer.configure(props, false);
        inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be ByteArrayDeserializer", inner instanceof ByteArrayDeserializer);
    }

    @Test
    public void testWindowDeserializeExpectedWindowSize() {
        final long randomLong = 5000000;
        final Map<String, String> props = new HashMap<>();
        final TimeWindowedDeserializer<StringSerializer> timeWindowedDeserializer = new TimeWindowedDeserializer<>(null, randomLong);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        timeWindowedDeserializer.configure(props, true);

        // test for deserializer expected window end time
        final byte[] byteValues = new StringSerializer().serialize("topic", "dummy topic");
        final Windowed<?> windowed = timeWindowedDeserializer.deserialize("topic", byteValues);
        final long actualSize = windowed.window().end() - windowed.window().start();
        assertEquals(randomLong, actualSize);
        timeWindowedDeserializer.close();
    }
}
