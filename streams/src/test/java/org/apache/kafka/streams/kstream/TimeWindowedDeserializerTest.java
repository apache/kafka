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
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TimeWindowedDeserializerTest {
    private final long windowSize = 5000000;
    private final TimeWindowedDeserializer<?> timeWindowedDeserializer = new TimeWindowedDeserializer<>(null, windowSize);
    private final Map<String, String> props = new HashMap<>();

    @Before
    public void setUp() {
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArraySerde.class.getName());
    }

    @Test
    public void testWindowedKeyDeserializerNoArgConstructors() {
        timeWindowedDeserializer.configure(props, true);
        final Deserializer<?> inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
        assertThat(timeWindowedDeserializer.getWindowSize(), is(5000000L));
    }

    @Test
    public void testWindowedValueDeserializerNoArgConstructors() {
        timeWindowedDeserializer.configure(props, false);
        final Deserializer<?> inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be ByteArrayDeserializer", inner instanceof ByteArrayDeserializer);
        assertThat(timeWindowedDeserializer.getWindowSize(), is(5000000L));
    }

    @Test
    public void shouldSetWindowSizeThroughConfigs() {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, "500");
        final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>();
        deserializer.configure(props, false);
        assertThat(deserializer.getWindowSize(), is(500L));
    }

    @Test
    public void shouldThrowErrorIfWindowSizeSetInConfigsAndConstructor() {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, "500");
        assertThrows(IllegalArgumentException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfWindowSizeIsNotSet() {
        final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>();
        assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
    }
}
