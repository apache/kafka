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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SessionWindowedDeserializerTest {
    private final SessionWindowedDeserializer<?> sessionWindowedDeserializer = new SessionWindowedDeserializer<>(new StringDeserializer());
    private final Map<String, String> props = new HashMap<>();

    @Test
    public void testSessionWindowedDeserializerConstructor() {
        sessionWindowedDeserializer.configure(props, true);
        final Deserializer<?> inner = sessionWindowedDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
    }

    @Test
    public void shouldSetWindowedInnerClassDeserialiserThroughConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>();
        deserializer.configure(props, false);
        assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
    }

    @Test
    public void shouldThrowErrorIfWindowInnerClassDeserialiserIsNotSet() {
        final SessionWindowedDeserializer<?> deserializer = new SessionWindowedDeserializer<>();
        assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfDeserialisersConflictInConstructorAndConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowInnerClassDeserialiserSupplied() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> sessionWindowedDeserializer.configure(props, false));
    }
}
