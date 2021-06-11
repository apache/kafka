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
import static org.junit.Assert.assertThrows;

public class SessionWindowedSerializerTest {
    private final SessionWindowedSerializer<?> sessionWindowedSerializer = new SessionWindowedSerializer<>(Serdes.String().serializer());
    private final Map<String, String> props = new HashMap<>();

    @Test
    public void testSessionWindowedSerializerConstructor() {
        sessionWindowedSerializer.configure(props, true);
        final Serializer<?> inner = sessionWindowedSerializer.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner);
        assertTrue("Inner serializer type should be StringSerializer", inner instanceof StringSerializer);
    }

    @Test
    public void shouldSetWindowedInnerClassSerialiserThroughConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        final SessionWindowedSerializer<?> serializer = new SessionWindowedSerializer<>();
        serializer.configure(props, false);
        assertTrue(serializer.innerSerializer() instanceof ByteArraySerializer);
    }

    @Test
    public void shouldThrowErrorIfWindowInnerClassSerialiserIsNotSet() {
        final SessionWindowedSerializer<?> serializer = new SessionWindowedSerializer<>();
        assertThrows(IllegalArgumentException.class, () -> serializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfSerialisersConflictInConstructorAndConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> sessionWindowedSerializer.configure(props, false));
    }

    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowInnerClassSerialiserSupplied() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> sessionWindowedSerializer.configure(props, false));
    }
}
