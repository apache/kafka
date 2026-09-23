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
package org.apache.kafka.common.serialization;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class ListDeserializerTest {
    private final ListDeserializer<?> listDeserializer = new ListDeserializer<>();
    private final Map<String, Object> props = new HashMap<>();
    private final String nonExistingClass = "non.existing.class";
    private static class FakeObject {
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsWithClassNames() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class.getName());
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        listDeserializer.configure(props, true);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsWithClassNames() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class.getName());
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.IntegerSerde.class.getName());
        listDeserializer.configure(props, false);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(IntegerDeserializer.class, inner, "Inner deserializer type should be IntegerDeserializer");
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsWithClassObjects() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listDeserializer.configure(props, true);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsWithClassObjects() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listDeserializer.configure(props, false);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingInnerClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Not able to determine the inner serde class because "
            + "it was neither passed via the constructor nor set in the config.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingInnerClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Not able to determine the inner serde class because "
            + "it was neither passed via the constructor nor set in the config.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingTypeClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Not able to determine the list class because "
            + "it was neither passed via the constructor nor set in the config.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingTypeClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Not able to determine the list class because "
            + "it was neither passed via the constructor nor set in the config.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, new FakeObject());
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Could not determine the list class instance using "
            + "\"" + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, new FakeObject());
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Could not determine the list class instance using "
            + "\"" + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Could not determine the inner serde class instance using "
            + "\"" + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Could not determine the inner serde class instance using "
            + "\"" + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + "\" property.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueListClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, nonExistingClass);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Invalid value " + nonExistingClass + " for configuration "
            + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS + ": Deserializer's list class "
            + "\"" + nonExistingClass + "\" could not be found.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueListClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, nonExistingClass);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Invalid value " + nonExistingClass + " for configuration "
            + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS + ": Deserializer's list class "
            + "\"" + nonExistingClass + "\" could not be found.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueInnerSerdeClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, nonExistingClass);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertEquals("Invalid value " + nonExistingClass + " for configuration "
            + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + ": Deserializer's inner serde class "
            + "\"" + nonExistingClass + "\" could not be found.", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowConfigExceptionDueInnerSerdeClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, nonExistingClass);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertEquals("Invalid value " + nonExistingClass + " for configuration "
            + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + ": Deserializer's inner serde class "
            + "\"" + nonExistingClass + "\" could not be found.", exception.getMessage());
    }

    @Test
    public void testListKeyDeserializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ListDeserializer<Integer> initializedListDeserializer = new ListDeserializer<>(ArrayList.class, new IntegerDeserializer());
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> initializedListDeserializer.configure(props, true)
        );
        assertEquals("List deserializer was already initialized using a non-default constructor", exception.getMessage());
    }

    @Test
    public void testListValueDeserializerShouldThrowConfigExceptionDueAlreadyInitialized() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ListDeserializer<Integer> initializedListDeserializer = new ListDeserializer<>(ArrayList.class, new IntegerDeserializer());
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> initializedListDeserializer.configure(props, true)
        );
        assertEquals("List deserializer was already initialized using a non-default constructor", exception.getMessage());
    }

    @Test
    public void shouldPassHeadersToUnderlyingDeserializer() {
        final Deserializer<String> mockDeserializer = mock(StringDeserializer.class);
        when(mockDeserializer.deserialize(anyString(), any(Headers.class), any(byte[].class))).thenReturn("test-value");

        final String topic = "topic";
        final List<String> data = List.of("test-value");
        final byte[] serializedData = new ListSerializer<>(new StringSerializer()).serialize(topic, data);
        final Headers headers = new RecordHeaders().add("key1", "value1".getBytes());

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, mockDeserializer);

        testDeserializer.deserialize(topic, headers, serializedData);

        verify(mockDeserializer).deserialize(eq(topic), eq(headers), any(byte[].class));
        verify(mockDeserializer, never()).deserialize(anyString(), any(byte[].class));
    }

    @Test
    public void shouldThrowOnNegativeLength() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE.ordinal(),
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // encodes length == -1
        };

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. The number of list entries cannot be negative.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowOnTooLargeLength() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE.ordinal(),
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF // encodes length 255
        };

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. The number of list entries cannot be larger than overall number of bytes.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowOnNegativeEntrySize() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE.ordinal(),
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, // encodes length == 0
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE // encodes entrySize == -2 (-1 would be a valid `null` entry)
        };

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. A list entry cannot have negative size.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowOnTooLargeEntrySize() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE.ordinal(),
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, // encodes length == 1
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF, // encodes entrySize == 255
        };

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. A list entry cannot be larger than the overall number of bytes.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowOnEntryTruncatedMidStream() {
        // entrySize (10) is within data.length (14) so it passes the bounds check, but only 5 payload bytes
        // actually follow, so deserialization must fail rather than return a truncated entry.
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.VARIABLE_SIZE.ordinal(),
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, // encodes length == 1
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0A, // encodes entrySize == 10
            (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o' // only 5 of the 10 declared bytes
        };
        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());
        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "End of the stream was reached prematurely",
            exception.getMessage()
        );
    }

    @Test
    public void shouldDeserializeListEndingInEmptyEntry() {
        // A list whose last element serializes to zero bytes leaves the stream at EOF when that final
        // (empty) entry is read: the deserializer must handle this correctly and should not fail.
        final String topic = "topic";
        final List<String> data = List.of("a", "");
        final byte[] serializedData = new ListSerializer<>(new StringSerializer()).serialize(topic, data);

        final ListDeserializer<String> testDeserializer = new ListDeserializer<>(ArrayList.class, new StringDeserializer());

        assertEquals(data, testDeserializer.deserialize(topic, serializedData));
    }

    @Test
    public void shouldThrowOnNegativeNullEntryLength() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE.ordinal(),
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // encodes number of null entries == -1
        };

        final ListDeserializer<Integer> testDeserializer = new ListDeserializer<>(ArrayList.class, new IntegerDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. The number of null list entries cannot be negative.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowOnTooLargeNullEntryLength() {
        final byte[] corruptedData = new byte[] {
            (byte) Serdes.ListSerde.SerializationStrategy.CONSTANT_SIZE.ordinal(),
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF // encodes number of null entries == 255
        };

        final ListDeserializer<Integer> testDeserializer = new ListDeserializer<>(ArrayList.class, new IntegerDeserializer());

        final SerializationException exception = assertThrows(
            SerializationException.class,
            () -> testDeserializer.deserialize(null, corruptedData)
        );
        assertEquals(
            "Corrupted byte[]. The number of null list entries cannot be larger than overall number of elements.",
            exception.getMessage()
        );
    }

}
