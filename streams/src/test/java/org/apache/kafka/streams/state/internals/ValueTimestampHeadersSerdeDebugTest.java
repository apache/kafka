package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ValueTimestampHeadersSerdeDebugTest {

    @Test
    public void testSerializeDeserializeString() {
        final ValueTimestampHeadersSerde<String> serde = new ValueTimestampHeadersSerde<>(Serdes.String());

        final ValueTimestampHeaders<String> input = ValueTimestampHeaders.make("A", 123456789L, new RecordHeaders());

        // Serialize
        final byte[] serialized = serde.serializer().serialize("test-topic", input);

        System.out.println("=== Serialization Test ===");
        System.out.println("Input value: " + input.value());
        System.out.println("Serialized bytes length: " + serialized.length);
        System.out.println("Serialized bytes (hex): " + bytesToHex(serialized));

        // Expected format: [headersSize=0][timestamp(8)][value]
        // For "A": [00][8 bytes timestamp][41]
        // Total: 1 + 8 + 1 = 10 bytes
        assertEquals(10, serialized.length, "Expected 10 bytes: 1 (headersSize) + 8 (timestamp) + 1 (value 'A')");
        assertEquals(0x00, serialized[0], "First byte should be 0x00 for empty headers");
        assertEquals(0x41, serialized[9], "Last byte should be 0x41 for 'A'");

        // Deserialize
        final ValueTimestampHeaders<String> deserialized = serde.deserializer().deserialize("test-topic", serialized);

        System.out.println("Deserialized value: " + deserialized.value());
        System.out.println("Deserialized timestamp: " + deserialized.timestamp());

        assertEquals("A", deserialized.value());
        assertEquals(123456789L, deserialized.timestamp());
    }

    private static String bytesToHex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }
}