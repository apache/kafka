package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GlobalKTableStoreDebugTest {

    @Test
    public void testConvertToHeaderFormat() {
        // Test the conversion function that's used when wrapping old-format stores

        // Simulate what an old TimestampedKeyValueStore would return
        // Format: [timestamp(8)][value]
        final long timestamp = 123456789L;
        final String value = "A";
        final byte[] valueBytes = value.getBytes();

        final ByteBuffer oldFormat = ByteBuffer.allocate(8 + valueBytes.length);
        oldFormat.putLong(timestamp);
        oldFormat.put(valueBytes);

        System.out.println("=== Old Format Test ===");
        System.out.println("Input (old format) length: " + oldFormat.array().length);
        System.out.println("Input (old format) hex: " + bytesToHex(oldFormat.array()));

        // Apply convertToHeaderFormat
        final byte[] converted = org.apache.kafka.streams.state.HeadersBytesStore.convertToHeaderFormat(oldFormat.array());

        System.out.println("After convertToHeaderFormat length: " + converted.length);
        System.out.println("After convertToHeaderFormat hex: " + bytesToHex(converted));

        // Expected: [00][timestamp(8)][value]
        // Should be: 1 + 8 + 1 = 10 bytes
        assertEquals(10, converted.length, "Expected 10 bytes");
        assertEquals(0x00, converted[0], "First byte should be 0x00 for empty headers");
        assertEquals(0x41, converted[9], "Last byte should be 0x41 for 'A'");

        // Now deserialize with ValueTimestampHeadersDeserializer
        final ValueTimestampHeadersSerde<String> serde = new ValueTimestampHeadersSerde<>(Serdes.String());
        final ValueTimestampHeaders<String> result = serde.deserializer().deserialize("test", converted);

        System.out.println("Deserialized value: '" + result.value() + "'");
        System.out.println("Deserialized value length: " + result.value().length());
        System.out.println("Deserialized timestamp: " + result.timestamp());

        if (result.value().length() > 1) {
            System.out.println("!!! VALUE HAS EXTRA BYTES !!!");
            final byte[] bytes = result.value().getBytes();
            for (int i = 0; i < bytes.length; i++) {
                System.out.printf("Byte[%d] = 0x%02X%n", i, bytes[i]);
            }
        }

        assertEquals("A", result.value());
        assertEquals(timestamp, result.timestamp());
    }

    private static String bytesToHex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }
}