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
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.CorruptRecordException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LegacyRecordTest {

    private static class Args {
        final byte magic;
        final long timestamp;
        final ByteBuffer key;
        final ByteBuffer value;
        final CompressionType compression;
        final TimestampType timestampType;
        final LegacyRecord record;

        public Args(byte magic, long timestamp, byte[] key, byte[] value, CompressionType compression) {
            this.magic = magic;
            this.timestamp = timestamp;
            this.timestampType = TimestampType.CREATE_TIME;
            this.key = key == null ? null : ByteBuffer.wrap(key);
            this.value = value == null ? null : ByteBuffer.wrap(value);
            this.compression = compression;
            this.record = LegacyRecord.create(magic, timestamp, key, value, compression, timestampType);
        }

        @Override
        public String toString() {
            return "magic=" + magic +
                ", compression=" + compression +
                ", timestamp=" + timestamp;
        }
    }

    private static class LegacyRecordArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            byte[] payload = new byte[1000];
            Arrays.fill(payload, (byte) 1);
            List<Arguments> arguments = new ArrayList<>();
            for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1))
                for (long timestamp : Arrays.asList(RecordBatch.NO_TIMESTAMP, 0L, 1L))
                    for (byte[] key : Arrays.asList(null, "".getBytes(), "key".getBytes(), payload))
                        for (byte[] value : Arrays.asList(null, "".getBytes(), "value".getBytes(), payload))
                            for (CompressionType compression : CompressionType.values())
                                arguments.add(Arguments.of(new Args(magic, timestamp, key, value, compression)));
            return arguments.stream();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider.class)
    public void testFields(Args args) {
        LegacyRecord record = args.record;
        ByteBuffer key = args.key;
        assertEquals(args.compression, record.compressionType());
        assertEquals(key != null, record.hasKey());
        assertEquals(key, record.key());
        if (key != null)
            assertEquals(key.limit(), record.keySize());
        assertEquals(args.magic, record.magic());
        assertEquals(args.value, record.value());
        if (args.value != null)
            assertEquals(args.value.limit(), record.valueSize());
        if (args.magic > 0) {
            assertEquals(args.timestamp, record.timestamp());
            assertEquals(args.timestampType, record.timestampType());
        } else {
            assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
            assertEquals(TimestampType.NO_TIMESTAMP_TYPE, record.timestampType());
        }
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider.class)
    public void testChecksum(Args args) {
        LegacyRecord record = args.record;
        assertEquals(record.checksum(), record.computeChecksum());

        byte attributes = LegacyRecord.computeAttributes(args.magic, args.compression, TimestampType.CREATE_TIME);
        assertEquals(record.checksum(), LegacyRecord.computeChecksum(
                args.magic,
                attributes,
                args.timestamp,
                args.key == null ? null : args.key.array(),
                args.value == null ? null : args.value.array()
        ));
        assertTrue(record.isValid());
        for (int i = LegacyRecord.CRC_OFFSET + LegacyRecord.CRC_LENGTH; i < record.sizeInBytes(); i++) {
            LegacyRecord copy = copyOf(record);
            copy.buffer().put(i, (byte) 69);
            assertFalse(copy.isValid());
            assertThrows(CorruptRecordException.class, copy::ensureValid);
        }
    }

    private LegacyRecord copyOf(LegacyRecord record) {
        ByteBuffer buffer = ByteBuffer.allocate(record.sizeInBytes());
        record.buffer().put(buffer);
        buffer.rewind();
        record.buffer().rewind();
        return new LegacyRecord(buffer);
    }

    @ParameterizedTest
    @ArgumentsSource(LegacyRecordArgumentsProvider.class)
    public void testEquality(Args args) {
        assertEquals(args.record, copyOf(args.record));
    }

}
