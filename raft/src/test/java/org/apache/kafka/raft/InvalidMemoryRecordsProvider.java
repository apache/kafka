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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public final class InvalidMemoryRecordsProvider implements ArgumentsProvider {
    // Use a baseOffset that not zero so that is less likely to match the LEO
    private static final long BASE_OFFSET = 1234;
    public static final int EPOCH = 4321;

    /** Returns a stream of arguements for invalid memory records and the expected exception.
     *
     * The first object in the Arguments is a MemoryRecords.
     *
     * The second object in the Arguments is an Class<Exception> which is the expected exception from the log layer
     */
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            Arguments.of(MemoryRecords.readableRecords(notEnoughtBytes()), CorruptRecordException.class),
            Arguments.of(MemoryRecords.readableRecords(recordsSizeTooSmall()), CorruptRecordException.class),
            Arguments.of(MemoryRecords.readableRecords(notEnoughBytesToMagic()), CorruptRecordException.class),
            Arguments.of(MemoryRecords.readableRecords(negativeMagic()), CorruptRecordException.class),
            Arguments.of(MemoryRecords.readableRecords(largeMagic()), CorruptRecordException.class),
            Arguments.of(MemoryRecords.readableRecords(lessBytesThanRecordSize()), CorruptRecordException.class)
        );
    }

    private static ByteBuffer notEnoughtBytes() {
        var buffer = ByteBuffer.allocate(Records.LOG_OVERHEAD - 1);
        buffer.limit(buffer.capacity());

        return buffer;
    }

    private static ByteBuffer recordsSizeTooSmall() {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(LegacyRecord.RECORD_OVERHEAD_V0 - 1);
        buffer.position(0);
        buffer.limit(buffer.capacity());

        return buffer;
    }

    private static ByteBuffer notEnoughBytesToMagic() {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        buffer.position(0);
        buffer.limit(Records.HEADER_SIZE_UP_TO_MAGIC - 1);

        return buffer;
    }

    private static ByteBuffer negativeMagic() {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put((byte) -1);
        buffer.position(0);
        buffer.limit(buffer.capacity());

        return buffer;
    }

    private static ByteBuffer largeMagic() {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put((byte) (RecordBatch.CURRENT_MAGIC_VALUE + 1));
        buffer.position(0);
        buffer.limit(buffer.capacity());

        return buffer;
    }

    private static ByteBuffer lessBytesThanRecordSize() {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put(RecordBatch.CURRENT_MAGIC_VALUE);
        buffer.position(0);
        buffer.limit(buffer.capacity() - Records.LOG_OVERHEAD - 1);

        return buffer;
    }
}
