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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

public final class InvalidMemoryRecordsProvider implements ArgumentsProvider {
    // Use a baseOffset that's not zero so that it is less likely to match the LEO
    private static final long BASE_OFFSET = 1234;
    private static final int EPOCH = 4321;

    /**
     * Returns a stream of arguments for invalid memory records and the expected exception.
     *
     * The first object in the {@code Arguments} is a {@code MemoryRecords}.
     *
     * The second object in the {@code Arguments} is an {@code Optional<Class<Exception>>} which is
     * the expected exception from the log layer.
     */
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            Arguments.of(MemoryRecords.readableRecords(notEnoughBytes()), Optional.empty()),
            Arguments.of(MemoryRecords.readableRecords(recordsSizeTooSmall()), Optional.of(CorruptRecordException.class)),
            Arguments.of(MemoryRecords.readableRecords(notEnoughBytesToMagic()), Optional.empty()),
            Arguments.of(MemoryRecords.readableRecords(negativeMagic()), Optional.of(CorruptRecordException.class)),
            Arguments.of(MemoryRecords.readableRecords(largeMagic()), Optional.of(CorruptRecordException.class)),
            Arguments.of(MemoryRecords.readableRecords(lessBytesThanRecordSize()), Optional.empty())
        );
    }

    private static ByteBuffer notEnoughBytes() {
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
