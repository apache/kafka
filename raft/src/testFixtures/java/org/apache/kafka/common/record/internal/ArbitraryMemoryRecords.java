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
package org.apache.kafka.common.record.internal;

import org.junit.jupiter.api.function.ThrowingConsumer;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public final class ArbitraryMemoryRecords {

    private ArbitraryMemoryRecords() {}

    public static void forRandomRecords(int tries, ThrowingConsumer<MemoryRecords> test) {
        for (int i = 0; i < tries; i++) {
            long seed = System.nanoTime() + i;
            Random random = new Random(seed);
            MemoryRecords records = buildRandomRecords(random);
            assertDoesNotThrow(
                    () -> test.accept(records),
                    () -> "Failed with seed=" + seed + ", size=" + records.sizeInBytes());
        }
    }

    static MemoryRecords buildRandomRecords(Random random) {
        int size = random.nextInt(128) + DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(Records.SIZE_OFFSET, size - Records.LOG_OVERHEAD);
        buffer.put(Records.MAGIC_OFFSET, (byte) (RecordBatch.CURRENT_MAGIC_VALUE + 1));
        return MemoryRecords.readableRecords(buffer);
    }
}
