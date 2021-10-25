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
package org.apache.kafka.jmh.compression;

import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;

@State(Scope.Benchmark)
public abstract class AbstractCompressionBenchmark {

    private final static Random RANDOM = new Random(0);
    private final static int BATCH_SIZE = 1;

    @Param(value = {"100", "1000", "10000", "100000"})
    private int messageSize = 1000;

    private ByteBuffer[] batch;
    private ByteBuffer buffer;

    @Setup
    public void init() {
        // Create batch
        this.batch = new ByteBuffer[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; ++i) {
            byte[] value = new byte[messageSize];
            RANDOM.nextBytes(value);
            this.batch[i] = ByteBuffer.wrap(value);
        }

        // Create Buffer
        byte[] value = new byte[messageSize];
        this.buffer = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.MAGIC_VALUE_V2, compressionConfig().getType(),
                new byte[0], value, Record.EMPTY_HEADERS) * BATCH_SIZE
        );
    }

    abstract CompressionConfig compressionConfig();

    @Benchmark
    public void measureMemoryRecords(Blackhole blackhole) {
        final MemoryRecordsBuilder builder =
            MemoryRecords.builder(this.buffer, RecordBatch.MAGIC_VALUE_V2, compressionConfig(), TimestampType.CREATE_TIME, 0);
        for (ByteBuffer value : this.batch) {
            builder.append(0L, null, value);
        }
        // consume
        blackhole.consume(builder.build());
    }
}
