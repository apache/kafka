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
package org.apache.kafka.jmh.record;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class RecordBatchIterationBenchmark extends BaseRecordBatchBenchmark {

    @Param(value = {"LZ4", "SNAPPY", "GZIP", "ZSTD", "NONE"})
    private CompressionType compressionType = CompressionType.NONE;

    @Override
    CompressionType compressionType() {
        return compressionType;
    }

    @Benchmark
    public void measureIteratorForBatchWithSingleMessage(Blackhole bh) throws IOException {
        for (RecordBatch batch : MemoryRecords.readableRecords(singleBatchBuffer.duplicate()).batches()) {
            try (CloseableIterator<Record> iterator = batch.streamingIterator(bufferSupplier)) {
                while (iterator.hasNext())
                    bh.consume(iterator.next());
            }
        }
    }

    @OperationsPerInvocation(value = batchCount)
    @Fork(jvmArgsAppend = "-Xmx8g")
    @Benchmark
    public void measureStreamingIteratorForVariableBatchSize(Blackhole bh) throws IOException {
        for (int i = 0; i < batchCount; ++i) {
            for (RecordBatch batch : MemoryRecords.readableRecords(batchBuffers[i].duplicate()).batches()) {
                try (CloseableIterator<Record> iterator = batch.streamingIterator(bufferSupplier)) {
                    while (iterator.hasNext())
                        bh.consume(iterator.next());
                }
            }
        }
    }

    @OperationsPerInvocation(value = batchCount)
    @Fork(jvmArgsAppend = "-Xmx8g")
    @Benchmark
    public void measureSkipIteratorForVariableBatchSize(Blackhole bh) throws IOException {
        for (int i = 0; i < batchCount; ++i) {
            for (MutableRecordBatch batch : MemoryRecords.readableRecords(batchBuffers[i].duplicate()).batches()) {
                try (CloseableIterator<Record> iterator = batch.skipKeyValueIterator(bufferSupplier)) {
                    while (iterator.hasNext())
                        bh.consume(iterator.next());
                }
            }
        }
    }
}
