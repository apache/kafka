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

import kafka.api.ApiVersion;
import kafka.common.LongRef;
import kafka.log.AppendOrigin;
import kafka.log.LogValidator;
import kafka.message.CompressionCodec;
import kafka.server.BrokerTopicStats;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class RecordBatchIterationBenchmark {

    private final Random random = new Random(0);
    private final int batchCount = 100;

    public enum Bytes {
        RANDOM, ONES
    }

    @Param(value = {"1", "2", "10", "50", "200", "500"})
    private int maxBatchSize = 200;

    @Param(value = {"LZ4", "SNAPPY", "GZIP", "ZSTD", "NONE"})
    private CompressionType compressionType = CompressionType.NONE;

    @Param(value = {"1", "2"})
    private byte messageVersion = CURRENT_MAGIC_VALUE;

    @Param(value = {"100", "1000", "10000", "100000"})
    private int messageSize = 1000;

    @Param(value = {"RANDOM", "ONES"})
    private Bytes bytes = Bytes.RANDOM;

    @Param(value = {"NO_CACHING", "CREATE"})
    private String bufferSupplierStr;

    // zero starting offset is much faster for v1 batches, but that will almost never happen
    private int startingOffset;

    // Used by measureSingleMessage
    private ByteBuffer singleBatchBuffer;

    // Used by measureVariableBatchSize
    private ByteBuffer[] batchBuffers;
    private int[] batchSizes;
    private BufferSupplier bufferSupplier;
    private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();

    @Setup
    public void init() {
        brokerTopicStats = new BrokerTopicStats();

        // For v0 batches a zero starting offset is much faster but that will almost never happen.
        // For v2 batches we use starting offset = 0 as these batches are relative to the base
        // offset and measureValidation will mutate these batches between iterations
        startingOffset = messageVersion == 2 ? 0 : 42;

        if (bufferSupplierStr.equals("NO_CACHING")) {
            bufferSupplier = BufferSupplier.NO_CACHING;
        } else if (bufferSupplierStr.equals("CREATE")) {
            bufferSupplier = BufferSupplier.create();
        } else {
            throw new IllegalArgumentException("Unsupported buffer supplier " + bufferSupplierStr);
        }
        singleBatchBuffer = createBatch(1);

        batchBuffers = new ByteBuffer[batchCount];
        batchSizes = new int[batchCount];
        for (int i = 0; i < batchCount; ++i) {
            int size = random.nextInt(maxBatchSize) + 1;
            batchBuffers[i] = createBatch(size);
            batchSizes[i] = size;
        }
    }

    private ByteBuffer createBatch(int batchSize) {
        byte[] value = new byte[messageSize];
        final ByteBuffer buf = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytesUpperBound(messageVersion, compressionType, new byte[0], value,
                    Record.EMPTY_HEADERS) * batchSize
        );

        final MemoryRecordsBuilder builder =
            MemoryRecords.builder(buf, messageVersion, compressionType, TimestampType.CREATE_TIME, startingOffset);

        for (int i = 0; i < batchSize; ++i) {
            switch (bytes) {
                case ONES:
                    Arrays.fill(value, (byte) 1);
                    break;
                case RANDOM:
                    random.nextBytes(value);
                    break;
            }

            builder.append(0, null, value);
        }
        return builder.build().buffer();
    }

    @Benchmark
    public void measureValidation(Blackhole bh) throws IOException {
        MemoryRecords records = MemoryRecords.readableRecords(singleBatchBuffer.duplicate());
        LogValidator.validateMessagesAndAssignOffsetsCompressed(records, new TopicPartition("a", 0),
                new LongRef(startingOffset), Time.SYSTEM, System.currentTimeMillis(),
                CompressionCodec.getCompressionCodec(compressionType.id),
                CompressionCodec.getCompressionCodec(compressionType.id),
                false,  messageVersion, TimestampType.CREATE_TIME, Long.MAX_VALUE, 0,
                new AppendOrigin.Client$(),
                ApiVersion.latestVersion(),
                brokerTopicStats);
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
