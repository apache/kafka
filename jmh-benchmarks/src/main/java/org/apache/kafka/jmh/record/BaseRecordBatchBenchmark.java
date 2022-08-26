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

import kafka.server.BrokerTopicStats;
import kafka.server.RequestLocal;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE;

@State(Scope.Benchmark)
public abstract class BaseRecordBatchBenchmark {
    private static final int MAX_HEADER_SIZE = 5;
    private static final int HEADER_KEY_SIZE = 30;

    private final Random random = new Random(0);

    final int batchCount = 100;

    public enum Bytes {
        RANDOM, ONES
    }

    @Param(value = {"1", "2", "10", "50", "200", "500"})
    private int maxBatchSize = 200;

    @Param(value = {"1", "2"})
    byte messageVersion = CURRENT_MAGIC_VALUE;

    @Param(value = {"100", "1000", "10000", "100000"})
    private int messageSize = 1000;

    @Param(value = {"RANDOM", "ONES"})
    private Bytes bytes = Bytes.RANDOM;

    @Param(value = {"NO_CACHING", "CREATE"})
    private String bufferSupplierStr = "NO_CACHING";

    // zero starting offset is much faster for v1 batches, but that will almost never happen
    int startingOffset;

    // Used by measureSingleMessage
    ByteBuffer singleBatchBuffer;

    // Used by measureVariableBatchSize
    ByteBuffer[] batchBuffers;
    RequestLocal requestLocal;
    final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();

    @Setup
    public void init() {
        // For v0 batches a zero starting offset is much faster but that will almost never happen.
        // For v2 batches we use starting offset = 0 as these batches are relative to the base
        // offset and measureValidation will mutate these batches between iterations
        startingOffset = messageVersion == 2 ? 0 : 42;

        if (bufferSupplierStr.equals("NO_CACHING")) {
            requestLocal = RequestLocal.NoCaching();
        } else if (bufferSupplierStr.equals("CREATE")) {
            requestLocal = RequestLocal.withThreadConfinedCaching();
        } else {
            throw new IllegalArgumentException("Unsupported buffer supplier " + bufferSupplierStr);
        }
        singleBatchBuffer = createBatch(1);

        batchBuffers = new ByteBuffer[batchCount];
        for (int i = 0; i < batchCount; ++i) {
            int size = random.nextInt(maxBatchSize) + 1;
            batchBuffers[i] = createBatch(size);
        }
    }

    private static Header[] createHeaders() {
        char[] headerChars = new char[HEADER_KEY_SIZE];
        Arrays.fill(headerChars, 'a');
        String headerKey = new String(headerChars);
        byte[] headerValue = new byte[0];
        return IntStream.range(0, MAX_HEADER_SIZE).mapToObj(index -> new Header() {
            @Override
            public String key() {
                return headerKey;
            }

            @Override
            public byte[] value() {
                return headerValue;
            }
        }).toArray(Header[]::new);
    }

    abstract CompressionType compressionType();

    private ByteBuffer createBatch(int batchSize) {
        // Magic v1 does not support record headers
        Header[] headers = messageVersion < RecordBatch.MAGIC_VALUE_V2 ? Record.EMPTY_HEADERS : createHeaders();
        byte[] value = new byte[messageSize];
        final ByteBuffer buf = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytesUpperBound(messageVersion, compressionType(), new byte[0], value,
                    headers) * batchSize
        );

        final MemoryRecordsBuilder builder =
            MemoryRecords.builder(buf, messageVersion, compressionType(), TimestampType.CREATE_TIME, startingOffset);

        for (int i = 0; i < batchSize; ++i) {
            switch (bytes) {
                case ONES:
                    Arrays.fill(value, (byte) 1);
                    break;
                case RANDOM:
                    random.nextBytes(value);
                    break;
            }

            builder.append(0, null, value, headers);
        }
        return builder.build().buffer();
    }
}
