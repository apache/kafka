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
package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerSubscriptionState;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.FetchMetricsRegistry;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@code CompletedFetch#fetchRecords}, the per-record path that turns fetched record batches into
 * {@link ConsumerRecord}s on the consumer. {@code CompletedFetch} is package-private, so it is created and
 * invoked through reflection resolved once in {@link #setup()}.
 */
@State(Scope.Benchmark)
@Fork(value = 3)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CompletedFetchBenchmark {

    private static final String INTERNALS = "org.apache.kafka.clients.consumer.internals.";
    private static final TopicPartition TP = new TopicPartition("topic", 0);

    @Param({"500"})
    private int numRecords;

    @Param({"100"})
    private int recordsPerBatch;

    @Param({"5", "1000"})
    private int partitionLeaderEpoch;

    @Param({"0"})
    private int numHeaders;

    private FetchResponseData.PartitionData partitionData;
    private FetchConfig fetchConfig;
    private Deserializers<ByteBuffer, ByteBuffer> deserializers;
    private Logger logger;
    private ConsumerSubscriptionState subscriptions;
    private BufferSupplier bufferSupplier;
    private Object metricAggregator;
    private MethodHandle newCompletedFetch;
    private MethodHandle fetchRecords;

    @Setup
    public void setup() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(numRecords * (64 + numHeaders * 32) + 1024);
        byte[] key = new byte[8];
        byte[] value = new byte[32];
        Header[] headers = Record.EMPTY_HEADERS;
        if (numHeaders > 0) {
            headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++)
                headers[i] = new RecordHeader("header-" + i, new byte[8]);
        }
        for (int offset = 0; offset < numRecords; offset += recordsPerBatch) {
            try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                    Compression.NONE, TimestampType.CREATE_TIME, offset, RecordBatch.NO_TIMESTAMP, partitionLeaderEpoch)) {
                for (int i = 0; i < recordsPerBatch; i++)
                    builder.append(0L, key, value, headers);
                builder.build();
            }
        }
        buffer.flip();
        partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(TP.partition())
                .setHighWatermark(numRecords)
                .setRecords(MemoryRecords.readableRecords(buffer));

        fetchConfig = new FetchConfig(
                ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                numRecords,
                false,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);
        deserializers = new Deserializers<>(new ByteBufferDeserializer(), new ByteBufferDeserializer(), null);

        LogContext logContext = new LogContext();
        logger = logContext.logger(CompletedFetchBenchmark.class);
        subscriptions = new ConsumerSubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        bufferSupplier = BufferSupplier.create();

        FetchMetricsManager metricsManager = new FetchMetricsManager(new Metrics(), new FetchMetricsRegistry());
        Class<?> aggregatorClass = Class.forName(INTERNALS + "FetchMetricsAggregator");
        Constructor<?> aggregatorCtor = aggregatorClass.getDeclaredConstructor(FetchMetricsManager.class, Set.class);
        aggregatorCtor.setAccessible(true);
        metricAggregator = aggregatorCtor.newInstance(metricsManager, Set.of(TP));

        Class<?> completedFetchClass = Class.forName(INTERNALS + "CompletedFetch");
        Constructor<?> ctor = completedFetchClass.getDeclaredConstructor(Logger.class, ConsumerSubscriptionState.class,
                BufferSupplier.class, TopicPartition.class, FetchResponseData.PartitionData.class, aggregatorClass, Long.class);
        ctor.setAccessible(true);
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        newCompletedFetch = lookup.unreflectConstructor(ctor).asType(MethodType.methodType(Object.class,
                Logger.class, ConsumerSubscriptionState.class, BufferSupplier.class, TopicPartition.class,
                FetchResponseData.PartitionData.class, Object.class, Long.class));

        Method method = completedFetchClass.getDeclaredMethod("fetchRecords", FetchConfig.class, Deserializers.class, int.class);
        method.setAccessible(true);
        fetchRecords = lookup.unreflect(method).asType(MethodType.methodType(List.class,
                Object.class, FetchConfig.class, Deserializers.class, int.class));
    }

    @Benchmark
    public List<ConsumerRecord<ByteBuffer, ByteBuffer>> fetchRecords() throws Throwable {
        Object completedFetch = newCompletedFetch.invokeExact(logger, subscriptions, bufferSupplier, TP,
                partitionData, metricAggregator, (Long) 0L);
        @SuppressWarnings("unchecked")
        List<ConsumerRecord<ByteBuffer, ByteBuffer>> records =
                (List<ConsumerRecord<ByteBuffer, ByteBuffer>>) fetchRecords.invokeExact(completedFetch, fetchConfig,
                        deserializers, numRecords);
        if (records.size() != numRecords)
            throw new IllegalStateException("Expected " + numRecords + " records but got " + records.size());
        return records;
    }
}
