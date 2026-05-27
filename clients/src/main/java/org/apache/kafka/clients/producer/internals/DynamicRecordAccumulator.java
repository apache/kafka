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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

public class DynamicRecordAccumulator extends RecordAccumulator {

    private final int chunkSize;
    private final ChunkedBufferPool free;

    public DynamicRecordAccumulator(LogContext logContext,
                                    int batchSize,
                                    int chunkSize,
                                    Compression compression,
                                    int lingerMs,
                                    long retryBackoffMs,
                                    long retryBackoffMaxMs,
                                    int deliveryTimeoutMs,
                                    PartitionerConfig partitionerConfig,
                                    Metrics metrics,
                                    String metricGrpName,
                                    Time time,
                                    TransactionManager transactionManager,
                                    ChunkedBufferPool bufferPool) {
        super(logContext, batchSize, compression, lingerMs, retryBackoffMs, retryBackoffMaxMs, deliveryTimeoutMs,
                partitionerConfig, metrics, metricGrpName, time, transactionManager, bufferPool);
        this.chunkSize = chunkSize;
        this.free = bufferPool;
    }

    @Override
    protected ProducerBatch newProducerBatch(TopicPartition topicPartition, long nowMs, MemoryRecordsBuilder recordsBuilder) {
        return new CompositeProducerBatch(topicPartition, recordsBuilder, nowMs);
    }
}
