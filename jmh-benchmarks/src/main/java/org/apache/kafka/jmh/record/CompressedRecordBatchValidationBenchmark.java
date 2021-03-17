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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class CompressedRecordBatchValidationBenchmark extends BaseRecordBatchBenchmark {

    @Param(value = {"LZ4", "SNAPPY", "GZIP", "ZSTD"})
    private CompressionType compressionType = CompressionType.LZ4;

    @Override
    CompressionType compressionType() {
        return compressionType;
    }

    @Benchmark
    public void measureValidateMessagesAndAssignOffsetsCompressed(Blackhole bh) {
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
}
