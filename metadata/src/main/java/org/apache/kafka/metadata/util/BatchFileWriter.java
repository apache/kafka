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

package org.apache.kafka.metadata.util;

import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.kafka.raft.KafkaRaftClient.MAX_BATCH_SIZE_BYTES;


/**
 * Write an arbitrary set of metadata records into a Kafka metadata log batch format.
 *
 * This is similar to the binary format used for metadata snapshot files, but the log epoch
 * and initial offset are set to zero. This type includes a SnapshotHeaderRecord record in the
 * first batch and a SnapshotFooterRecord record in the last batch.
 */
public class BatchFileWriter implements AutoCloseable {
    private final FileChannel channel;
    private final BatchAccumulator<ApiMessageAndVersion> batchAccumulator;
    private final Time time;

    private BatchFileWriter(
        FileChannel channel,
        BatchAccumulator<ApiMessageAndVersion> batchAccumulator,
        Time time
    ) {
        this.channel = channel;
        this.batchAccumulator = batchAccumulator;
        this.time = time;
    }

    public void append(ApiMessageAndVersion apiMessageAndVersion) {
        batchAccumulator.append(0, Collections.singletonList(apiMessageAndVersion), OptionalLong.empty(), false);
    }

    public void append(List<ApiMessageAndVersion> messageBatch) {
        batchAccumulator.append(0, messageBatch, OptionalLong.empty(), false);
    }

    public void close() throws IOException {
        // Append the footer before draining the batch accumulator and force it to create a batch
        batchAccumulator.appendSnapshotFooterRecord(
            new SnapshotFooterRecord()
                .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION),
            time.milliseconds()
        );
        batchAccumulator.forceDrain();

        try {
            for (BatchAccumulator.CompletedBatch<ApiMessageAndVersion> batch : batchAccumulator.drain()) {
                Utils.writeFully(channel, batch.data.buffer());
            }
        } finally {
            batchAccumulator.close();
            channel.close();
        }
    }

    public static BatchFileWriter open(Path snapshotPath) throws IOException {
        Time time = Time.SYSTEM;
        BatchAccumulator<ApiMessageAndVersion> batchAccumulator = new BatchAccumulator<>(
            0,
            0,
            Integer.MAX_VALUE,
            MAX_BATCH_SIZE_BYTES,
            new BatchMemoryPool(5, MAX_BATCH_SIZE_BYTES),
            time,
            CompressionType.NONE,
            new MetadataRecordSerde()
        );

        // Append the snapshot header control record and force it to create a batch
        batchAccumulator.appendSnapshotHeaderRecord(
            new SnapshotHeaderRecord()
                .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                .setLastContainedLogTimestamp(0),
            time.milliseconds()
        );
        batchAccumulator.forceDrain();

        FileChannel channel = FileChannel.open(
            snapshotPath,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
        );

        return new BatchFileWriter(channel, batchAccumulator, time);
    }
}
