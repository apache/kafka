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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.DataOutputStreamWritable;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.raft.RecordSerde;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BatchBuilder<T> {
    private final ByteBuffer initialBuffer;
    private final CompressionType compressionType;
    private final ByteBufferOutputStream batchOutput;
    private final DataOutputStreamWritable recordOutput;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isControlBatch;
    private final int leaderEpoch;
    private final int initialPosition;
    private final int maxBytes;
    private final RecordSerde<T> serde;
    private final List<T> records;

    private long nextOffset;
    private int unflushedBytes;

    public BatchBuilder(
        ByteBuffer buffer,
        RecordSerde<T> serde,
        CompressionType compressionType,
        long baseOffset,
        long logAppendTime,
        boolean isControlBatch,
        int leaderEpoch,
        int maxBytes
    ) {
        this.initialBuffer = buffer;
        this.batchOutput = new ByteBufferOutputStream(buffer);
        this.serde = serde;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.isControlBatch = isControlBatch;
        this.initialPosition = batchOutput.position();
        this.leaderEpoch = leaderEpoch;
        this.maxBytes = maxBytes;
        this.records = new ArrayList<>();

        int batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(
            RecordBatch.MAGIC_VALUE_V2, compressionType);
        batchOutput.position(initialPosition + batchHeaderSizeInBytes);

        this.recordOutput = new DataOutputStreamWritable(new DataOutputStream(
            compressionType.wrapForOutput(this.batchOutput, RecordBatch.MAGIC_VALUE_V2)));
    }

    public long appendRecord(T record, Object serdeContext) {
        if (nextOffset - baseOffset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot include more than " + Integer.MAX_VALUE +
                " records in a single batch");
        }

        long offset = nextOffset++;
        int recordSizeInBytes = writeRecord(offset, record, serdeContext);
        unflushedBytes += recordSizeInBytes;
        records.add(record);
        return offset;
    }

    public boolean hasRoomFor(int sizeInBytes) {
        if (nextOffset - baseOffset >= Integer.MAX_VALUE) {
            return false;
        }

        int recordSizeInBytes = DefaultRecord.sizeOfBodyInBytes(
            (int) (nextOffset - baseOffset),
            0,
            -1,
            sizeInBytes,
            DefaultRecord.EMPTY_HEADERS
        );

        int unusedSizeInBytes = maxBytes - approximateSizeInBytes();
        if (unusedSizeInBytes >= recordSizeInBytes) {
            return true;
        } else if (unflushedBytes > 0) {
            recordOutput.flush();
            unflushedBytes = 0;
            unusedSizeInBytes = maxBytes - flushedSizeInBytes();
            return unusedSizeInBytes >= recordSizeInBytes;
        } else {
            return false;
        }
    }

    private int flushedSizeInBytes() {
        return batchOutput.position() - initialPosition;
    }

    public int approximateSizeInBytes() {
        return flushedSizeInBytes() + unflushedBytes;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return nextOffset - 1;
    }

    public int numRecords() {
        return (int) (nextOffset - baseOffset);
    }

    public boolean nonEmpty() {
        return numRecords() > 0;
    }

    public ByteBuffer initialBuffer() {
        return initialBuffer;
    }

    public List<T> records() {
        return records;
    }

    private void writeDefaultBatchHeader() {
        ByteBuffer buffer = batchOutput.buffer();
        int lastPosition = buffer.position();

        buffer.position(initialPosition);
        int size = lastPosition - initialPosition;
        int lastOffsetDelta = (int) (lastOffset() - baseOffset);

        DefaultRecordBatch.writeHeader(
            buffer,
            baseOffset,
            lastOffsetDelta,
            size,
            RecordBatch.MAGIC_VALUE_V2,
            compressionType,
            TimestampType.CREATE_TIME,
            logAppendTime,
            logAppendTime,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false,
            isControlBatch,
            leaderEpoch,
            numRecords()
        );

        buffer.position(lastPosition);
    }

    public MemoryRecords build() {
        recordOutput.close();
        writeDefaultBatchHeader();
        ByteBuffer buffer = batchOutput.buffer().duplicate();
        buffer.flip();
        buffer.position(initialPosition);
        return MemoryRecords.readableRecords(buffer.slice());
    }

    private int writeRecord(
        long offset,
        T payload,
        Object serdeContext
    ) {
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = 0;

        int payloadSize = serde.recordSize(payload, serdeContext);
        int sizeInBytes = DefaultRecord.sizeOfBodyInBytes(
            offsetDelta,
            timestampDelta,
            -1,
            payloadSize,
            DefaultRecord.EMPTY_HEADERS
        );
        recordOutput.writeVarint(sizeInBytes);

        // Write attributes (currently unused)
        recordOutput.writeByte((byte) 0);

        // Write timestamp and offset
        recordOutput.writeVarlong(timestampDelta);
        recordOutput.writeVarint(offsetDelta);

        // Write key, which is always null for controller messages
        recordOutput.writeVarint(-1);

        // Write value
        recordOutput.writeVarint(payloadSize);
        serde.write(payload, serdeContext, recordOutput);

        // Write headers (currently unused)
        recordOutput.writeVarint(0);
        return ByteUtils.sizeOfVarint(sizeInBytes) + sizeInBytes;
    }

}
