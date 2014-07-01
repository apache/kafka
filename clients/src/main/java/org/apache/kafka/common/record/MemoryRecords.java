/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 */
public class MemoryRecords implements Records {

    private final Compressor compressor;
    private final int capacity;
    private final int sizeLimit;
    private ByteBuffer buffer;
    private boolean writable;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer, CompressionType type, boolean writable, int sizeLimit) {
        this.writable = writable;
        this.capacity = buffer.capacity();
        this.sizeLimit = sizeLimit;
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int capacity) {
        return new MemoryRecords(buffer, type, true, capacity);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords iterableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, buffer.capacity());
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     */
    public void append(long offset, byte[] key, byte[] value) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        int size = Record.recordSize(key, value);
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.putRecord(key, value);
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     * 
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     *
     * Also note that besides the records' capacity, there is also a size limit for the batch. This size limit may be
     * smaller than the capacity (e.g. when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size, but the size limit will still be the batch size), and when the records' size has
     * exceed this limit we also mark this record as full.
     */
    public boolean hasRoomFor(byte[] key, byte[] value) {
        return this.writable &&
            this.capacity >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value) &&
            this.sizeLimit >= this.compressor.estimatedBytesWritten();
    }

    public boolean isFull() {
        return !this.writable ||
            this.capacity <= this.compressor.estimatedBytesWritten() ||
            this.sizeLimit <= this.compressor.estimatedBytesWritten();
    }

    /**
     * Close this batch for no more appends
     */
    public void close() {
        compressor.close();
        writable = false;
        buffer = compressor.buffer();
    }

    /** Write the records in this set to the given channel */
    public int writeTo(GatheringByteChannel channel) throws IOException {
        return channel.write(buffer);
    }

    /**
     * The size of this record set
     */
    public int sizeInBytes() {
        return compressor.buffer().position();
    }
    
    /**
     * The compression rate of this record set
     */
    public double compressionRate() {
        if (compressor == null)
            return 1.0;
        else
            return compressor.compressionRate();
    }

    /**
     * Return the capacity of the buffer
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Get the byte buffer that backs this records instance
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        ByteBuffer copy = (ByteBuffer) this.buffer.duplicate().flip();
        return new RecordsIterator(copy, CompressionType.NONE, false);
    }

    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;
        private final DataInputStream stream;
        private final CompressionType type;
        private final boolean shallow;
        private RecordsIterator innerIter;

        public RecordsIterator(ByteBuffer buffer, CompressionType type, boolean shallow) {
            this.type = type;
            this.buffer = buffer;
            this.shallow = shallow;
            stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type);
        }

        /*
         * Read the next record from the buffer.
         * 
         * Note that in the compressed message set, each message value size is set as the size of the un-compressed
         * version of the message value, so when we do de-compression allocating an array of the specified size for
         * reading compressed value data is sufficient.
         */
        @Override
        protected LogEntry makeNext() {
            if (innerDone()) {
                try {
                    // read the offset
                    long offset = stream.readLong();
                    // read record size
                    int size = stream.readInt();
                    if (size < 0)
                        throw new IllegalStateException("Record with size " + size);
                    // read the record, if compression is used we cannot depend on size
                    // and hence has to do extra copy
                    ByteBuffer rec;
                    if (type == CompressionType.NONE) {
                        rec = buffer.slice();
                        buffer.position(buffer.position() + size);
                        rec.limit(size);
                    } else {
                        byte[] recordBuffer = new byte[size];
                        stream.read(recordBuffer, 0, size);
                        rec = ByteBuffer.wrap(recordBuffer);
                    }
                    LogEntry entry = new LogEntry(offset, new Record(rec));
                    entry.record().ensureValid();

                    // decide whether to go shallow or deep iteration if it is compressed
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        return entry;
                    } else {
                        // init the inner iterator with the value payload of the message,
                        // which will de-compress the payload to a set of messages
                        ByteBuffer value = entry.record().value();
                        innerIter = new RecordsIterator(value, compression, true);
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private boolean innerDone() {
            return (innerIter == null || !innerIter.hasNext());
        }
    }
}
