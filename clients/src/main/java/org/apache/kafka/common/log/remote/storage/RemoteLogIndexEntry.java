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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Entry representation in a remote log index
 */
public class RemoteLogIndexEntry {

    private static final Logger log = LoggerFactory.getLogger(RemoteLogIndexEntry.class);

    public final short magic;
    public final int crc;
    public final long firstOffset;
    public final long lastOffset;
    public final long firstTimeStamp;
    public final long lastTimeStamp;
    public final int dataLength;
    public final byte[] rdi;

    /**
     * @param magic          magic version of protocol
     * @param crc            checksum value of the entry
     * @param firstOffset    offset value of the first record for this entry stored at respective { @link #rdi}
     * @param lastOffset     offset value of the last record for this entry stored at respective { @link #rdi}
     * @param firstTimeStamp timestamp value of the first record for this entry stored at respective { @link #rdi}
     * @param lastTimeStamp  timestamp value of the last record for this entry stored at respective { @link #rdi}
     * @param dataLength     length of the data stored in remote tier at rdi.
     * @param rdi            bytes value of rdi.
     */
    public RemoteLogIndexEntry(short magic, int crc, long firstOffset, long lastOffset,
                               long firstTimeStamp, long lastTimeStamp, int dataLength,
                               byte[] rdi) {
        this.magic = magic;
        this.crc = crc;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.firstTimeStamp = firstTimeStamp;
        this.lastTimeStamp = lastTimeStamp;
        this.dataLength = dataLength;
        this.rdi = rdi;
    }

    /**
     * @return bytes length of this entry value
     */
    public short entryLength() {
        return (short) (4 // crc - int
                + 8 // firstOffset - long
                + 8 // lastOffset - long
                + 8 // firstTimestamp - long
                + 8 // lastTimestamp - long
                + 4 // dataLength - int
                + 2 // rdiLength - short
                + rdi.length);
    }

    public short totalLength = (short) (entryLength() + 4);


    public ByteBuffer asBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(entryLength() + 2 + 2);
        buffer.putShort(magic);
        buffer.putShort(entryLength());
        buffer.putInt(crc);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(firstTimeStamp);
        buffer.putLong(lastTimeStamp);
        buffer.putInt(dataLength);
        buffer.putShort((short) rdi.length);
        buffer.put(rdi);
        buffer.flip();
        return buffer;
    }


    public static RemoteLogIndexEntry create(long firstOffset, long lastOffset, long firstTimeStamp, long lastTimeStamp,
                                             int dataLength, byte[] rdi) {

        int length = 8 // firstOffset - long
                + 8 // lastOffset - long
                + 8 // firstTimestamp - long
                + 8 // lastTimestamp - long
                + 4 // dataLength - int
                + 2 // rdiLength - short
                + rdi.length;

        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(firstTimeStamp);
        buffer.putLong(lastTimeStamp);
        buffer.putInt(dataLength);
        buffer.putShort((short) rdi.length);
        buffer.put(rdi);
        buffer.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        int crc = (int) crc32.getValue();

        return new RemoteLogIndexEntry((short) 0, crc, firstOffset, lastOffset, firstTimeStamp, lastTimeStamp, dataLength, rdi);
    }

    public static List<RemoteLogIndexEntry> readAll(InputStream is) throws IOException {
        List<RemoteLogIndexEntry> index = new ArrayList<>();

        boolean done = false;
        while (!done) {
            Optional<RemoteLogIndexEntry> entry = parseEntry(is);
            if (entry.isPresent()) {
                index.add(entry.get());
            } else {
                done = true;
            }
        }

        return index;
    }

    private static Optional<RemoteLogIndexEntry> parseEntry(InputStream is) throws IOException {
        ByteBuffer magicAndEntryLengthBuffer = ByteBuffer.allocate(java.lang.Short.BYTES + java.lang.Short.BYTES);
        Utils.readFully(is, magicAndEntryLengthBuffer);
        if (magicAndEntryLengthBuffer.hasRemaining()) {
            return Optional.empty();
        } else {
            magicAndEntryLengthBuffer.flip();
            short magic = magicAndEntryLengthBuffer.getShort();
            if (magic == 0) {
                short entryLength = magicAndEntryLengthBuffer.getShort();
                ByteBuffer valueBuffer = ByteBuffer.allocate(entryLength);
                Utils.readFully(is, valueBuffer);
                valueBuffer.flip();

                try {
                    int crc = valueBuffer.getInt();
                    long firstOffset = valueBuffer.getLong();
                    long lastOffset = valueBuffer.getLong();
                    long firstTimestamp = valueBuffer.getLong();
                    long lastTimestamp = valueBuffer.getLong();
                    int dataLength = valueBuffer.getInt();
                    short rdiLength = valueBuffer.getShort();
                    byte[] rdi = new byte[rdiLength];
                    valueBuffer.get(rdi);
                    return Optional.of(new RemoteLogIndexEntry(magic, crc, firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength, rdi));
                } catch (BufferUnderflowException ex) {
                    // TODO decide how to deal with incomplete records
                    throw new KafkaException(ex);
                }
            } else {
                // TODO: Throw Custom Exceptions?
                throw new RuntimeException("magic version " + magic + " is not supported");
            }
        }
    }

    public static Optional<RemoteLogIndexEntry> parseEntry(FileChannel ch, long position) throws IOException {
        ByteBuffer magicBuffer = ByteBuffer.allocate(2);
        // The last 8 bytes of remote log index file is the "last offset" rather than a regular index entry
        if (position + 8 >= ch.size())
            return Optional.empty();
        int readCt = ch.read(magicBuffer, position);
        if (readCt > 0) {
            magicBuffer.flip();
            short magic = magicBuffer.getShort();
            if (magic == 0) {
                ByteBuffer valBuffer = ByteBuffer.allocate(4);
                long nextPos = position + 2;
                ch.read(valBuffer, nextPos);
                valBuffer.flip();
                short length = valBuffer.getShort();

                ByteBuffer valueBuffer = ByteBuffer.allocate(length);
                ch.read(valueBuffer, nextPos + 2);
                valueBuffer.flip();

                int crc = valueBuffer.getInt();
                long firstOffset = valueBuffer.getLong();
                long lastOffset = valueBuffer.getLong();
                long firstTimestamp = valueBuffer.getLong();
                long lastTimestamp = valueBuffer.getLong();
                int dataLength = valueBuffer.getInt();
                short rdiLength = valueBuffer.getShort();
                ByteBuffer rdiBuffer = ByteBuffer.allocate(rdiLength);
                valueBuffer.get(rdiBuffer.array());
                return Optional.of(new RemoteLogIndexEntry(magic, crc, firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength,
                        rdiBuffer.array()));
            } else {
                // TODO: Throw Custom Exceptions?
                throw new RuntimeException("magic version " + magic + " is not supported");
            }
        } else {
            log.debug("Reached limit of the file for channel:$ch for position: {}", position);
            return Optional.empty();
        }
    }

    public static List<RemoteLogIndexEntry> readAll(FileChannel ch) throws IOException {
        List<RemoteLogIndexEntry> index = new ArrayList<>();

        long pos = 0L;
        while (pos < ch.size()) {
            Optional<RemoteLogIndexEntry> entryOption = parseEntry(ch, pos);
            final long x = pos;
            RemoteLogIndexEntry entry = entryOption.orElseThrow(() -> new KafkaException("Entry could not be found at position: " + x));
            index.add(entry);
            pos += entry.entryLength() + 4;
        }

        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogIndexEntry that = (RemoteLogIndexEntry) o;
        return magic == that.magic &&
                crc == that.crc &&
                firstOffset == that.firstOffset &&
                lastOffset == that.lastOffset &&
                firstTimeStamp == that.firstTimeStamp &&
                lastTimeStamp == that.lastTimeStamp &&
                dataLength == that.dataLength &&
                totalLength == that.totalLength &&
                Arrays.equals(rdi, that.rdi);
    }

    @Override
    public int hashCode() {
        return Objects.hash(crc);
    }
}