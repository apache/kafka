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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Checksums;
import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class represents the serialized key and value along with the associated CRC and other fields
 * of message format versions 0 and 1. Note that it is uncommon to need to access this class directly.
 * Usually it should be accessed indirectly through the {@link Record} interface which is exposed
 * through the {@link Records} object.
 */
public final class LegacyRecord {

    /**
     * The current offset and size for all the fixed-length fields
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTES_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * The size for the record header
     */
    public static final int HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;
    public static final int HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH;

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    private static final int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Specify the mask of timestamp type: 0 for CreateTime, 1 for LogAppendTime.
     */
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public LegacyRecord(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    public LegacyRecord(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return Crc32.crc32(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this record
     */
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum();
    }

    public Long wrapperRecordTimestamp() {
        return wrapperRecordTimestamp;
    }

    public TimestampType wrapperRecordTimestampType() {
        return wrapperRecordTimestampType;
    }

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0)
            throw new CorruptRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = " + sizeInBytes() + ")");

        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc), but
     * excluding the log overhead (offset and record size).
     * @return the size in bytes
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     * @return the size in bytes of the key (0 if the key is null)
     */
    public int keySize() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        else
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
    }

    /**
     * Does the record have a key?
     * @return true if so, false otherwise
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        else
            return KEY_OFFSET_V1 + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     * @return the size in bytes of the value (0 if the value is null)
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * Check whether the value field of this record is null.
     * @return true if the value is null, false otherwise
     */
    public boolean hasNullValue() {
        return valueSize() < 0;
    }

    /**
     * The magic value (i.e. message format version) of this record
     * @return the magic value
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this record
     * @return the attributes
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * When magic value is greater than 0, the timestamp of a record is determined in the following way:
     * 1. wrapperRecordTimestampType = null and wrapperRecordTimestamp is null - Uncompressed message, timestamp is in the message.
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME and WrapperRecordTimestamp is not null - Compressed message using LOG_APPEND_TIME
     * 3. wrapperRecordTimestampType = CREATE_TIME and wrapperRecordTimestamp is not null - Compressed message using CREATE_TIME
     *
     * @return the timestamp as determined above
     */
    public long timestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return RecordBatch.NO_TIMESTAMP;
        else {
            // case 2
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null)
                return wrapperRecordTimestamp;
            // Case 1, 3
            else
                return buffer.getLong(TIMESTAMP_OFFSET);
        }
    }

    /**
     * Get the timestamp type of the record.
     *
     * @return The timestamp type or {@link TimestampType#NO_TIMESTAMP_TYPE} if the magic is 0.
     */
    public TimestampType timestampType() {
        return timestampType(magic(), wrapperRecordTimestampType, attributes());
    }

    /**
     * The compression type used with this record
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the value of this record
     * @return the value or null if the value for this record is null
     */
    public ByteBuffer value() {
        return Utils.sizeDelimited(buffer, valueSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     * @return the buffer or null if the key for this record is null
     */
    public ByteBuffer key() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V0);
        else
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V1);
    }

    /**
     * Get the underlying buffer backing this record instance.
     *
     * @return the buffer
     */
    public ByteBuffer buffer() {
        return this.buffer;
    }

    public String toString() {
        if (magic() > 0)
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 timestampType(),
                                 timestamp(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        else
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(LegacyRecord.class))
            return false;
        LegacyRecord record = (LegacyRecord) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    /**
     * Create a new record instance. If the record's compression type is not none, then
     * its value payload should be already compressed with the specified type; the constructor
     * would always write the value payload as is and will not do the compression itself.
     *
     * @param magic The magic value to use
     * @param timestamp The timestamp of the record
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param compressionType The compression type used on the contents of the record (if any)
     * @param timestampType The timestamp type to be used for this record
     */
    public static LegacyRecord create(byte magic,
                                      long timestamp,
                                      byte[] key,
                                      byte[] value,
                                      CompressionType compressionType,
                                      TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new LegacyRecord(buffer);
    }

    public static LegacyRecord create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    /**
     * Write the header for a compressed record set in-place (i.e. assuming the compressed record data has already
     * been written at the value offset in a wrapped record). This lets you dynamically create a compressed message
     * set, and then go back later and fill in its size and CRC, which saves the need for copying to another buffer.
     *
     * @param buffer The buffer containing the compressed record data positioned at the first offset of the
     * @param magic The magic value of the record set
     * @param recordSize The size of the record (including record overhead)
     * @param timestamp The timestamp of the wrapper record
     * @param compressionType The compression type used
     * @param timestampType The timestamp type of the wrapper record
     */
    public static void writeCompressedRecordHeader(ByteBuffer buffer,
                                                   byte magic,
                                                   int recordSize,
                                                   long timestamp,
                                                   CompressionType compressionType,
                                                   TimestampType timestampType) {
        int recordPosition = buffer.position();
        int valueSize = recordSize - recordOverhead(magic);

        // write the record header with a null value (the key is always null for the wrapper)
        write(buffer, magic, timestamp, null, null, compressionType, timestampType);
        buffer.position(recordPosition);

        // now fill in the value size
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // compute and fill the crc from the beginning of the message
        long crc = Crc32.crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try {
            DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Write the record data with the given compression type and return the computed crc.
     *
     * @param out The output stream to write to
     * @param magic The magic value to be used
     * @param timestamp The timestamp of the record
     * @param key The record key
     * @param value The record value
     * @param compressionType The compression type
     * @param timestampType The timestamp type
     * @return the computed CRC for this record.
     * @throws IOException for any IO errors writing to the output stream.
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             byte[] key,
                             byte[] value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             ByteBuffer key,
                             ByteBuffer value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }

    /**
     * Write a record using raw fields (without validation). This should only be used in testing.
     */
    public static void write(DataOutputStream out,
                             byte magic,
                             long crc,
                             byte attributes,
                             long timestamp,
                             byte[] key,
                             byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    private static void write(DataOutputStream out,
                              byte magic,
                              long crc,
                              byte attributes,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value) throws IOException {
        if (magic != RecordBatch.MAGIC_VALUE_V0 && magic != RecordBatch.MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        // write crc
        out.writeInt((int) (crc & 0xffffffffL));
        // write magic value
        out.writeByte(magic);
        // write attributes
        out.writeByte(attributes);

        // maybe write timestamp
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            out.writeLong(timestamp);

        // write the key
        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            Utils.writeTo(out, key, size);
        }
        // write the value
        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            Utils.writeTo(out, value, size);
        }
    }

    static int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
        return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
    }

    public static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    // visible only for testing
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0)
            attributes |= COMPRESSION_CODEC_MASK & type.id;
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
                throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for " +
                        "message format v1");
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                attributes |= TIMESTAMP_TYPE_MASK;
        }
        return attributes;
    }

    // visible only for testing
    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        Crc32 crc = new Crc32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            Checksums.updateLong(crc, timestamp);
        // update for the key
        if (key == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = key.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, key, size);
        }
        // update for the value
        if (value == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = value.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, value, size);
        }
        return crc.getValue();
    }

    static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        else if (magic == 1)
            return RECORD_OVERHEAD_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    static int headerSize(byte magic) {
        if (magic == 0)
            return HEADER_SIZE_V0;
        else if (magic == 1)
            return HEADER_SIZE_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        else if (magic == 1)
            return KEY_OFFSET_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    public static TimestampType timestampType(byte magic, TimestampType wrapperRecordTimestampType, byte attributes) {
        if (magic == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else if (wrapperRecordTimestampType != null)
            return wrapperRecordTimestampType;
        else
            return (attributes & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

}
