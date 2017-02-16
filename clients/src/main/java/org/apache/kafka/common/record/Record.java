/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * A record: a serialized key and value along with the associated CRC and other fields
 */
public final class Record {

    /**
     * The current offset and size for all the fixed-length fields
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTE_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * The size for the record header
     */
    public static final int HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH;

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The "magic" values
     */
    public static final byte MAGIC_VALUE_V0 = 0;
    public static final byte MAGIC_VALUE_V1 = 1;

    /**
     * The current "magic" value
     */
    public static final byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V1;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    public static final int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Specify the mask of timestamp type.
     * 0 for CreateTime, 1 for LogAppendTime.
     */
    public static final byte TIMESTAMP_TYPE_MASK = 0x08;
    public static final int TIMESTAMP_TYPE_ATTRIBUTE_OFFSET = 3;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public Record(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    public Record(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return Utils.computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this record
     */
    public long checksum() {
        return Utils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return sizeInBytes() >= CRC_LENGTH && checksum() == computeChecksum();
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
        if (!isValid()) {
            if (sizeInBytes() < CRC_LENGTH)
                throw new InvalidRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                        + "small, size = " + sizeInBytes() + ")");
            else
                throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                        + ", computed crc = " + computeChecksum() + ")");
        }
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
        if (magic() == MAGIC_VALUE_V0)
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
        if (magic() == MAGIC_VALUE_V0)
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
        if (magic() == MAGIC_VALUE_V0)
            return NO_TIMESTAMP;
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
        if (magic() == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else
            return wrapperRecordTimestampType == null ? TimestampType.forAttributes(attributes()) : wrapperRecordTimestampType;
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
        if (magic() == MAGIC_VALUE_V0)
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
            return String.format("Record(magic = %d, attributes = %d, compression = %s, crc = %d, %s = %d, key = %d bytes, value = %d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 timestampType(),
                                 timestamp(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        else
            return String.format("Record(magic = %d, attributes = %d, compression = %s, crc = %d, key = %d bytes, value = %d bytes)",
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
        if (!other.getClass().equals(Record.class))
            return false;
        Record record = (Record) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    /**
     * Get the size of this record if converted to the given format.
     *
     * @param toMagic The target magic version to convert to
     * @return The size in bytes after conversion
     */
    public int convertedSize(byte toMagic) {
        return recordSize(toMagic, Math.max(0, keySize()), Math.max(0, valueSize()));
    }

    /**
     * Convert this record to another message format.
     *
     * @param toMagic The target magic version to convert to
     * @param upconvertTimestampType The timestamp type to use if up-converting from magic 0, ignored if
     *                               down-converting or if no conversion is needed
     * @return A new record instance with a freshly allocated ByteBuffer.
     */
    public Record convert(byte toMagic, TimestampType upconvertTimestampType) {
        byte magic = magic();
        if (toMagic == magic)
            return this;

        final TimestampType timestampType;
        if (magic == Record.MAGIC_VALUE_V0) {
            if (upconvertTimestampType == TimestampType.NO_TIMESTAMP_TYPE)
                throw new IllegalArgumentException("Cannot up-convert using timestamp type " + upconvertTimestampType);
            timestampType = upconvertTimestampType;
        } else {
            timestampType = timestampType();
        }

        ByteBuffer buffer = ByteBuffer.allocate(convertedSize(toMagic));
        convertTo(buffer, toMagic, timestamp(), timestampType);
        buffer.rewind();
        return new Record(buffer);
    }

    private void convertTo(ByteBuffer buffer, byte toMagic, long timestamp, TimestampType timestampType) {
        if (compressionType() != CompressionType.NONE)
            throw new IllegalArgumentException("Cannot use convertTo for deep conversion");

        write(buffer, toMagic, timestamp, key(), value(), CompressionType.NONE, timestampType);
    }

    /**
     * Convert this record to another message format and write the converted data to the provided outputs stream.
     *
     * @param out The output stream to write the converted data to
     * @param toMagic The target magic version for conversion
     * @param timestamp The timestamp to use in the converted record (for up-conversion)
     * @param timestampType The timestamp type to use in the converted record (for up-conversion)
     * @throws IOException for any IO errors writing the converted record.
     */
    public void convertTo(DataOutputStream out, byte toMagic, long timestamp, TimestampType timestampType) throws IOException {
        if (compressionType() != CompressionType.NONE)
            throw new IllegalArgumentException("Cannot use convertTo for deep conversion");

        write(out, toMagic, timestamp, key(), value(), CompressionType.NONE, timestampType);
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
    public static Record create(byte magic,
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
        return new Record(buffer);
    }

    public static Record create(long timestamp, byte[] key, byte[] value) {
        return create(CURRENT_MAGIC_VALUE, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, TimestampType timestampType, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, timestampType);
    }

    public static Record create(byte magic, long timestamp, byte[] value) {
        return create(magic, timestamp, null, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, byte[] key, byte[] value) {
        return create(magic, NO_TIMESTAMP, key, value);
    }

    public static Record create(byte[] key, byte[] value) {
        return create(NO_TIMESTAMP, key, value);
    }

    public static Record create(byte[] value) {
        return create(CURRENT_MAGIC_VALUE, NO_TIMESTAMP, null, value, CompressionType.NONE, TimestampType.CREATE_TIME);
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

        // now fill in the value size
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // compute and fill the crc from the beginning of the message
        long crc = Utils.computeChecksum(buffer, recordPosition + MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        Utils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try {
            ByteBufferOutputStream out = new ByteBufferOutputStream(buffer);
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

    private static long write(DataOutputStream out,
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
        if (magic != MAGIC_VALUE_V0 && magic != MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (timestamp < 0 && timestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        // write crc
        out.writeInt((int) (crc & 0xffffffffL));
        // write magic value
        out.writeByte(magic);
        // write attributes
        out.writeByte(attributes);

        // maybe write timestamp
        if (magic > 0)
            out.writeLong(timestamp);

        // write the key
        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            out.write(key.array(), key.arrayOffset(), size);
        }
        // write the value
        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            out.write(value.array(), value.arrayOffset(), size);
        }
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(CURRENT_MAGIC_VALUE, key, value);
    }

    public static int recordSize(byte magic, byte[] key, byte[] value) {
        return recordSize(magic, key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    private static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    // visible only for testing
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        if (magic > 0)
            return timestampType.updateAttributes(attributes);
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
        if (magic > 0)
            crc.updateLong(timestamp);
        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            int size = key.remaining();
            crc.updateInt(size);
            crc.update(key.array(), key.arrayOffset(), size);
        }
        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = value.remaining();
            crc.updateInt(size);
            crc.update(value.array(), value.arrayOffset(), size);
        }
        return crc.getValue();
    }

    public static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        return RECORD_OVERHEAD_V1;
    }

    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        return KEY_OFFSET_V1;
    }

}
