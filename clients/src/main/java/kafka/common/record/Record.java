package kafka.common.record;

import java.nio.ByteBuffer;

import kafka.common.utils.Utils;

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
    public static final int KEY_SIZE_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET = KEY_SIZE_OFFSET + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    /** The amount of overhead bytes in a record */
    public static final int RECORD_OVERHEAD = KEY_OFFSET + VALUE_SIZE_LENGTH;

    /**
     * The minimum valid size for the record header
     */
    public static final int MIN_HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The current "magic" value
     */
    public static final byte CURRENT_MAGIC_VALUE = 0;

    /**
     * Specifies the mask for the compression code. 2 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    public static final int COMPRESSION_CODEC_MASK = 0x03;

    /**
     * Compression code for uncompressed records
     */
    public static final int NO_COMPRESSION = 0;

    private final ByteBuffer buffer;

    public Record(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * A constructor to create a LogRecord
     * 
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param codec The compression codec used on the contents of the record (if any)
     * @param valueOffset The offset into the payload array used to extract payload
     * @param valueSize The size of the payload to use
     */
    public Record(byte[] key, byte[] value, CompressionType codec, int valueOffset, int valueSize) {
        this(ByteBuffer.allocate(recordSize(key == null ? 0 : key.length, value == null ? 0 : valueSize >= 0 ? valueSize
                                                                                                            : value.length - valueOffset)));
        write(this.buffer, key, value, codec, valueOffset, valueSize);
        this.buffer.rewind();
    }

    public Record(byte[] key, byte[] value, CompressionType codec) {
        this(key, value, codec, 0, -1);
    }

    public Record(byte[] value, CompressionType codec) {
        this(null, value, codec);
    }

    public Record(byte[] key, byte[] value) {
        this(key, value, CompressionType.NONE);
    }

    public Record(byte[] value) {
        this(null, value, CompressionType.NONE);
    }

    public static void write(ByteBuffer buffer, byte[] key, byte[] value, CompressionType codec, int valueOffset, int valueSize) {
        // skip crc, we will fill that in at the end
        int pos = buffer.position();
        buffer.position(pos + MAGIC_OFFSET);
        buffer.put(CURRENT_MAGIC_VALUE);
        byte attributes = 0;
        if (codec.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & codec.id));
        buffer.put(attributes);
        // write the key
        if (key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            buffer.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            buffer.putInt(size);
            buffer.put(value, valueOffset, size);
        }

        // now compute the checksum and fill it in
        long crc = computeChecksum(buffer,
                                   buffer.arrayOffset() + pos + MAGIC_OFFSET,
                                   buffer.position() - pos - MAGIC_OFFSET - buffer.arrayOffset());
        Utils.writeUnsignedInt(buffer, pos + CRC_OFFSET, crc);
    }

    public static void write(ByteBuffer buffer, byte[] key, byte[] value, CompressionType codec) {
        write(buffer, key, value, codec, 0, -1);
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(int keySize, int valueSize) {
        return CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public static long computeChecksum(ByteBuffer buffer, int position, int size) {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + position, size - buffer.arrayOffset());
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
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
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (!isValid())
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                                             + ", computed crc = "
                                             + computeChecksum()
                                             + ")");
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return buffer.getInt(KEY_SIZE_OFFSET);
    }

    /**
     * Does the record have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        return KEY_OFFSET + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * The magic version of this record
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this record
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * The compression codec used with this record
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the value of this record
     */
    public ByteBuffer value() {
        return sliceDelimited(valueSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        return sliceDelimited(KEY_SIZE_OFFSET);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public String toString() {
        return String.format("Record(magic = %d, attributes = %d, crc = %d, key = %d bytes, value = %d bytes)",
                             magic(),
                             attributes(),
                             checksum(),
                             key().limit(),
                             value().limit());
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

}
