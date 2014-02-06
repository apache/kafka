package kafka.common.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import kafka.common.KafkaException;

public class Utils {

    /**
     * Turn the given UTF8 byte array into a string
     * 
     * @param bytes The byte array
     * @return The string
     */
    public static String utf8(byte[] bytes) {
        try {
            return new String(bytes, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    /**
     * Turn a string into a utf8 byte[]
     * 
     * @param string The string
     * @return The byte[]
     */
    public static byte[] utf8(String string) {
        try {
            return string.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     * 
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     * 
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * 
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writetUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * 
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     * 
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static long crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     * 
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        Crc32 crc = new Crc32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is different from
     * java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return n & 0x7fffffff;
    }

    /**
     * Get the length for UTF8-encoding a string without encoding it first
     * 
     * @param s The string to calculate the length for
     * @return The length when serialized
     */
    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    /**
     * Read the given byte buffer into a byte array
     */
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.limit());
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Check that the parameter t is not null
     * 
     * @param t The object to check
     * @return t if it isn't null
     * @throws NullPointerException if t is null.
     */
    public static <T> T notNull(T t) {
        if (t == null)
            throw new NullPointerException();
        else
            return t;
    }

    /**
     * Instantiate the class
     */
    public static Object newInstance(Class<?> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new KafkaException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        }
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

}
