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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    // This matches URIs of formats: host:port and protocol:\\host:port
    // IPv6 is supported with [ip] pattern
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    // Prints up to 2 decimal digits. Used for human readable printing
    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##");

    private static final String[] BYTE_SCALE_SUFFIXES = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    public static final String NL = System.getProperty("line.separator");

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Get a sorted list representation of a collection.
     * @param collection The collection to sort
     * @param <T> The class of objects in the collection
     * @return An unmodifiable sorted list with the contents of the collection
     */
    public static <T extends Comparable<? super T>> List<T> sorted(Collection<T> collection) {
        List<T> res = new ArrayList<>(collection);
        Collections.sort(res);
        return Collections.unmodifiableList(res);
    }

    /**
     * Turn the given UTF8 byte array into a string
     *
     * @param bytes The byte array
     * @return The string
     */
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Read a UTF8 string from a byte buffer. Note that the position of the byte buffer is not affected
     * by this method.
     *
     * @param buffer The buffer to read from
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer, int length) {
        return utf8(buffer, 0, length);
    }

    /**
     * Read a UTF8 string from a byte buffer at a given offset. Note that the position of the byte buffer
     * is not affected by this method.
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position in the buffer
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray())
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length, StandardCharsets.UTF_8);
        else
            return utf8(toArray(buffer, offset, length));
    }

    /**
     * Turn a string into a utf8 byte[]
     *
     * @param string The string
     * @return The byte[]
     */
    public static byte[] utf8(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is different from
     * java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    /**
     * Get the minimum of some long values.
     * @param first Used to ensure at least one value
     * @param rest The remaining values to compare
     * @return The minimum of all passed values
     */
    public static long min(long first, long ... rest) {
        long min = first;
        for (long r : rest) {
            if (r < min)
                min = r;
        }
        return min;
    }

    /**
     * Get the maximum of some long values.
     * @param first Used to ensure at least one value
     * @param rest The remaining values to compare
     * @return The maximum of all passed values
     */
    public static long max(long first, long ... rest) {
        long max = first;
        for (long r : rest) {
            if (r > max)
                max = r;
        }
        return max;
    }


    public static short min(short first, short second) {
        return (short) Math.min(first, second);
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
     * Read the given byte buffer from its current position to its limit into a byte array.
     * @param buffer The buffer to read from
     */
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    /**
     * Read a byte array from its current position given the size in the buffer
     * @param buffer The buffer to read from
     * @param size The number of bytes to read into the array
     */
    public static byte[] toArray(ByteBuffer buffer, int size) {
        return toArray(buffer, 0, size);
    }

    /**
     * Convert a ByteBuffer to a nullable array.
     * @param buffer The buffer to convert
     * @return The resulting array or null if the buffer is null
     */
    public static byte[] toNullableArray(ByteBuffer buffer) {
        return buffer == null ? null : toArray(buffer);
    }

    /**
     * Wrap an array as a nullable ByteBuffer.
     * @param array The nullable array to wrap
     * @return The wrapping ByteBuffer or null if array is null
     */
    public static ByteBuffer wrapNullable(byte[] array) {
        return array == null ? null : ByteBuffer.wrap(array);
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position of the buffer
     * @param size The number of bytes to read into the array
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
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
     * Sleep for a bit
     * @param ms The duration of the sleep
     */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Instantiate the class
     */
    public static <T> T newInstance(Class<T> c) {
        if (c == null)
            throw new KafkaException("class cannot be null");
        try {
            return c.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new KafkaException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        }
    }

    /**
     * Look up the class by name and instantiate it.
     * @param klass class name
     * @param base super class of the class to be instantiated
     * @param <T>
     * @return the new instance
     */
    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        return Utils.newInstance(Class.forName(klass, true, Utils.getContextOrKafkaClassLoader()).asSubclass(base));
    }

    /**
     * Construct a new object using a class name and parameters.
     *
     * @param className                 The full name of the class to construct.
     * @param params                    A sequence of (type, object) elements.
     * @param <T>                       The type of object to construct.
     * @return                          The new object.
     * @throws ClassNotFoundException   If there was a problem constructing the object.
     */
    public static <T> T newParameterizedInstance(String className, Object... params)
            throws ClassNotFoundException {
        Class<?>[] argTypes = new Class<?>[params.length / 2];
        Object[] args = new Object[params.length / 2];
        try {
            Class<?> c = Class.forName(className, true, Utils.getContextOrKafkaClassLoader());
            for (int i = 0; i < params.length / 2; i++) {
                argTypes[i] = (Class<?>) params[2 * i];
                args[i] = params[(2 * i) + 1];
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor = (Constructor<T>) c.getConstructor(argTypes);
            return constructor.newInstance(args);
        } catch (NoSuchMethodException e) {
            throw new ClassNotFoundException(String.format("Failed to find " +
                "constructor with %s for %s", Utils.join(argTypes, ", "), className), e);
        } catch (InstantiationException e) {
            throw new ClassNotFoundException(String.format("Failed to instantiate " +
                "%s", className), e);
        } catch (IllegalAccessException e) {
            throw new ClassNotFoundException(String.format("Unable to access " +
                "constructor of %s", className), e);
        } catch (InvocationTargetException e) {
            throw new ClassNotFoundException(String.format("Unable to invoke " +
                "constructor of %s", className), e);
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
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * Extracts the hostname from a "host:port" address string.
     * @param address address string to parse
     * @return hostname or null if the given address is incorrect
     */
    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extracts the port number from a "host:port" address string.
     * @param address address string to parse
     * @return port number or null if the given address is incorrect
     */
    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    /**
     * Formats hostname and port number as a "host:port" address string,
     * surrounding IPv6 addresses with braces '[', ']'
     * @param host hostname
     * @param port port number
     * @return address string
     */
    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                ? "[" + host + "]:" + port // IPv6
                : host + ":" + port;
    }

    /**
     * Formats a byte number as a human readable String ("3.2 MB")
     * @param bytes some size in bytes
     * @return
     */
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "" + bytes;
        }
        double asDouble = (double) bytes;
        int ordinal = (int) Math.floor(Math.log(asDouble) / Math.log(1024.0));
        double scale = Math.pow(1024.0, ordinal);
        double scaled = asDouble / scale;
        String formatted = TWO_DIGIT_FORMAT.format(scaled);
        try {
            return formatted + " " + BYTE_SCALE_SUFFIXES[ordinal];
        } catch (IndexOutOfBoundsException e) {
            //huge number?
            return "" + asDouble;
        }
    }

    /**
     * Create a string representation of an array joined by the given separator
     * @param strs The array of items
     * @param separator The separator
     * @return The string representation.
     */
    public static <T> String join(T[] strs, String separator) {
        return join(Arrays.asList(strs), separator);
    }

    /**
     * Create a string representation of a list joined by the given separator
     * @param list The list of items
     * @param separator The separator
     * @return The string representation.
     */
    public static <T> String join(Collection<T> list, String separator) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = list.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext())
                sb.append(separator);
        }
        return sb.toString();
    }

    public static <K, V> String mkString(Map<K, V> map, String begin, String end,
                                         String keyValueSeparator, String elementSeparator) {
        StringBuilder bld = new StringBuilder();
        bld.append(begin);
        String prefix = "";
        for (Map.Entry<K, V> entry : map.entrySet()) {
            bld.append(prefix).append(entry.getKey()).
                    append(keyValueSeparator).append(entry.getValue());
            prefix = elementSeparator;
        }
        bld.append(end);
        return bld.toString();
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     */
    public static Properties loadProps(String filename) throws IOException, FileNotFoundException {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
            props.load(propStream);
        }
        return props;
    }

    /**
     * Converts a Properties object to a Map<String, String>, calling {@link #toString} to ensure all keys and values
     * are Strings.
     */
    public static Map<String, String> propsToStringMap(Properties props) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet())
            result.put(entry.getKey().toString(), entry.getValue().toString());
        return result;
    }

    /**
     * Get the stack trace from an exception as a string
     */
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Print an error message and shutdown the JVM
     * @param message The error message
     */
    public static void croak(String message) {
        System.err.println(message);
        Exit.exit(1);
    }

    /**
     * Read a buffer into a Byte array for the given offset and length
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest, 0, length);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Read the given byte buffer into a Byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Attempt to read a file as a string
     * @throws IOException
     */
    public static String readFileAsString(String path, Charset charset) throws IOException {
        if (charset == null) charset = Charset.defaultCharset();

        try (FileInputStream stream = new FileInputStream(new File(path))) {
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            return charset.decode(bb).toString();
        }

    }

    public static String readFileAsString(String path) throws IOException {
        return Utils.readFileAsString(path, Charset.defaultCharset());
    }

    /**
     * Check if the given ByteBuffer capacity
     * @param existingBuffer ByteBuffer capacity to check
     * @param newLength new length for the ByteBuffer.
     * returns ByteBuffer
     */
    public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
        if (newLength > existingBuffer.capacity()) {
            ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
            existingBuffer.flip();
            newBuffer.put(existingBuffer);
            return newBuffer;
        }
        return existingBuffer;
    }

    /*
     * Creates a set
     * @param elems the elements
     * @param <T> the type of element
     * @return Set
     */
    @SafeVarargs
    public static <T> Set<T> mkSet(T... elems) {
        return new HashSet<>(Arrays.asList(elems));
    }

    /*
     * Creates a list
     * @param elems the elements
     * @param <T> the type of element
     * @return List
     */
    @SafeVarargs
    public static <T> List<T> mkList(T... elems) {
        return Arrays.asList(elems);
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void delete(final File file) throws IOException {
        if (file == null)
            return;
        Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                // If the root path did not exist, ignore the error; otherwise throw it.
                if (exc instanceof NoSuchFileException && path.toFile().equals(file))
                    return FileVisitResult.TERMINATE;
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Returns an empty collection if this list is null
     * @param other
     * @return
     */
    public static <T> List<T> safe(List<T> other) {
        return other == null ? Collections.<T>emptyList() : other;
    }

   /**
    * Get the ClassLoader which loaded Kafka.
    */
    public static ClassLoader getKafkaClassLoader() {
        return Utils.class.getClassLoader();
    }

    /**
     * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
     * loaded Kafka.
     *
     * This should be used whenever passing a ClassLoader to Class.forName
     */
    public static ClassLoader getContextOrKafkaClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            return getKafkaClassLoader();
        else
            return cl;
    }

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     *
     * @throws IOException if both atomic and non-atomic moves fail
     */
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Non-atomic move of {} to {} succeeded after atomic move failed due to {}", source, target,
                        outer.getMessage());
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        }
    }

    /**
     * Closes all the provided closeables.
     * @throws IOException if any of the close methods throws an IOException.
     *         The first IOException is thrown with subsequent exceptions
     *         added as suppressed exceptions.
     */
    public static void closeAll(Closeable... closeables) throws IOException {
        IOException exception = null;
        for (Closeable closeable : closeables) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (IOException e) {
                if (exception != null)
                    exception.addSuppressed(e);
                else
                    exception = e;
            }
        }
        if (exception != null)
            throw exception;
    }

    /**
     * Closes {@code closeable} and if an exception is thrown, it is logged at the WARN level.
     */
    public static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                log.warn("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
     * value.
     *
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition since it is used
     * in producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public static int longHashcode(long value) {
        return (int) (value ^ (value >>> 32));
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset.
     * @param buffer Buffer containing the size and data
     * @param start Offset in the buffer to read from
     * @return A slice of the buffer containing only the delimited data (excluding the size)
     */
    public static ByteBuffer sizeDelimited(ByteBuffer buffer, int start) {
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

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer. If the end
     * of the file is reached while there are bytes remaining in the buffer, an EOFException is thrown.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     * @param description A description of what is being read, this will be included in the EOFException if it is thrown
     *
     * @throws IllegalArgumentException If position is negative
     * @throws EOFException If the end of the file is reached while there are remaining bytes in the destination buffer
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     * possible exceptions
     */
    public static void readFullyOrFail(FileChannel channel, ByteBuffer destinationBuffer, long position,
                                       String description) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                    "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    /**
     * Read data from the channel to the given byte buffer until there are no bytes remaining in the buffer or the end
     * of the file has been reached.
     *
     * @param channel File channel containing the data to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin; it must be non-negative
     *
     * @throws IllegalArgumentException If position is negative
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     * possible exceptions
     */
    public static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    /**
     * Read data from the input stream to the given byte buffer until there are no bytes remaining in the buffer or the
     * end of the stream has been reached.
     *
     * @param inputStream Input stream to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred (it must be backed by an array)
     *
     * @throws IOException If an I/O error occurs
     */
    public static final void readFully(InputStream inputStream, ByteBuffer destinationBuffer) throws IOException {
        if (!destinationBuffer.hasArray())
            throw new IllegalArgumentException("destinationBuffer must be backed by an array");
        int initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position();
        byte[] array = destinationBuffer.array();
        int length = destinationBuffer.remaining();
        int totalBytesRead = 0;
        do {
            int bytesRead = inputStream.read(array, initialOffset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1)
                break;
            totalBytesRead += bytesRead;
        } while (length > totalBytesRead);
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead);
    }

    public static void writeFully(FileChannel channel, ByteBuffer sourceBuffer) throws IOException {
        while (sourceBuffer.hasRemaining())
            channel.write(sourceBuffer);
    }

    /**
     * Write the contents of a buffer to an output stream. The bytes are copied from the current position
     * in the buffer.
     * @param out The output to write to
     * @param buffer The buffer to write from
     * @param length The number of bytes to write
     * @throws IOException For any errors writing to the output
     */
    public static void writeTo(DataOutput out, ByteBuffer buffer, int length) throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.position() + buffer.arrayOffset(), length);
        } else {
            int pos = buffer.position();
            for (int i = pos; i < length + pos; i++)
                out.writeByte(buffer.get(i));
        }
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext())
            res.add(iterator.next());
        return res;
    }

}
