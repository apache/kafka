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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.TransferableChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Utils {

    private Utils() {}

    // This matches URIs of formats: host:port and protocol://host:port
    // IPv6 is supported with [ip] pattern
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(?:[0-9a-zA-Z\\-%._]*://)?\\[?([0-9a-zA-Z\\-%._:]*)]?:([0-9]+)");

    private static final Pattern VALID_HOST_CHARACTERS = Pattern.compile("([0-9a-zA-Z\\-%._:]*)");

    // Prints up to 2 decimal digits. Used for human readable printing
    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##",
        DecimalFormatSymbols.getInstance(Locale.ENGLISH));

    private static final String[] BYTE_SCALE_SUFFIXES = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    public static final String NL = System.lineSeparator();

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
     * Read a UTF8 string from the current position till the end of a byte buffer. The position of the byte buffer is
     * not affected by this method.
     *
     * @param buffer The buffer to read from
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer) {
        return utf8(buffer, buffer.remaining());
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
    public static long min(long first, long... rest) {
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
    public static long max(long first, long... rest) {
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
     * Starting from the current position, read an integer indicating the size of the byte array to read,
     * then read the array. Consumes the buffer: upon returning, the buffer's position is after the array
     * that is returned.
     * @param buffer The buffer to read a size-prefixed array from
     * @return The array
     */
    public static byte[] getNullableSizePrefixedArray(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        return getNullableArray(buffer, size);
    }

    /**
     * Read a byte array of the given size. Consumes the buffer: upon returning, the buffer's position
     * is after the array that is returned.
     * @param buffer The buffer to read a size-prefixed array from
     * @param size The number of bytes to read out of the buffer
     * @return The array
     */
    public static byte[] getNullableArray(final ByteBuffer buffer, final int size) {
        if (size > buffer.remaining()) {
            // preemptively throw this when the read is doomed to fail, so we don't have to allocate the array.
            throw new BufferUnderflowException();
        }
        final byte[] oldBytes = size == -1 ? null : new byte[size];
        if (oldBytes != null) {
            buffer.get(oldBytes);
        }
        return oldBytes;
    }

    /**
     * Returns a copy of src byte array
     * @param src The byte array to copy
     * @return The copy
     */
    public static byte[] copyArray(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }

    /**
     * Compares two character arrays for equality using a constant-time algorithm, which is needed
     * for comparing passwords. Two arrays are equal if they have the same length and all
     * characters at corresponding positions are equal.
     *
     * All characters in the first array are examined to determine equality.
     * The calculation time depends only on the length of this first character array; it does not
     * depend on the length of the second character array or the contents of either array.
     *
     * @param first the first array to compare
     * @param second the second array to compare
     * @return true if the arrays are equal, or false otherwise
     */
    public static boolean isEqualConstantTime(char[] first, char[] second) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }

        if (second.length == 0) {
            return first.length == 0;
        }

        // time-constant comparison that always compares all characters in first array
        boolean matches = first.length == second.length;
        for (int i = 0; i < first.length; ++i) {
            int j = i < second.length ? i : 0;
            if (first[i] != second[j]) {
                matches = false;
            }
        }
        return matches;
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
     * @param <T> the type of the base class
     * @return the new instance
     */
    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        return Utils.newInstance(loadClass(klass, base));
    }

    /**
     * Look up a class by name.
     * @param klass class name
     * @param base super class of the class for verification
     * @param <T> the type of the base class
     * @return the new class
     */
    public static <T> Class<? extends T> loadClass(String klass, Class<T> base) throws ClassNotFoundException {
        ClassLoader contextOrKafkaClassLoader = Utils.getContextOrKafkaClassLoader();
        // Use loadClass here instead of Class.forName because the name we use here may be an alias
        // and not match the name of the class that gets loaded. If that happens, Class.forName can
        // throw an exception.
        Class<?> loadedClass = contextOrKafkaClassLoader.loadClass(klass);
        // Invoke forName here with the true name of the requested class to cause class
        // initialization to take place.
        return Class.forName(loadedClass.getName(), true, contextOrKafkaClassLoader).asSubclass(base);
    }

    /**
     * Cast {@code klass} to {@code base} and instantiate it.
     * @param klass The class to instantiate
     * @param base A know baseclass of klass.
     * @param <T> the type of the base class
     * @throws ClassCastException If {@code klass} is not a subclass of {@code base}.
     * @return the new instance.
     */
    public static <T> T newInstance(Class<?> klass, Class<T> base) {
        return Utils.newInstance(klass.asSubclass(base));
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
            Class<?> c = Utils.loadClass(className, Object.class);
            for (int i = 0; i < params.length / 2; i++) {
                argTypes[i] = (Class<?>) params[2 * i];
                args[i] = params[(2 * i) + 1];
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor = (Constructor<T>) c.getConstructor(argTypes);
            return constructor.newInstance(args);
        } catch (NoSuchMethodException e) {
            throw new ClassNotFoundException(String.format("Failed to find " +
                "constructor with %s for %s", Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(", ")), className), e);
        } catch (InstantiationException e) {
            throw new ClassNotFoundException(String.format("Failed to instantiate " +
                "%s", className), e);
        } catch (IllegalAccessException e) {
            throw new ClassNotFoundException(String.format("Unable to access " +
                "constructor of %s", className), e);
        } catch (InvocationTargetException e) {
            throw new KafkaException(String.format("The constructor of %s threw an exception", className), e.getCause());
        }
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    @SuppressWarnings("fallthrough")
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
     * Basic validation of the supplied address. checks for valid characters
     * @param address hostname string to validate
     * @return true if address contains valid characters
     */
    public static boolean validHostPattern(String address) {
        return VALID_HOST_CHARACTERS.matcher(address).matches();
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
     * Formats a byte number as a human-readable String ("3.2 MB")
     * @param bytes some size in bytes
     * @return
     */
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return String.valueOf(bytes);
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
            return String.valueOf(asDouble);
        }
    }

    /**
     *  Converts a {@code Map} class into a string, concatenating keys and values
     *  Example:
     *      {@code mkString({ key: "hello", keyTwo: "hi" }, "|START|", "|END|", "=", ",")
     *          => "|START|key=hello,keyTwo=hi|END|"}
     */
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
     *  Converts an extensions string into a {@code Map<String, String>}.
     *
     *  Example:
     *      {@code parseMap("key=hey,keyTwo=hi,keyThree=hello", "=", ",") => { key: "hey", keyTwo: "hi", keyThree: "hello" }}
     *
     */
    public static Map<String, String> parseMap(String mapStr, String keyValueSeparator, String elementSeparator) {
        Map<String, String> map = new HashMap<>();

        if (!mapStr.isEmpty()) {
            String[] attrvals = mapStr.split(elementSeparator);
            for (String attrval : attrvals) {
                String[] array = attrval.split(keyValueSeparator, 2);
                map.put(array[0], array[1]);
            }
        }
        return map;
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     * @return the loaded properties
     */
    public static Properties loadProps(String filename) throws IOException {
        return loadProps(filename, null);
    }

    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     * @param onlyIncludeKeys When non-null, only return values associated with these keys and ignore all others
     * @return the loaded properties
     */
    public static Properties loadProps(String filename, List<String> onlyIncludeKeys) throws IOException {
        Properties props = new Properties();

        if (filename != null) {
            try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
                props.load(propStream);
            }
        } else {
            System.out.println("Did not load any properties since the property file is not specified");
        }

        if (onlyIncludeKeys == null || onlyIncludeKeys.isEmpty())
            return props;
        Properties requestedProps = new Properties();
        onlyIncludeKeys.forEach(key -> {
            String value = props.getProperty(key);
            if (value != null)
                requestedProps.setProperty(key, value);
        });
        return requestedProps;
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
     * Read a buffer into a Byte array for the given offset and length
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest);
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
     * Reads bytes from a source buffer and returns a new buffer.
     * <p> The content of the new buffer will start at this buffer's current
     * position.  Changes to this buffer's content will be visible in the new
     * buffer, and vice versa; the two buffers' position, limit, and mark
     * values will be independent.
     *
     * <p> The new buffer's position will be zero, its limit will be the number of bytes
     * read i.e. <code>bytesToRead</code>, it's capacity will be the number of bytes remaining in
     * source buffer , its mark will be undefined, and its byte order will be {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN}.
     *
     * <p>Since JDK 13, this method could be replaced with slice(int index, int length).
     *
     * @param srcBuf Source buffer where data is read from
     * @param bytesToRead Number of bytes to read
     * @return Destination buffer or null if bytesToRead is < 0
     *
     * @see ByteBuffer#slice()
     */
    public static ByteBuffer readBytes(ByteBuffer srcBuf, int bytesToRead) {
        if (bytesToRead < 0)
            return null;

        final ByteBuffer dstBuf = srcBuf.slice();
        dstBuf.limit(bytesToRead);
        srcBuf.position(srcBuf.position() + bytesToRead);

        return dstBuf;
    }

    /**
     * Read a file as string and return the content. The file is treated as a stream and no seek is performed.
     * This allows the program to read from a regular file as well as from a pipe/fifo.
     */
    public static String readFileAsString(String path) throws IOException {
        try {
            byte[] allBytes = Files.readAllBytes(Paths.get(path));
            return new String(allBytes, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IOException("Unable to read file " + path, ex);
        }
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

    /**
     * Creates a sorted set
     * @param elems the elements
     * @param <T> the type of element, must be comparable
     * @return SortedSet
     */
    @SafeVarargs
    public static <T extends Comparable<T>> SortedSet<T> mkSortedSet(T... elems) {
        SortedSet<T> result = new TreeSet<>();
        for (T elem : elems)
            result.add(elem);
        return result;
    }

    /**
     * Creates a map entry (for use with {@link Utils#mkMap(java.util.Map.Entry[])})
     *
     * @param k   The key
     * @param v   The value
     * @param <K> The key type
     * @param <V> The value type
     * @return An entry
     */
    public static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
        return new AbstractMap.SimpleEntry<>(k, v);
    }

    /**
     * Creates a map from a sequence of entries
     *
     * @param entries The entries to map
     * @param <K>     The key type
     * @param <V>     The value type
     * @return A map
     */
    @SafeVarargs
    public static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
        final LinkedHashMap<K, V> result = new LinkedHashMap<>();
        for (final Map.Entry<K, V> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Creates a {@link Properties} from a map
     *
     * @param properties A map of properties to add
     * @return The properties object
     */
    public static Properties mkProperties(final Map<String, String> properties) {
        final Properties result = new Properties();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Creates a {@link Properties} from a map
     *
     * @param properties A map of properties to add
     * @return The properties object
     */
    public static Properties mkObjectProperties(final Map<String, Object> properties) {
        final Properties result = new Properties();
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param rootFile The root file at which to begin deleting
     */
    public static void delete(final File rootFile) throws IOException {
        if (rootFile == null)
            return;
        Files.walkFileTree(rootFile.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                if (exc instanceof NoSuchFileException) {
                    if (path.toFile().equals(rootFile)) {
                        // If the root path did not exist, ignore the error and terminate;
                        return FileVisitResult.TERMINATE;
                    } else {
                        // Otherwise, just continue walking as the file might already be deleted by other threads.
                        return FileVisitResult.CONTINUE;
                    }
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                // KAFKA-8999: if there's an exception thrown previously already, we should throw it
                if (exc != null) {
                    throw exc;
                }

                Files.deleteIfExists(path);
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
        return other == null ? Collections.emptyList() : other;
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
     * This function also flushes the parent directory to guarantee crash consistency.
     *
     * @throws IOException if both atomic and non-atomic moves fail, or parent dir flush fails.
     */
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        atomicMoveWithFallback(source, target, true);
    }

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     * This function allows callers to decide whether to flush the parent directory. This is needed
     * when a sequence of atomicMoveWithFallback is called for the same directory and we don't want
     * to repeatedly flush the same parent directory.
     *
     * @throws IOException if both atomic and non-atomic moves fail,
     * or parent dir flush fails if needFlushParentDir is true.
     */
    public static void atomicMoveWithFallback(Path source, Path target, boolean needFlushParentDir) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                log.warn("Failed atomic move of {} to {} retrying with a non-atomic move", source, target, outer);
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Non-atomic move of {} to {} succeeded after atomic move failed", source, target);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            if (needFlushParentDir) {
                flushDir(target.toAbsolutePath().normalize().getParent());
            }
        }
    }

    /**
     * Flushes dirty directories to guarantee crash consistency.
     *
     * Note: We don't fsync directories on Windows OS because otherwise it'll throw AccessDeniedException (KAFKA-13391)
     *
     * @throws IOException if flushing the directory fails.
     */
    public static void flushDir(Path path) throws IOException {
        if (path != null && !OperatingSystem.IS_WINDOWS && !OperatingSystem.IS_ZOS) {
            try (FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
                dir.force(true);
            }
        }
    }

    /**
     * Flushes dirty directories to guarantee crash consistency with swallowing {@link NoSuchFileException}
     *
     * @throws IOException if flushing the directory fails.
     */
    public static void flushDirIfExists(Path path) throws IOException {
        try {
            flushDir(path);
        } catch (NoSuchFileException e) {
            log.warn("Failed to flush directory {}", path);
        }
    }

    /**
     * Flushes dirty file with swallowing {@link NoSuchFileException}
     */
    public static void flushFileIfExists(Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            fileChannel.force(true);
        } catch (NoSuchFileException e) {
            log.warn("Failed to flush file {}", path, e);
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

    @FunctionalInterface
    public interface SwallowAction {
        void run() throws Throwable;
    }

    public static void swallow(final Logger log, final Level level, final String what, final SwallowAction code) {
        swallow(log, level, what, code, null);
    }

    /**
     * Run the supplied code. If an exception is thrown, it is swallowed and registered to the firstException parameter.
     */
    public static void swallow(final Logger log, final Level level, final String what, final SwallowAction code,
                               final AtomicReference<Throwable> firstException) {
        if (code != null) {
            try {
                code.run();
            } catch (Throwable t) {
                switch (level) {
                    case INFO:
                        log.info(what, t);
                        break;
                    case DEBUG:
                        log.debug(what, t);
                        break;
                    case ERROR:
                        log.error(what, t);
                        break;
                    case TRACE:
                        log.trace(what, t);
                        break;
                    case WARN:
                    default:
                        log.warn(what, t);
                }
                if (firstException != null)
                    firstException.compareAndSet(null, t);
            }
        }
    }

    /**
     * An {@link AutoCloseable} interface without a throws clause in the signature
     *
     * This is used with lambda expressions in try-with-resources clauses
     * to avoid casting un-checked exceptions to checked exceptions unnecessarily.
     */
    @FunctionalInterface
    public interface UncheckedCloseable extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Closes {@code maybeCloseable} if it implements the {@link AutoCloseable} interface,
     * and if an exception is thrown, it is logged at the WARN level.
     */
    public static void maybeCloseQuietly(Object maybeCloseable, String name) {
        if (maybeCloseable instanceof AutoCloseable)
            closeQuietly((AutoCloseable) maybeCloseable, name);
    }

    /**
     * Closes {@code closeable} and if an exception is thrown, it is logged at the WARN level.
     * <b>Be cautious when passing method references as an argument.</b> For example:
     * <p>
     * {@code closeQuietly(task::stop, "source task");}
     * <p>
     * Although this method gracefully handles null {@link AutoCloseable} objects, attempts to take a method
     * reference from a null object will result in a {@link NullPointerException}. In the example code above,
     * it would be the caller's responsibility to ensure that {@code task} was non-null before attempting to
     * use a method reference from it.
     */
    public static void closeQuietly(AutoCloseable closeable, String name) {
        closeQuietly(closeable, name, log);
    }

    /**
     * Closes {@code closeable} and if an exception is thrown, it is logged with the provided logger at the WARN level.
     * <b>Be cautious when passing method references as an argument.</b> For example:
     * <p>
     * {@code closeQuietly(task::stop, "source task");}
     * <p>
     * Although this method gracefully handles null {@link AutoCloseable} objects, attempts to take a method
     * reference from a null object will result in a {@link NullPointerException}. In the example code above,
     * it would be the caller's responsibility to ensure that {@code task} was non-null before attempting to
     * use a method reference from it.
     */
    public static void closeQuietly(AutoCloseable closeable, String name, Logger logger) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                logger.warn("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
    * Closes {@code closeable} and if an exception is thrown, it is registered to the firstException parameter.
    * <b>Be cautious when passing method references as an argument.</b> For example:
    * <p>
    * {@code closeQuietly(task::stop, "source task");}
    * <p>
    * Although this method gracefully handles null {@link AutoCloseable} objects, attempts to take a method
    * reference from a null object will result in a {@link NullPointerException}. In the example code above,
    * it would be the caller's responsibility to ensure that {@code task} was non-null before attempting to
    * use a method reference from it.
    */
    public static void closeQuietly(AutoCloseable closeable, String name, AtomicReference<Throwable> firstException) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
     * close all closable objects even if one of them throws exception.
     * @param firstException keeps the first exception
     * @param name message of closing those objects
     * @param closeables closable objects
     */
    public static void closeAllQuietly(AtomicReference<Throwable> firstException, String name, AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) closeQuietly(closeable, name, firstException);
    }

    /**
     * Invokes every function in `all` even if one or more functions throws an exception.
     *
     * If any of the functions throws an exception, the first one will be rethrown at the end with subsequent exceptions
     * added as suppressed exceptions.
     */
    // Note that this is a generalised version of `closeAll`. We could potentially make it more general by
    // changing the signature to `public <R> List<R> tryAll(all: List[Callable<R>])`
    public static void tryAll(List<Callable<Void>> all) throws Throwable {
        Throwable exception = null;
        for (Callable<Void> call : all) {
            try {
                call.call();
            } catch (Throwable t) {
                if (exception != null)
                    exception.addSuppressed(t);
                else
                    exception = t;
            }
        }
        if (exception != null)
            throw exception;
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolute
     * value.
     *
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition since it is used
     * in producer's partition selection logic {@link org.apache.kafka.clients.producer.KafkaProducer}
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
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
     * @param inputStream       Input stream to read from
     * @param destinationBuffer The buffer into which bytes are to be transferred (it must be backed by an array)
     * @return number of byte read from the input stream
     * @throws IOException If an I/O error occurs
     */
    public static int readFully(InputStream inputStream, ByteBuffer destinationBuffer) throws IOException {
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
        return totalBytesRead;
    }

    public static void writeFully(FileChannel channel, ByteBuffer sourceBuffer) throws IOException {
        while (sourceBuffer.hasRemaining())
            channel.write(sourceBuffer);
    }

    /**
     * Trying to write data in source buffer to a {@link TransferableChannel}, we may need to call this method multiple
     * times since this method doesn't ensure the data in the source buffer can be fully written to the destination channel.
     *
     * @param destChannel The destination channel
     * @param position From which the source buffer will be written
     * @param length The max size of bytes can be written
     * @param sourceBuffer The source buffer
     *
     * @return The length of the actual written data
     * @throws IOException If an I/O error occurs
     */
    public static int tryWriteTo(TransferableChannel destChannel,
                                  int position,
                                  int length,
                                  ByteBuffer sourceBuffer) throws IOException {

        ByteBuffer dup = sourceBuffer.duplicate();
        dup.position(position);
        dup.limit(position + length);
        return destChannel.write(dup);
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

    public static <T> List<T> toList(Iterable<T> iterable) {
        return toList(iterable.iterator());
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext())
            res.add(iterator.next());
        return res;
    }

    public static <T> List<T> toList(Iterator<T> iterator, Predicate<T> predicate) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext()) {
            T e = iterator.next();
            if (predicate.test(e)) {
                res.add(e);
            }
        }
        return res;
    }

    public static int to32BitField(final Set<Byte> bytes) {
        int value = 0;
        for (final byte b : bytes)
            value |= 1 << checkRange(b);
        return value;
    }

    private static byte checkRange(final byte i) {
        if (i > 31)
            throw new IllegalArgumentException("out of range: i>31, i = " + i);
        if (i < 0)
            throw new IllegalArgumentException("out of range: i<0, i = " + i);
        return i;
    }

    public static Set<Byte> from32BitField(final int intValue) {
        Set<Byte> result = new HashSet<>();
        for (int itr = intValue, count = 0; itr != 0; itr >>>= 1) {
            if ((itr & 1) != 0)
                result.add((byte) count);
            count++;
        }
        return result;
    }

    /**
     * A Collector that offers two kinds of convenience:
     * 1. You can specify the concrete type of the returned Map
     * 2. You can turn a stream of Entries directly into a Map without having to mess with a key function
     *    and a value function. In particular, this is handy if all you need to do is apply a filter to a Map's entries.
     *
     *
     * One thing to be wary of: These types are too "distant" for IDE type checkers to warn you if you
     * try to do something like build a TreeMap of non-Comparable elements. You'd get a runtime exception for that.
     *
     * @param mapSupplier The constructor for your concrete map type.
     * @param <K> The Map key type
     * @param <V> The Map value type
     * @param <M> The type of the Map itself.
     * @return new Collector<Map.Entry<K, V>, M, M>
     */
    public static <K, V, M extends Map<K, V>> Collector<Map.Entry<K, V>, M, M> entriesToMap(final Supplier<M> mapSupplier) {
        return new Collector<Map.Entry<K, V>, M, M>() {
            @Override
            public Supplier<M> supplier() {
                return mapSupplier;
            }

            @Override
            public BiConsumer<M, Map.Entry<K, V>> accumulator() {
                return (map, entry) -> map.put(entry.getKey(), entry.getValue());
            }

            @Override
            public BinaryOperator<M> combiner() {
                return (map, map2) -> {
                    map.putAll(map2);
                    return map;
                };
            }

            @Override
            public Function<M, M> finisher() {
                return map -> map;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH);
            }
        };
    }

    @SafeVarargs
    public static <E> Set<E> union(final Supplier<Set<E>> constructor, final Set<E>... set) {
        final Set<E> result = constructor.get();
        for (final Set<E> s : set) {
            result.addAll(s);
        }
        return result;
    }

    @SafeVarargs
    public static <E> Set<E> intersection(final Supplier<Set<E>> constructor, final Set<E> first, final Set<E>... set) {
        final Set<E> result = constructor.get();
        result.addAll(first);
        for (final Set<E> s : set) {
            result.retainAll(s);
        }
        return result;
    }

    public static <E> Set<E> diff(final Supplier<Set<E>> constructor, final Set<E> left, final Set<E> right) {
        final Set<E> result = constructor.get();
        result.addAll(left);
        result.removeAll(right);
        return result;
    }

    public static <K, V> Map<K, V> filterMap(final Map<K, V> map, final Predicate<Entry<K, V>> filterPredicate) {
        return map.entrySet().stream().filter(filterPredicate).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Convert a properties to map. All keys in properties must be string type. Otherwise, a ConfigException is thrown.
     * @param properties to be converted
     * @return a map including all elements in properties
     */
    public static Map<String, Object> propsToMap(Properties properties) {
        return castToStringObjectMap(properties);
    }

    /**
     * Cast a map with arbitrary type keys to be keyed on String.
     * @param inputMap A map with unknown type keys
     * @return A map with the same contents as the input map, but with String keys
     * @throws ConfigException if any key is not a String
     */
    public static Map<String, Object> castToStringObjectMap(Map<?, ?> inputMap) {
        Map<String, Object> map = new HashMap<>(inputMap.size());
        for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, entry.getValue());
            } else {
                throw new ConfigException(String.valueOf(entry.getKey()), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }

    /**
     * Convert timestamp to an epoch value
     * @param timestamp the timestamp to be converted, the accepted formats are:
     *                 (1) yyyy-MM-dd'T'HH:mm:ss.SSS, ex: 2020-11-10T16:51:38.198
     *                 (2) yyyy-MM-dd'T'HH:mm:ss.SSSZ, ex: 2020-11-10T16:51:38.198+0800
     *                 (3) yyyy-MM-dd'T'HH:mm:ss.SSSX, ex: 2020-11-10T16:51:38.198+08
     *                 (4) yyyy-MM-dd'T'HH:mm:ss.SSSXX, ex: 2020-11-10T16:51:38.198+0800
     *                 (5) yyyy-MM-dd'T'HH:mm:ss.SSSXXX, ex: 2020-11-10T16:51:38.198+08:00
     *
     * @return epoch value of a given timestamp (i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT)
     * @throws ParseException for timestamp that doesn't follow ISO8601 format or the format is not expected
     */
    public static long getDateTime(String timestamp) throws ParseException, IllegalArgumentException {
        if (timestamp == null) {
            throw new IllegalArgumentException("Error parsing timestamp with null value");
        }

        final String[] timestampParts = timestamp.split("T");
        if (timestampParts.length < 2) {
            throw new ParseException("Error parsing timestamp. It does not contain a 'T' according to ISO8601 format", timestamp.length());
        }

        final String secondPart = timestampParts[1];
        if (!(secondPart.contains("+") || secondPart.contains("-") || secondPart.contains("Z"))) {
            timestamp = timestamp + "Z";
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
        // strictly parsing the date/time format
        simpleDateFormat.setLenient(false);
        try {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        } catch (final ParseException e) {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        }
    }

    /**
     * Checks if a string is null, empty or whitespace only.
     * @param str a string to be checked
     * @return true if the string is null, empty or whitespace only; otherwise, return false.
     */
    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Get an array containing all of the {@link Object#toString string representations} of a given enumerable type.
     * @param enumClass the enum class; may not be null
     * @return an array with the names of every value for the enum class; never null, but may be empty
     * if there are no values defined for the enum
     */
    public static String[] enumOptions(Class<? extends Enum<?>> enumClass) {
        Objects.requireNonNull(enumClass);
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException("Class " + enumClass + " is not an enumerable type");
        }

        return Stream.of(enumClass.getEnumConstants())
                .map(Object::toString)
                .toArray(String[]::new);
    }

    /**
     * Ensure that the class is concrete (i.e., not abstract), and that it subclasses a given base class.
     * If it is abstract or does not subclass the given base class, throw a {@link ConfigException}
     * with a friendly error message suggesting a list of concrete child subclasses (if any are known).
     * @param baseClass the expected superclass; may not be null
     * @param klass the class to check; may not be null
     * @throws ConfigException if the class is not concrete
     */
    public static void ensureConcreteSubclass(Class<?> baseClass, Class<?> klass) {
        Objects.requireNonNull(baseClass);
        Objects.requireNonNull(klass);

        if (!baseClass.isAssignableFrom(klass)) {
            String inheritFrom = baseClass.isInterface() ? "implement" : "extend";
            String baseClassType = baseClass.isInterface() ? "interface" : "class";
            throw new ConfigException("Class " + klass + " does not " + inheritFrom + " the " + baseClass.getSimpleName() + " " + baseClassType);
        }

        if (Modifier.isAbstract(klass.getModifiers())) {
            String childClassNames = Stream.of(klass.getClasses())
                    .filter(baseClass::isAssignableFrom)
                    .filter(c -> !Modifier.isAbstract(c.getModifiers()))
                    .filter(c -> Modifier.isPublic(c.getModifiers()))
                    .map(Class::getName)
                    .collect(Collectors.joining(", "));
            String message = "This class is abstract and cannot be created.";
            if (!Utils.isBlank(childClassNames))
                message += " Did you mean " + childClassNames + "?";
            throw new ConfigException(message);
        }
    }

    /**
     * Convert time instant to readable string for logging
     * @param timestamp the timestamp of the instant to be converted.
     *
     * @return string value of a given timestamp in the format "yyyy-MM-dd HH:mm:ss,SSS"
     */
    public static String toLogDateTimeFormat(long timestamp) {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS XXX");
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(dateTimeFormatter);
    }

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
     */
    public static String replaceSuffix(String str, String oldSuffix, String newSuffix) {
        if (!str.endsWith(oldSuffix))
            throw new IllegalArgumentException("Expected string to end with " + oldSuffix + " but string is " + str);
        return str.substring(0, str.length() - oldSuffix.length()) + newSuffix;
    }

    /**
     * Find all key/value pairs whose keys begin with the given prefix, and remove that prefix from all
     * resulting keys.
     * @param map the map to filter key/value pairs from
     * @param prefix the prefix to search keys for
     * @return a {@link Map} containing a key/value pair for every key/value pair in the {@code map}
     * parameter whose key begins with the given {@code prefix} and whose corresponding keys have
     * the prefix stripped from them; may be empty, but never null
     * @param <V> the type of values stored in the map
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix) {
        return entriesWithPrefix(map, prefix, true);
    }

    /**
     * Find all key/value pairs whose keys begin with the given prefix, optionally removing that prefix
     * from all resulting keys.
     * @param map the map to filter key/value pairs from
     * @param prefix the prefix to search keys for
     * @param strip whether the keys of the returned map should not include the prefix
     * @return a {@link Map} containing a key/value pair for every key/value pair in the {@code map}
     * parameter whose key begins with the given {@code prefix}; may be empty, but never null
     * @param <V> the type of values stored in the map
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip) {
        return entriesWithPrefix(map, prefix, strip, false);
    }

    /**
     * Find all key/value pairs whose keys begin with the given prefix, optionally removing that prefix
     * from all resulting keys.
     * @param map the map to filter key/value pairs from
     * @param prefix the prefix to search keys for
     * @param strip whether the keys of the returned map should not include the prefix
     * @param allowMatchingLength whether to include keys that are exactly the same length as the prefix
     * @return a {@link Map} containing a key/value pair for every key/value pair in the {@code map}
     * parameter whose key begins with the given {@code prefix}; may be empty, but never null
     * @param <V> the type of values stored in the map
     */
    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip, boolean allowMatchingLength) {
        Map<String, V> result = new HashMap<>();
        for (Map.Entry<String, V> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix) && (allowMatchingLength || entry.getKey().length() > prefix.length())) {
                if (strip)
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Checks requirement. Throw {@link IllegalArgumentException} if {@code requirement} failed.
     * @param requirement Requirement to check.
     */
    public static void require(boolean requirement) {
        if (!requirement)
            throw new IllegalArgumentException("requirement failed");
    }

    /**
     * Checks requirement. Throw {@link IllegalArgumentException} if {@code requirement} failed.
     * @param requirement Requirement to check.
     * @param errorMessage String to include in the failure message
     */
    public static void require(boolean requirement, String errorMessage) {
        if (!requirement)
            throw new IllegalArgumentException(errorMessage);
    }

    /**
     * Merge multiple {@link ConfigDef} into one
     * @param configDefs List of {@link ConfigDef}
     */
    public static ConfigDef mergeConfigs(List<ConfigDef> configDefs) {
        ConfigDef all = new ConfigDef();
        configDefs.forEach(configDef -> configDef.configKeys().values().forEach(all::define));
        return all;
    }
    /**
     * A runnable that can throw checked exception.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
