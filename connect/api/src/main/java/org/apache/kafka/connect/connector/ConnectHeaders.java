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
package org.apache.kafka.connect.connector;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Connect utilities for working with {@link Headers} objects and {@link Header} values.
 */
public class ConnectHeaders {

    private static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;
    private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
    private static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
    private static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
    private static final Reader READER = new SimpleReader();

    /**
     * Get a {@link Reader} utility for easily converting header values to common primitive types.
     *
     * @return the header reader utility; never null
     */
    public static Reader reader() {
        return READER;
    }

    /**
     * Obtain a new {@link Builder fluent builder} of {@link Headers} objects.
     *
     * @return the fluent builder for a new {@link Headers} object; never null
     */
    public static Builder create() {
        return new SimpleBuilder();
    }

    /**
     * Fluent API for creating new {@link Headers} objects.
     */
    public interface Builder {
        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, byte[] value);

        /**
         * Add a header with the given name and entire contents of the specified {@link ByteBuffer}.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, ByteBuffer value);

        /**
         * Add a header with the given name and String value encoded in UTF-8.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, String value);

        /**
         * Add a header with the given name and String value encoded in the specified character set.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, String value, Charset charset);

        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, short value);

        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, int value);

        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, long value);

        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, float value);

        /**
         * Add a header with the given name and value.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, double value);

        /**
         * Add a header with the given name and boolean value encoded as a short.
         *
         * @param key   the name of the header; may not be null
         * @param value the value for the header; may be null or empty
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, boolean value);

        /**
         * Use the specified converter to convert the schema and value to a binary form, and add a header with the given name and binary
         * value.
         * A null topic name is supplied to the converter.
         *
         * @param key       the name of the header; may not be null
         * @param schema    the schema for the value; may be null
         * @param value     the value to be converted to a binary form and used as the header value; may be null or empty
         * @param converter the converter that should be used to convert the schema and value to a binary form; may not be null
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, Schema schema, Object value, Converter converter);

        /**
         * Use the specified converter to convert the schema and value to a binary form, and add a header with the given name and binary
         * value.
         *
         * @param key       the name of the header; may not be null
         * @param schema    the schema for the value; may be null
         * @param value     the value to be converted to a binary form and used as the header value; may be null or empty
         * @param converter the converter that should be used to convert the schema and value to a binary form; may not be null
         * @param topic     the name of the topic to pass to the {@link Converter#fromConnectData(String, Schema, Object)} method; may be
         *                  null
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, Schema schema, Object value, Converter converter, String topic);

        /**
         * Use the specified converter to convert the schema and value to a binary form, and add a header with the given name and binary
         * value.
         * A null topic name is supplied to the converter.
         *
         * @param key       the name of the header; may not be null
         * @param value     the object to be converted to a binary form and used as the header value; may be null or empty
         * @param converter the converter that should be used to convert the schema and value to a binary form; may not be null
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, SchemaAndValue value, Converter converter);

        /**
         * Use the specified converter to convert the schema and value to a binary form, and add a header with the given name and binary
         * value.
         *
         * @param key       the name of the header; may not be null
         * @param value     the object to be converted to a binary form and used as the header value; may be null or empty
         * @param converter the converter that should be used to convert the schema and value to a binary form; may not be null
         * @param topic     the name of the topic to pass to the {@link Converter#fromConnectData(String, Schema, Object)} method; may be
         *                  null
         * @return this builder so methods can be chained together; never null
         */
        Builder with(String key, SchemaAndValue value, Converter converter, String topic);

        /**
         * Construct a new {@link Headers} object with the header objects added to this builder previously via the {@code with(...)}
         * methods.
         *
         * @return the new {@link Headers} object; never null
         */
        Headers build();

        /**
         * Remove all header objects added to this builder by previous calls to {@code with(...)}.
         * @return this builder so methods can be chained together; never null
         */
        Builder clear();
    }

    /**
     * A utility API for reading the header values and converting to various primitive types. This reader assumes big endian byte order
     * for all conversions.
     */
    public interface Reader {
        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} as a byte array.
         *
         * @param header the header; may be null
         * @return the header's value as a byte array, or null if the header or its value is null
         */
        byte[] readAsBytes(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} as a {@link ByteBuffer}.
         *
         * @param header the header; may be null
         * @return the header's value as a {@link ByteBuffer}, or null if the header or its value is null
         */
        ByteBuffer readAsByteBuffer(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as a short from big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as a short, or null if the header or its value is null
         */
        Short readAsShort(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as an integer from big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as an integer, or null if the header or its value is null
         */
        Integer readAsInt(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as a long from big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as a long, or null if the header or its value is null
         */
        Long readAsLong(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as a float from big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as a float, or null if the header or its value is null
         */
        Float readAsFloat(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as a double from big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as a double, or null if the header or its value is null
         */
        Double readAsDouble(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} converted as either a 1 or 0 short value from
         * big-endian byte order.
         *
         * @param header the header; may be null
         * @return the header's value as a boolean, or null if the header or its value is null
         */
        Boolean readAsBoolean(Header header);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} encoded in the specified character set converted
         * to a string.
         *
         * @param header  the header; may be null
         * @param charset the character set; may not be null
         * @return the header's value as a string, or null if the header or its value is null
         */
        String readAsString(Header header, Charset charset);

        /**
         * Read the supplied {@link Header} and return its {@link Header#value() value} encoded in UTF-8 converted to a string.
         *
         * @param header the header; may be null
         * @return the header's value as a string, or null if the header or its value is null
         */
        String readAsStringUtf8(Header header);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value} as a
         * byte array.
         *
         * @param headers the {@link Headers} object; may be null
         * @return the header's value as a byte array, or null if the header or its value is null
         */
        byte[] readAsBytes(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value} as a
         * {@link ByteBuffer}.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a {@link ByteBuffer}, or null if the header or its value is null
         */
        ByteBuffer readAsByteBuffer(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as a short from big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a short, or null if the header or its value is null
         */
        Short readAsShort(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as an integer from big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as an integer, or null if the header or its value is null
         */
        Integer readAsInt(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as a long from big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a long, or null if the header or its value is null
         */
        Long readAsLong(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as a float from big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a float, or null if the header or its value is null
         */
        Float readAsFloat(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as a double from big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a double, or null if the header or its value is null
         */
        Double readAsDouble(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * converted as either a 1 or 0 short value from
         * big-endian byte order.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a boolean, or null if the header or its value is null
         */
        Boolean readAsBoolean(Headers headers, String key);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * encoded in the specified character set converted
         * to a string.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @param charset the character set; may not be null
         * @return the header's value as a string, or null if the header or its value is null
         */
        String readAsString(Headers headers, String key, Charset charset);

        /**
         * Look in the given {@link Headers} for the last header with the specified key and return its {@link Header#value() value}
         * encoded in UTF-8 converted to a string.
         *
         * @param headers the {@link Headers} object; may be null
         * @param key     the name of the header; may not be null
         * @return the header's value as a string, or null if the header or its value is null
         */
        String readAsStringUtf8(Headers headers, String key);
    }

    private static class SimpleReader implements Reader {


        @Override
        public byte[] readAsBytes(Header header) {
            return header == null ? null : header.value();
        }

        @Override
        public ByteBuffer readAsByteBuffer(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value());
        }

        @Override
        public Short readAsShort(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getShort();
        }

        @Override
        public Integer readAsInt(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getInt();
        }

        @Override
        public Long readAsLong(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getLong();
        }

        @Override
        public Float readAsFloat(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getFloat();
        }

        @Override
        public Double readAsDouble(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getDouble();
        }

        @Override
        public Boolean readAsBoolean(Header header) {
            return header == null || header.value() == null ? null : ByteBuffer.wrap(header.value()).getShort() == 1 ? false : true;
        }

        @Override
        public String readAsString(Header header, Charset charset) {
            return header == null || header.value() == null ? null : new String(header.value(), charset);
        }

        @Override
        public String readAsStringUtf8(Header header) {
            return readAsString(header, StandardCharsets.UTF_8);
        }

        @Override
        public byte[] readAsBytes(Headers headers, String key) {
            return headers == null ? null : readAsBytes(headers.lastHeader(key));
        }

        @Override
        public ByteBuffer readAsByteBuffer(Headers headers, String key) {
            return headers == null ? null : readAsByteBuffer(headers.lastHeader(key));
        }

        @Override
        public Short readAsShort(Headers headers, String key) {
            return headers == null ? null : readAsShort(headers.lastHeader(key));
        }

        @Override
        public Integer readAsInt(Headers headers, String key) {
            return headers == null ? null : readAsInt(headers.lastHeader(key));
        }

        @Override
        public Long readAsLong(Headers headers, String key) {
            return headers == null ? null : readAsLong(headers.lastHeader(key));
        }

        @Override
        public Float readAsFloat(Headers headers, String key) {
            return headers == null ? null : readAsFloat(headers.lastHeader(key));
        }

        @Override
        public Double readAsDouble(Headers headers, String key) {
            return headers == null ? null : readAsDouble(headers.lastHeader(key));
        }

        @Override
        public Boolean readAsBoolean(Headers headers, String key) {
            return headers == null ? null : readAsBoolean(headers.lastHeader(key));
        }

        @Override
        public String readAsString(Headers headers, String key, Charset charset) {
            return headers == null ? null : readAsString(headers.lastHeader(key), charset);
        }

        @Override
        public String readAsStringUtf8(Headers headers, String key) {
            return headers == null ? null : readAsStringUtf8(headers.lastHeader(key));
        }
    }

    private static class SimpleBuilder implements Builder {
        private List<Header> headers = new ArrayList<>();

        @Override
        public Builder clear() {
            headers.clear();
            return this;
        }

        @Override
        public Builder with(String key, byte[] value) {
            headers.add(new RecordHeader(key, value));
            return this;
        }

        @Override
        public Builder with(String key, ByteBuffer value) {
            if (value != null) {
                // Make sure the buffer is ready for reading
                value.rewind();
                byte[] bytes = new byte[value.remaining()];
                value.get(bytes);
                return with(key, bytes);
            }
            return with(key, value.array());
        }

        @Override
        public Builder with(String key, String value) {
            return with(key, value, StandardCharsets.UTF_8);
        }

        @Override
        public Builder with(String key, String value, Charset charset) {
            return with(key, value.getBytes(charset));
        }

        @Override
        public Builder with(String key, short value) {
            return with(key, ByteBuffer.allocate(SHORT_SIZE).putShort(value));
        }

        @Override
        public Builder with(String key, int value) {
            return with(key, ByteBuffer.allocate(INT_SIZE).putInt(value));
        }

        @Override
        public Builder with(String key, long value) {
            return with(key, ByteBuffer.allocate(LONG_SIZE).putLong(value));
        }

        @Override
        public Builder with(String key, float value) {
            return with(key, ByteBuffer.allocate(FLOAT_SIZE).putFloat(value));
        }

        @Override
        public Builder with(String key, double value) {
            return with(key, ByteBuffer.allocate(DOUBLE_SIZE).putDouble(value));
        }

        @Override
        public Builder with(String key, boolean value) {
            return with(key, ByteBuffer.allocate(SHORT_SIZE).putShort(value ? (short) 1 : 0));
        }

        @Override
        public Builder with(String key, Schema schema, Object value, Converter converter) {
            return with(key, schema, value, converter, null);
        }

        @Override
        public Builder with(String key, Schema schema, Object value, Converter converter, String topic) {
            return with(key, converter.fromConnectData(topic, schema, value));
        }

        @Override
        public Builder with(String key, SchemaAndValue value, Converter converter) {
            return with(key, value, converter, null);
        }

        @Override
        public Builder with(String key, SchemaAndValue value, Converter converter, String topic) {
            return with(key, converter.fromConnectData(topic, value.schema(), value.value()));
        }

        @Override
        public Headers build() {
            return new RecordHeaders(headers);
        }
    }
}
