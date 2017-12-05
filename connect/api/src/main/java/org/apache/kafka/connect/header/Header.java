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
package org.apache.kafka.connect.header;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * A {@link Header} is a key-value pair, and multiple headers can be included with the key, value, and timestamp in each Kafka message.
 * If the value contains schema information, then the header will have a non-null {@link #schema() schema}.
 * <p>
 * This is an immutable interface.
 */
public interface Header {

    /**
     * The header's key, which is not necessarily unique within the set of headers on a Kafka message.
     *
     * @return the header's key; never null
     */
    String key();

    /**
     * Return the {@link SchemaAndValue} associated with this header.
     *
     * @return the header's schema and value combination; never null
     */
    SchemaAndValue schemaAndValue();

    /**
     * Return the {@link Schema} associated with this header, if there is one. Not all headers will have schemas, and the schema may be
     * required to obtain the value in certain forms such as {@link #valueAsStruct()}.
     *
     * @return the header's schema, or null if no schema is associated with this header
     */
    Schema schema();

    /**
     * Get the header's value.
     *
     * @return the deserialized object representation of the header's value; may be null
     * @throws DataException if the header's value cannot be deserialized
     */
    Object valueAsObject();

    /**
     * Get the header's value as a {@code byte[]}. This is the raw value for the record's header as written to Kafka.
     *
     * @return the raw binary representation of the header's value; may be null
     * @throws DataException if the header's value cannot be converted to a byte[]
     */
    byte[] valueAsBytes() throws DataException;

    /**
     * Get the {@link Schema.Type#STRING} representation of the header's value.
     *
     * @return the header's value as a string; may be null if the header value is null
     */
    String valueAsString();

    /**
     * Get the {@link Schema.Type#INT8} representation of the header's value.
     *
     * @return the header's value as a byte; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a byte
     */
    byte valueAsByte() throws DataException;

    /**
     * Get the {@link Schema.Type#INT16} representation of the header's value.
     *
     * @return the header's value as a short; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a short
     */
    short valueAsShort() throws DataException;

    /**
     * Get the {@link Schema.Type#INT32} representation of the header's value.
     *
     * @return the header's value as an integer; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to an int
     */
    int valueAsInt() throws DataException;

    /**
     * Get the {@link Schema.Type#INT64} representation of the header's value.
     *
     * @return the header's value as a long; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a long
     */
    long valueAsLong() throws DataException;

    /**
     * Get the {@link Schema.Type#FLOAT32} representation of the header's value.
     *
     * @return the header's value as a float; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a float
     */
    float valueAsFloat() throws DataException;

    /**
     * Get the {@link Schema.Type#FLOAT64} representation of the header's value.
     *
     * @return the header's value as a double; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a double
     */
    double valueAsDouble() throws DataException;

    /**
     * Get the {@link Schema.Type#BOOLEAN} representation of the header's value.
     *
     * @return the header's value as a boolean; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a boolean
     */
    boolean valueAsBoolean() throws DataException;

    /**
     * Get the {@link Schema.Type#ARRAY} representation of the header's value.
     *
     * @param <T> the type of values expected in the list
     * @return the header's value as an ordered list; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a list, or if the values cannot be cast to the specified type
     */
    <T> List<T> valueAsList() throws DataException;

    /**
     * Get the {@link Schema.Type#MAP} representation of the header's value.
     *
     * @param <K> the type of keys expected in the map
     * @param <V> the type of values expected in the map
     * @return the header's value as an unordered map of entries; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a map, or if the keys and/or values cannot be cast to the
     *                       specified type
     */
    <K, V> Map<K, V> valueAsMap() throws DataException;

    /**
     * Get the {@link Schema.Type#STRUCT} representation of the header's value. This conversion can only be made if the header has a
     * non-null {@link #schema()}.
     *
     * @return the header's value as a struct; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to a {@link Struct}
     */
    Struct valueAsStruct() throws DataException;

    /**
     * Get the given representation of the header's value. This is equivalent to calling one of the other {@code valueAsX()} methods.
     *
     * @param type the desired type of the header's value; may not be null
     * @return the header's value as as the specified type; may be null if the header value is null
     * @throws DataException if the header's value cannot be converted to the specified type
     */
    Object valueAsType(Schema.Type type) throws DataException;

    /**
     * Get the {@link org.apache.kafka.connect.data.Date} representation of the headers' value.
     *
     * @return the header's value as a {@link org.apache.kafka.connect.data.Date}; may be null if the header value is null
     * @throws DataException if the exiting value cannot be converted to a Date
     */
    java.util.Date valueAsDate() throws DataException;

    /**
     * Get the {@link org.apache.kafka.connect.data.Time} representation of the headers' value.
     *
     * @return the header's value as a {@link org.apache.kafka.connect.data.Time}; may be null if the header value is null
     * @throws DataException if the exiting value cannot be converted to a Time
     */
    java.util.Date valueAsTime() throws DataException;

    /**
     * Get the {@link org.apache.kafka.connect.data.Timestamp} representation of the headers' value.
     *
     * @return the header's value as a {@link org.apache.kafka.connect.data.Timestamp}; may be null if the header value is null
     * @throws DataException if the exiting value cannot be converted to a Timestamp
     */
    java.util.Date valueAsTimestamp() throws DataException;

    /**
     * Get the {@link org.apache.kafka.connect.data.Decimal} representation of the headers' value.
     *
     * @param scale        the desired scale for the value
     * @param roundingMode the rounding mode to use if the setting the scale may cause rounding;
     *                     if null, {@link RoundingMode#HALF_EVEN} will be used
     * @return the header's value as a {@link org.apache.kafka.connect.data.Decimal}; may be null if the header value is null
     * @throws DataException if the exiting value cannot be converted to a Decimal
     */
    BigDecimal valueAsDecimal(int scale, RoundingMode roundingMode) throws DataException;

    /**
     * Return a new {@link Header} object that has the same key but with the supplied value.
     *
     * @param schema the schema for the new value; may be null
     * @param value  the new value
     * @return the new {@link Header}; never null
     */
    Header with(Schema schema, Object value);

    /**
     * Return a new {@link Header} object that has the same schema and value but with the supplied key.
     *
     * @param key the key for the new header; may not be null
     * @return the new {@link Header}; never null
     */
    Header rename(String key);
}
