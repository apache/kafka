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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A mutable ordered collection of {@link Header} objects. Note that multiple headers may have the same {@link Header#key() key}.
 */
public interface Headers extends Iterable<Header> {

    /**
     * Get the number of headers in this object.
     *
     * @return the number of headers; never negative
     */
    int size();

    /**
     * Determine whether this object has no headers.
     *
     * @return true if there are no headers, or false if there is at least one header
     */
    boolean isEmpty();

    /**
     * Get the collection of {@link Header} objects whose {@link Header#key() keys} all match the specified key.
     *
     * @param key the key; may not be null
     * @return the iterator over headers with the specified key; may be null if there are no headers with the specified key
     */
    Iterator<Header> allWithName(String key);

    /**
     * Return the last {@link Header} with the specified key.
     *
     * @param key the key for the header; may not be null
     * @return the last Header, or null if there are no headers with the specified key
     */
    Header lastWithName(String key);

    /**
     * Add the given {@link Header} to this collection.
     *
     * @param header the header; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers add(Header header);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key            the header's key; may not be null
     * @param schemaAndValue the {@link SchemaAndValue} for the header; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers add(String key, SchemaAndValue schemaAndValue);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key    the header's key; may not be null
     * @param value  the header's value; may be null
     * @param schema the schema for the header's value; may not be null if the value is not null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers add(String key, Object value, Schema schema);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addString(String key, String value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addBoolean(String key, boolean value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addByte(String key, byte value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addShort(String key, short value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addInt(String key, int value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addLong(String key, long value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addFloat(String key, float value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDouble(String key, double value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addBytes(String key, byte[] value);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key    the header's key; may not be null
     * @param value  the header's value; may be null
     * @param schema the schema describing the list value; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws DataException if the header's value is invalid
     */
    Headers addList(String key, List<?> value, Schema schema);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key    the header's key; may not be null
     * @param value  the header's value; may be null
     * @param schema the schema describing the map value; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws DataException if the header's value is invalid
     */
    Headers addMap(String key, Map<?, ?> value, Schema schema);

    /**
     * Add to this collection a {@link Header} with the given key and value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws DataException if the header's value is invalid
     */
    Headers addStruct(String key, Struct value);

    /**
     * Add to this collection a {@link Header} with the given key and {@link org.apache.kafka.connect.data.Decimal} value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's {@link org.apache.kafka.connect.data.Decimal} value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDecimal(String key, BigDecimal value);

    /**
     * Add to this collection a {@link Header} with the given key and {@link org.apache.kafka.connect.data.Date} value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's {@link org.apache.kafka.connect.data.Date} value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addDate(String key, java.util.Date value);

    /**
     * Add to this collection a {@link Header} with the given key and {@link org.apache.kafka.connect.data.Time} value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's {@link org.apache.kafka.connect.data.Time} value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addTime(String key, java.util.Date value);

    /**
     * Add to this collection a {@link Header} with the given key and {@link org.apache.kafka.connect.data.Timestamp} value.
     *
     * @param key   the header's key; may not be null
     * @param value the header's {@link org.apache.kafka.connect.data.Timestamp} value; may be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers addTimestamp(String key, java.util.Date value);

    /**
     * Removes all {@link Header} objects whose {@link Header#key() key} matches the specified key.
     *
     * @param key the key; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers remove(String key);

    /**
     * Removes all but the latest {@link Header} objects whose {@link Header#key() key} matches the specified key.
     *
     * @param key the key; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers retainLatest(String key);

    /**
     * Removes all but the last {@link Header} object with each key.
     *
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers retainLatest();

    /**
     * Removes all headers from this object.
     *
     * @return this object to facilitate chaining multiple methods; never null
     */
    Headers clear();

    /**
     * Create a copy of this {@link Headers} object. The new copy will contain all of the same {@link Header} objects as this object.
     * @return the copy; never null
     */
    Headers duplicate();

    /**
     * Get all {@link Header}s, apply the transform to each and store the result in place of the original.
     *
     * @param transform the transform to apply; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws DataException if the header's value is invalid
     */
    Headers apply(HeaderTransform transform);

    /**
     * Get all {@link Header}s with the given key, apply the transform to each and store the result in place of the original.
     *
     * @param key       the header's key; may not be null
     * @param transform the transform to apply; may not be null
     * @return this object to facilitate chaining multiple methods; never null
     * @throws DataException if the header's value is invalid
     */
    Headers apply(String key, HeaderTransform transform);

    /**
     * A function to transform the supplied {@link Header}. Implementations will likely need to use {@link Header#with(Schema, Object)}
     * to create the new instance.
     */
    interface HeaderTransform {
        /**
         * Transform the given {@link Header} and return the updated {@link Header}.
         *
         * @param header the input header; never null
         * @return the new header, or null if the supplied {@link Header} is to be removed
         */
        Header apply(Header header);
    }
}
