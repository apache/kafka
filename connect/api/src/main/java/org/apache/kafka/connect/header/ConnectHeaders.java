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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * A basic {@link Headers} implementation.
 */
public class ConnectHeaders implements Headers {

    private static final int EMPTY_HASH = Objects.hash(new LinkedList<>());

    /**
     * An immutable and therefore sharable empty iterator.
     */
    private static final Iterator<Header> EMPTY_ITERATOR = new Iterator<Header>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Header next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new IllegalStateException();
        }
    };


    // This field is set lazily, but once set to a list it is never set back to null
    private LinkedList<Header> headers;

    public ConnectHeaders() {
    }

    public ConnectHeaders(Iterable<Header> original) {
        if (original == null) {
            return;
        }
        if (original instanceof ConnectHeaders) {
            ConnectHeaders originalHeaders = (ConnectHeaders) original;
            if (!originalHeaders.isEmpty()) {
                headers = new LinkedList<>(originalHeaders.headers);
            }
        } else {
            headers = new LinkedList<>();
            for (Header header : original) {
                headers.add(header);
            }
        }
    }

    @Override
    public int size() {
        return headers == null ? 0 : headers.size();
    }

    @Override
    public boolean isEmpty() {
        return headers == null ? true : headers.isEmpty();
    }

    @Override
    public Headers clear() {
        if (headers != null) {
            headers.clear();
        }
        return this;
    }

    @Override
    public Headers add(Header header) {
        Objects.requireNonNull(header, "Unable to add a null header.");
        if (headers == null) {
            headers = new LinkedList<>();
        }
        headers.add(header);
        return this;
    }

    protected Headers addWithoutValidating(String key, Object value, Schema schema) {
        return add(new ConnectHeader(key, new SchemaAndValue(schema, value)));
    }

    @Override
    public Headers add(String key, SchemaAndValue schemaAndValue) {
        checkSchemaMatches(schemaAndValue);
        return add(new ConnectHeader(key, schemaAndValue != null ? schemaAndValue : SchemaAndValue.NULL));
    }

    @Override
    public Headers add(String key, Object value, Schema schema) {
        return add(key, value != null || schema != null ? new SchemaAndValue(schema, value) : SchemaAndValue.NULL);
    }

    @Override
    public Headers addString(String key, String value) {
        return addWithoutValidating(key, value, value != null ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Override
    public Headers addBytes(String key, byte[] value) {
        return addWithoutValidating(key, value, value != null ? Schema.BYTES_SCHEMA : Schema.OPTIONAL_BYTES_SCHEMA);
    }

    @Override
    public Headers addBoolean(String key, boolean value) {
        return addWithoutValidating(key, value, Schema.BOOLEAN_SCHEMA);
    }

    @Override
    public Headers addByte(String key, byte value) {
        return addWithoutValidating(key, value, Schema.INT8_SCHEMA);
    }

    @Override
    public Headers addShort(String key, short value) {
        return addWithoutValidating(key, value, Schema.INT16_SCHEMA);
    }

    @Override
    public Headers addInt(String key, int value) {
        return addWithoutValidating(key, value, Schema.INT32_SCHEMA);
    }

    @Override
    public Headers addLong(String key, long value) {
        return addWithoutValidating(key, value, Schema.INT64_SCHEMA);
    }

    @Override
    public Headers addFloat(String key, float value) {
        return addWithoutValidating(key, value, Schema.FLOAT32_SCHEMA);
    }

    @Override
    public Headers addDouble(String key, double value) {
        return addWithoutValidating(key, value, Schema.FLOAT64_SCHEMA);
    }

    @Override
    public Headers addList(String key, List<?> value, Schema schema) {
        if (value == null) {
            return add(key, null, null);
        }
        checkSchemaType(schema, Type.ARRAY);
        return addWithoutValidating(key, value, schema);
    }

    @Override
    public Headers addMap(String key, Map<?, ?> value, Schema schema) {
        if (value == null) {
            return add(key, null, null);
        }
        checkSchemaType(schema, Type.MAP);
        return addWithoutValidating(key, value, schema);
    }

    @Override
    public Headers addStruct(String key, Struct value) {
        if (value == null) {
            return add(key, null, null);
        }
        checkSchemaType(value.schema(), Type.STRUCT);
        return addWithoutValidating(key, value, value.schema());
    }

    @Override
    public Headers addDecimal(String key, BigDecimal value) {
        if (value == null) {
            return add(key, null, null);
        }
        // Check that this is a decimal ...
        Schema schema = Decimal.schema(value.scale());
        Decimal.fromLogical(schema, value);
        return addWithoutValidating(key, value, schema);
    }

    @Override
    public Headers addDate(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a date ...
            Date.fromLogical(Date.SCHEMA, value);
        }
        return addWithoutValidating(key, value, Date.SCHEMA);
    }

    @Override
    public Headers addTime(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a time ...
            Time.fromLogical(Time.SCHEMA, value);
        }
        return addWithoutValidating(key, value, Time.SCHEMA);
    }

    @Override
    public Headers addTimestamp(String key, java.util.Date value) {
        if (value != null) {
            // Check that this is a timestamp ...
            Timestamp.fromLogical(Timestamp.SCHEMA, value);
        }
        return addWithoutValidating(key, value, Timestamp.SCHEMA);
    }

    @Override
    public Header lastWithName(String key) {
        checkKey(key);
        if (headers != null) {
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                Header header = iter.previous();
                if (key.equals(header.key())) {
                    return header;
                }
            }
        }
        return null;
    }

    @Override
    public Iterator<Header> allWithName(String key) {
        return new FilterByKeyIterator(iterator(), key);
    }

    @Override
    public Iterator<Header> iterator() {
        if (headers != null) {
            return headers.iterator();
        }
        return EMPTY_ITERATOR;
    }

    @Override
    public Headers remove(String key) {
        checkKey(key);
        if (!headers.isEmpty()) {
            Iterator<Header> iterator = iterator();
            while (iterator.hasNext()) {
                if (iterator.next().key().equals(key)) {
                    iterator.remove();
                }
            }
        }
        return this;
    }

    @Override
    public Headers retainLatest() {
        if (!headers.isEmpty()) {
            Set<String> keys = new HashSet<>();
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                Header header = iter.previous();
                String key = header.key();
                if (!keys.add(key)) {
                    iter.remove();
                }
            }
        }
        return this;
    }

    @Override
    public Headers retainLatest(String key) {
        checkKey(key);
        if (!headers.isEmpty()) {
            boolean found = false;
            ListIterator<Header> iter = headers.listIterator(headers.size());
            while (iter.hasPrevious()) {
                String headerKey = iter.previous().key();
                if (key.equals(headerKey)) {
                    if (found)
                        iter.remove();
                    found = true;
                }
            }
        }
        return this;
    }

    @Override
    public Headers apply(String key, HeaderTransform transform) {
        checkKey(key);
        if (!headers.isEmpty()) {
            ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                Header orig = iter.next();
                if (orig.key().equals(key)) {
                    Header updated = transform.apply(orig);
                    if (updated != null) {
                        iter.set(updated);
                    } else {
                        iter.remove();
                    }
                }
            }
        }
        return this;
    }

    @Override
    public Headers apply(HeaderTransform transform) {
        if (!headers.isEmpty()) {
            ListIterator<Header> iter = headers.listIterator();
            while (iter.hasNext()) {
                Header orig = iter.next();
                Header updated = transform.apply(orig);
                if (updated != null) {
                    iter.set(updated);
                } else {
                    iter.remove();
                }
            }
        }
        return this;
    }

    @Override
    public int hashCode() {
        return isEmpty() ? EMPTY_HASH : Objects.hash(headers);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Headers) {
            Headers that = (Headers) obj;
            Iterator<Header> thisIter = this.iterator();
            Iterator<Header> thatIter = that.iterator();
            while (thisIter.hasNext() && thatIter.hasNext()) {
                if (!Objects.equals(thisIter.next(), thatIter.next()))
                    return false;
            }
            return !thisIter.hasNext() && !thatIter.hasNext();
        }
        return false;
    }

    @Override
    public String toString() {
        return "ConnectHeaders(headers=" + (headers != null ? headers : "") + ")";
    }

    @Override
    public ConnectHeaders duplicate() {
        return new ConnectHeaders(this);
    }

    /**
     * Check that the key is not null
     *
     * @param key the key; may not be null
     * @throws NullPointerException if the supplied key is null
     */
    private void checkKey(String key) {
        Objects.requireNonNull(key, "Header key cannot be null");
    }

    /**
     * Check the {@link Schema#type() schema's type} matches the specified type.
     *
     * @param schema the schema; never null
     * @param type   the expected type
     * @throws DataException if the schema's type does not match the expected type
     */
    private void checkSchemaType(Schema schema, Type type) {
        if (schema.type() != type) {
            throw new DataException("Expecting " + type + " but instead found " + schema.type());
        }
    }

    /**
     * Check that the value and its schema are compatible.
     *
     * @param schemaAndValue the schema and value pair
     * @throws DataException if the schema is not compatible with the value
     */
    // visible for testing
    void checkSchemaMatches(SchemaAndValue schemaAndValue) {
        if (schemaAndValue != null) {
            Schema schema = schemaAndValue.schema();
            if (schema == null)
                return;
            schema = schema.schema(); // in case a SchemaBuilder is used
            Object value = schemaAndValue.value();
            if (value == null && !schema.isOptional()) {
                throw new DataException("A null value requires an optional schema but was " + schema);
            }
            if (value != null) {
                switch (schema.type()) {
                    case BYTES:
                        if (value instanceof ByteBuffer)
                            return;
                        if (value instanceof byte[])
                            return;
                        if (value instanceof BigDecimal && Decimal.LOGICAL_NAME.equals(schema.name()))
                            return;
                        break;
                    case STRING:
                        if (value instanceof String)
                            return;
                        break;
                    case BOOLEAN:
                        if (value instanceof Boolean)
                            return;
                        break;
                    case INT8:
                        if (value instanceof Byte)
                            return;
                        break;
                    case INT16:
                        if (value instanceof Short)
                            return;
                        break;
                    case INT32:
                        if (value instanceof Integer)
                            return;
                        if (value instanceof java.util.Date && Date.LOGICAL_NAME.equals(schema.name()))
                            return;
                        if (value instanceof java.util.Date && Time.LOGICAL_NAME.equals(schema.name()))
                            return;
                        break;
                    case INT64:
                        if (value instanceof Long)
                            return;
                        if (value instanceof java.util.Date && Timestamp.LOGICAL_NAME.equals(schema.name()))
                            return;
                        break;
                    case FLOAT32:
                        if (value instanceof Float)
                            return;
                        break;
                    case FLOAT64:
                        if (value instanceof Double)
                            return;
                        break;
                    case ARRAY:
                        if (value instanceof List)
                            return;
                        break;
                    case MAP:
                        if (value instanceof Map)
                            return;
                        break;
                    case STRUCT:
                        if (value instanceof Struct)
                            return;
                        break;
                }
                throw new DataException("The value " + value + " is not compatible with the schema " + schema);
            }
        }
    }

    private static final class FilterByKeyIterator extends AbstractIterator<Header> {

        private final Iterator<Header> original;
        private final String key;

        private FilterByKeyIterator(Iterator<Header> original, String key) {
            this.original = original;
            this.key = key;
        }

        @Override
        protected Header makeNext() {
            while (original.hasNext()) {
                Header header = original.next();
                if (!header.key().equals(key)) {
                    continue;
                }
                return header;
            }
            return this.allDone();
        }
    }
}
