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
package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.types.nullable.CompactNullableArrayOf;
import org.apache.kafka.common.protocol.types.nullable.CompactNullableBytes;
import org.apache.kafka.common.protocol.types.nullable.CompactNullableRecords;
import org.apache.kafka.common.protocol.types.nullable.CompactNullableString;
import org.apache.kafka.common.protocol.types.nullable.NullableArrayOf;
import org.apache.kafka.common.protocol.types.nullable.NullableBytes;
import org.apache.kafka.common.protocol.types.nullable.NullableRecords;
import org.apache.kafka.common.protocol.types.nullable.NullableSchema;
import org.apache.kafka.common.protocol.types.nullable.NullableString;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * A serializable type
 */
public abstract class Type {

    /**
     * Write the typed object to the buffer
     *
     * @throws SchemaException If the object is not valid for its type
     */
    public abstract void write(ByteBuffer buffer, Object o);

    /**
     * Read the typed object from the buffer
     * Please remember to do size validation before creating the container (ex: array) for the following data
     *
     * @throws SchemaException If the object is not valid for its type
     */
    public abstract Object read(ByteBuffer buffer);

    /**
     * Validate the object. If succeeded return its typed object.
     *
     * @throws SchemaException If validation failed
     */
    public abstract Object validate(Object o);

    /**
     * Return the size of the object in bytes
     */
    public abstract int sizeOf(Object o);

    /**
     * Check if the type supports null values
     * @return whether or not null is a valid value for the type implementation
     */
    public boolean isNullable() {
        return false;
    }

    /**
     * If the type is an array, return the type of the array elements.  Otherwise, return empty.
     */
    public Optional<Type> arrayElementType() {
        return Optional.empty();
    }

    /**
     * Returns true if the type is an array.
     */
    public final boolean isArray() {
        return arrayElementType().isPresent();
    }

    /**
     * For annotation in the generated html protocol doc.
     */
    public String leftBracket() {
        return "";
    }

    /**
     * For annotation in the generated html protocol doc.
     */
    public String rightBracket() {
        return "";
    }

    /**
     * A Type that can return its description for documentation purposes.
     */
    public abstract static class DocumentedType extends Type {

        /**
         * Short name of the type to identify it in documentation;
         * @return the name of the type
         */
        public abstract String typeName();

        /**
         * Documentation of the Type.
         *
         * @return details about valid values, representation
         */
        public abstract String documentation();

        @Override
        public String toString() {
            return typeName();
        }
    }
    /**
     * The Boolean type represents a boolean value in a byte by using
     * the value of 0 to represent false, and 1 to represent true.
     *
     * If for some reason a value that is not 0 or 1 is read,
     * then any non-zero value will return true.
     */
    public static final DocumentedType BOOLEAN = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            if ((Boolean) o)
                buffer.put((byte) 1);
            else
                buffer.put((byte) 0);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            byte value = buffer.get();
            return value != 0;
        }

        @Override
        public int sizeOf(Object o) {
            return 1;
        }

        @Override
        public String typeName() {
            return "BOOLEAN";
        }

        @Override
        public Boolean validate(Object item) {
            if (item instanceof Boolean)
                return (Boolean) item;
            else
                throw new SchemaException(item + " is not a Boolean.");
        }

        @Override
        public String documentation() {
            return "Represents a boolean value in a byte. " +
                    "Values 0 and 1 are used to represent false and true respectively. " +
                    "When reading a boolean value, any non-zero value is considered true.";
        }
    };

    public static final DocumentedType INT8 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.put((Byte) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.get();
        }

        @Override
        public int sizeOf(Object o) {
            return 1;
        }

        @Override
        public String typeName() {
            return "INT8";
        }

        @Override
        public Byte validate(Object item) {
            if (item instanceof Byte)
                return (Byte) item;
            else
                throw new SchemaException(item + " is not a Byte.");
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>7</sup> and 2<sup>7</sup>-1 inclusive.";
        }
    };

    public static final DocumentedType INT16 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putShort((Short) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getShort();
        }

        @Override
        public int sizeOf(Object o) {
            return 2;
        }

        @Override
        public String typeName() {
            return "INT16";
        }

        @Override
        public Short validate(Object item) {
            if (item instanceof Short)
                return (Short) item;
            else
                throw new SchemaException(item + " is not a Short.");
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>15</sup> and 2<sup>15</sup>-1 inclusive. " +
                    "The values are encoded using two bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType UINT16 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            Integer value = (Integer) o;
            buffer.putShort((short) value.intValue());
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short value = buffer.getShort();
            return Short.toUnsignedInt(value);
        }

        @Override
        public int sizeOf(Object o) {
            return 2;
        }

        @Override
        public String typeName() {
            return "UINT16";
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer)
                return (Integer) item;
            else
                throw new SchemaException(item + " is not an a Integer (encoding an unsigned short)");
        }

        @Override
        public String documentation() {
            return "Represents an integer between 0 and 65535 inclusive. " +
                    "The values are encoded using two bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType INT32 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putInt((Integer) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getInt();
        }

        @Override
        public int sizeOf(Object o) {
            return 4;
        }

        @Override
        public String typeName() {
            return "INT32";
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer)
                return (Integer) item;
            else
                throw new SchemaException(item + " is not an Integer.");
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>31</sup> and 2<sup>31</sup>-1 inclusive. " +
                    "The values are encoded using four bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType UNSIGNED_INT32 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteUtils.writeUnsignedInt(buffer, (long) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return ByteUtils.readUnsignedInt(buffer);
        }

        @Override
        public int sizeOf(Object o) {
            return 4;
        }

        @Override
        public String typeName() {
            return "UINT32";
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long)
                return (Long) item;
            else
                throw new SchemaException(item + " is not an a Long (encoding an unsigned integer).");
        }

        @Override
        public String documentation() {
            return "Represents an integer between 0 and 2<sup>32</sup>-1 inclusive. " +
                    "The values are encoded using four bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType INT64 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putLong((Long) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getLong();
        }

        @Override
        public int sizeOf(Object o) {
            return 8;
        }

        @Override
        public String typeName() {
            return "INT64";
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long)
                return (Long) item;
            else
                throw new SchemaException(item + " is not a Long.");
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. " +
                    "The values are encoded using eight bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType UUID = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            final Uuid uuid = (Uuid) o;
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return new Uuid(buffer.getLong(), buffer.getLong());
        }

        @Override
        public int sizeOf(Object o) {
            return 16;
        }

        @Override
        public String typeName() {
            return "UUID";
        }

        @Override
        public Uuid validate(Object item) {
            if (item instanceof Uuid)
                return (Uuid) item;
            else
                throw new SchemaException(item + " is not a Uuid.");
        }

        @Override
        public String documentation() {
            return "Represents a type 4 immutable universally unique identifier (Uuid). " +
                    "The values are encoded using sixteen bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType FLOAT64 = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteUtils.writeDouble((Double) o, buffer);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return ByteUtils.readDouble(buffer);
        }

        @Override
        public int sizeOf(Object o) {
            return 8;
        }

        @Override
        public String typeName() {
            return "FLOAT64";
        }

        @Override
        public Double validate(Object item) {
            if (item instanceof Double)
                return (Double) item;
            else
                throw new SchemaException(item + " is not a Double.");
        }

        @Override
        public String documentation() {
            return "Represents a double-precision 64-bit format IEEE 754 value. " +
                    "The values are encoded using eight bytes in network byte order (big-endian).";
        }
    };

    public static final DocumentedType STRING = new StringType();

    public static final DocumentedType COMPACT_STRING = new CompactString();

    public static final DocumentedType NULLABLE_STRING = new NullableString();

    public static final DocumentedType COMPACT_NULLABLE_STRING = new CompactNullableString();

    public static final DocumentedType BYTES = new Bytes();

    public static final DocumentedType COMPACT_BYTES = new CompactBytes();

    public static final DocumentedType NULLABLE_BYTES = new NullableBytes();

    public static final DocumentedType COMPACT_NULLABLE_BYTES = new CompactNullableBytes();

    public static final DocumentedType RECORDS = new Records();

    public static final DocumentedType COMPACT_RECORDS = new CompactRecords();

    public static final DocumentedType NULLABLE_RECORDS = new NullableRecords();

    public static final DocumentedType COMPACT_NULLABLE_RECORDS = new CompactNullableRecords();

    public static final DocumentedType VARINT = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteUtils.writeVarint((Integer) o, buffer);
        }

        @Override
        public Integer read(ByteBuffer buffer) {
            return ByteUtils.readVarint(buffer);
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer)
                return (Integer) item;
            throw new SchemaException(item + " is not an integer");
        }

        public String typeName() {
            return "VARINT";
        }

        @Override
        public int sizeOf(Object o) {
            return ByteUtils.sizeOfVarint((Integer) o);
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>31</sup> and 2<sup>31</sup>-1 inclusive. " +
                    "Encoding follows the variable-length zig-zag encoding from " +
                    " <a href=\"https://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>.";
        }
    };

    public static final DocumentedType VARLONG = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteUtils.writeVarlong((Long) o, buffer);
        }

        @Override
        public Long read(ByteBuffer buffer) {
            return ByteUtils.readVarlong(buffer);
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long)
                return (Long) item;
            throw new SchemaException(item + " is not a long");
        }

        public String typeName() {
            return "VARLONG";
        }

        @Override
        public int sizeOf(Object o) {
            return ByteUtils.sizeOfVarlong((Long) o);
        }

        @Override
        public String documentation() {
            return "Represents an integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. " +
                    "Encoding follows the variable-length zig-zag encoding from " +
                    " <a href=\"https://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>.";
        }
    };

    private static String toHtml() {
        DocumentedType[] types = {
            BOOLEAN, INT8, INT16, INT32, INT64,
            UINT16, UNSIGNED_INT32, VARINT, VARLONG, UUID, FLOAT64,
            STRING, COMPACT_STRING, NULLABLE_STRING, COMPACT_NULLABLE_STRING,
            BYTES, COMPACT_BYTES, NULLABLE_BYTES, COMPACT_NULLABLE_BYTES,
            RECORDS, COMPACT_RECORDS, NULLABLE_RECORDS, COMPACT_NULLABLE_RECORDS,
            new ArrayOf(STRING), new CompactArrayOf(COMPACT_STRING), new NullableArrayOf(STRING), new CompactNullableArrayOf(STRING),
            new Schema(), new NullableSchema(new Schema())};

        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Type</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>\n");
        for (DocumentedType type : types) {
            b.append("<tr>");
            b.append("<td>");
            b.append(type.typeName());
            b.append("</td>");
            b.append("<td>");
            b.append(type.documentation());
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</tbody></table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }
}
