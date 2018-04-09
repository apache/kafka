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

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

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
     * A Type that can return its description for documentation purposes.
     */
    public static abstract class DocumentedType extends Type {

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
                throw new SchemaException(item + " is not a Long.");
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

    public static final DocumentedType STRING = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE)
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public String read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                throw new SchemaException("String length " + length + " cannot be negative");
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");
            String result = Utils.utf8(buffer, length);
            buffer.position(buffer.position() + length);
            return result;
        }

        @Override
        public int sizeOf(Object o) {
            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String typeName() {
            return "STRING";
        }

        @Override
        public String validate(Object item) {
            if (item instanceof String)
                return (String) item;
            else
                throw new SchemaException(item + " is not a String.");
        }

        @Override
        public String documentation() {
            return "Represents a sequence of characters. First the length N is given as an " + INT16 +
                    ". Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
                    "Length must not be negative.";
        }
    };

    public static final DocumentedType NULLABLE_STRING = new DocumentedType() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o == null) {
                buffer.putShort((short) -1);
                return;
            }

            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE)
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public String read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                return null;
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");
            String result = Utils.utf8(buffer, length);
            buffer.position(buffer.position() + length);
            return result;
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 2;

            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String typeName() {
            return "NULLABLE_STRING";
        }

        @Override
        public String validate(Object item) {
            if (item == null)
                return null;

            if (item instanceof String)
                return (String) item;
            else
                throw new SchemaException(item + " is not a String.");
        }

        @Override
        public String documentation() {
            return "Represents a sequence of characters or null. For non-null strings, first the length N is given as an " + INT16 +
                    ". Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
                    "A null value is encoded with length of -1 and there are no following bytes.";
        }
    };

    public static final DocumentedType BYTES = new DocumentedType() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            buffer.putInt(arg.remaining());
            buffer.put(arg);
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            if (size < 0)
                throw new SchemaException("Bytes size " + size + " cannot be negative");
            if (size > buffer.remaining())
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");

            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            ByteBuffer buffer = (ByteBuffer) o;
            return 4 + buffer.remaining();
        }

        @Override
        public String typeName() {
            return "BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item instanceof ByteBuffer)
                return (ByteBuffer) item;
            else
                throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }

        @Override
        public String documentation() {
            return "Represents a raw sequence of bytes. First the length N is given as an " + INT32 +
                    ". Then N bytes follow.";
        }
    };

    public static final DocumentedType NULLABLE_BYTES = new DocumentedType() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o == null) {
                buffer.putInt(-1);
                return;
            }

            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            buffer.putInt(arg.remaining());
            buffer.put(arg);
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            if (size < 0)
                return null;
            if (size > buffer.remaining())
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");

            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 4;

            ByteBuffer buffer = (ByteBuffer) o;
            return 4 + buffer.remaining();
        }

        @Override
        public String typeName() {
            return "NULLABLE_BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item == null)
                return null;

            if (item instanceof ByteBuffer)
                return (ByteBuffer) item;

            throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }

        @Override
        public String documentation() {
            return "Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an " + INT32 +
                    ". Then N bytes follow. A null value is encoded with length of -1 and there are no following bytes.";
        }
    };

    public static final DocumentedType RECORDS = new DocumentedType() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o instanceof FileRecords)
                throw new IllegalArgumentException("FileRecords must be written to the channel directly");
            MemoryRecords records = (MemoryRecords) o;
            NULLABLE_BYTES.write(buffer, records.buffer().duplicate());
        }

        @Override
        public Records read(ByteBuffer buffer) {
            ByteBuffer recordsBuffer = (ByteBuffer) NULLABLE_BYTES.read(buffer);
            return MemoryRecords.readableRecords(recordsBuffer);
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 4;

            Records records = (Records) o;
            return 4 + records.sizeInBytes();
        }

        @Override
        public String typeName() {
            return "RECORDS";
        }

        @Override
        public Records validate(Object item) {
            if (item == null)
                return null;

            if (item instanceof Records)
                return (Records) item;

            throw new SchemaException(item + " is not an instance of " + Records.class.getName());
        }

        @Override
        public String documentation() {
            return "Represents a sequence of Kafka records as " + NULLABLE_BYTES + ". " +
                    "For a detailed description of records see " +
                    "<a href=\"/documentation/#messageformat\">Message Sets</a>.";
        }
    };

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
                    " <a href=\"http://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>.";
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
                    " <a href=\"http://code.google.com/apis/protocolbuffers/docs/encoding.html\"> Google Protocol Buffers</a>.";
        }
    };

    private static String toHtml() {

        DocumentedType[] types = {
            BOOLEAN, INT8, INT16, INT32, INT64,
            UNSIGNED_INT32, VARINT, VARLONG,
            STRING, NULLABLE_STRING, BYTES, NULLABLE_BYTES,
            RECORDS, new ArrayOf(STRING)};
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
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }
}
