/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.ByteBuffer;

import org.apache.kafka.common.utils.Utils;

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
     * The Boolean type represents a boolean value in a byte by using
     * the value of 0 to represent false, and 1 to represent true.
     *
     * If for some reason a value that is not 0 or 1 is read,
     * then any non-zero value will return true.
     */
    public static final Type BOOLEAN = new Type() {
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
        public String toString() {
            return "BOOLEAN";
        }

        @Override
        public Boolean validate(Object item) {
            if (item instanceof Boolean)
                return (Boolean) item;
            else
                throw new SchemaException(item + " is not a Boolean.");
        }
    };

    public static final Type INT8 = new Type() {
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
        public String toString() {
            return "INT8";
        }

        @Override
        public Byte validate(Object item) {
            if (item instanceof Byte)
                return (Byte) item;
            else
                throw new SchemaException(item + " is not a Byte.");
        }
    };

    public static final Type INT16 = new Type() {
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
        public String toString() {
            return "INT16";
        }

        @Override
        public Short validate(Object item) {
            if (item instanceof Short)
                return (Short) item;
            else
                throw new SchemaException(item + " is not a Short.");
        }
    };

    public static final Type INT32 = new Type() {
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
        public String toString() {
            return "INT32";
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer)
                return (Integer) item;
            else
                throw new SchemaException(item + " is not an Integer.");
        }
    };

    public static final Type INT64 = new Type() {
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
        public String toString() {
            return "INT64";
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long)
                return (Long) item;
            else
                throw new SchemaException(item + " is not a Long.");
        }
    };

    public static final Type STRING = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE)
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                throw new SchemaException("String length " + length + " cannot be negative");
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");

            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
            return "STRING";
        }

        @Override
        public String validate(Object item) {
            if (item instanceof String)
                return (String) item;
            else
                throw new SchemaException(item + " is not a String.");
        }
    };

    public static final Type NULLABLE_STRING = new Type() {
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
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0)
                return null;
            if (length > buffer.remaining())
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");

            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null)
                return 2;

            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
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
    };

    public static final Type BYTES = new Type() {
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
        public String toString() {
            return "BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item instanceof ByteBuffer)
                return (ByteBuffer) item;
            else
                throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }
    };

    public static final Type NULLABLE_BYTES = new Type() {
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
        public String toString() {
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
    };


}
