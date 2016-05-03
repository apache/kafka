/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.protocol.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A record that can be serialized and deserialized according to a pre-defined schema
 */
public class Struct {
    private final Schema schema;
    private final Object[] values;

    Struct(Schema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public Struct(Schema schema) {
        this.schema = schema;
        this.values = new Object[this.schema.numFields()];
    }

    /**
     * The schema for this struct.
     */
    public Schema schema() {
        return this.schema;
    }

    /**
     * Return the value of the given pre-validated field, or if the value is missing return the default value.
     *
     * @param field The field for which to get the default value
     * @throws SchemaException if the field has no value and has no default.
     */
    private Object getFieldOrDefault(Field field) {
        Object value = this.values[field.index];
        if (value != null)
            return value;
        else if (field.defaultValue != Field.NO_DEFAULT)
            return field.defaultValue;
        else if (field.type.isNullable())
            return null;
        else
            throw new SchemaException("Missing value for field '" + field.name + "' which has no default value.");
    }

    /**
     * Get the value for the field directly by the field index with no lookup needed (faster!)
     *
     * @param field The field to look up
     * @return The value for that field.
     * @throws SchemaException if the field has no value and has no default.
     */
    public Object get(Field field) {
        validateField(field);
        return getFieldOrDefault(field);
    }

    /**
     * Get the record value for the field with the given name by doing a hash table lookup (slower!)
     *
     * @param name The name of the field
     * @return The value in the record
     * @throws SchemaException If no such field exists
     */
    public Object get(String name) {
        Field field = schema.get(name);
        if (field == null)
            throw new SchemaException("No such field: " + name);
        return getFieldOrDefault(field);
    }

    /**
     * Check if the struct contains a field.
     * @param name
     * @return Whether a field exists.
     */
    public boolean hasField(String name) {
        return schema.get(name) != null;
    }

    public Struct getStruct(Field field) {
        return (Struct) get(field);
    }

    public Struct getStruct(String name) {
        return (Struct) get(name);
    }

    public Byte getByte(Field field) {
        return (Byte) get(field);
    }

    public byte getByte(String name) {
        return (Byte) get(name);
    }

    public Short getShort(Field field) {
        return (Short) get(field);
    }

    public Short getShort(String name) {
        return (Short) get(name);
    }

    public Integer getInt(Field field) {
        return (Integer) get(field);
    }

    public Integer getInt(String name) {
        return (Integer) get(name);
    }

    public Long getLong(Field field) {
        return (Long) get(field);
    }

    public Long getLong(String name) {
        return (Long) get(name);
    }

    public Object[] getArray(Field field) {
        return (Object[]) get(field);
    }

    public Object[] getArray(String name) {
        return (Object[]) get(name);
    }

    public String getString(Field field) {
        return (String) get(field);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    public Boolean getBoolean(Field field) {
        return (Boolean) get(field);
    }

    public Boolean getBoolean(String name) {
        return (Boolean) get(name);
    }

    public ByteBuffer getBytes(Field field) {
        Object result = get(field);
        if (result instanceof byte[])
            return ByteBuffer.wrap((byte[]) result);
        return (ByteBuffer) result;
    }

    public ByteBuffer getBytes(String name) {
        Object result = get(name);
        if (result instanceof byte[])
            return ByteBuffer.wrap((byte[]) result);
        return (ByteBuffer) result;
    }

    /**
     * Set the given field to the specified value
     *
     * @param field The field
     * @param value The value
     * @throws SchemaException If the validation of the field failed
     */
    public Struct set(Field field, Object value) {
        validateField(field);
        this.values[field.index] = value;
        return this;
    }

    /**
     * Set the field specified by the given name to the value
     *
     * @param name The name of the field
     * @param value The value to set
     * @throws SchemaException If the field is not known
     */
    public Struct set(String name, Object value) {
        Field field = this.schema.get(name);
        if (field == null)
            throw new SchemaException("Unknown field: " + name);
        this.values[field.index] = value;
        return this;
    }

    /**
     * Create a struct for the schema of a container type (struct or array). Note that for array type, this method
     * assumes that the type is an array of schema and creates a struct of that schema. Arrays of other types can't be
     * instantiated with this method.
     *
     * @param field The field to create an instance of
     * @return The struct
     * @throws SchemaException If the given field is not a container type
     */
    public Struct instance(Field field) {
        validateField(field);
        if (field.type() instanceof Schema) {
            return new Struct((Schema) field.type());
        } else if (field.type() instanceof ArrayOf) {
            ArrayOf array = (ArrayOf) field.type();
            return new Struct((Schema) array.type());
        } else {
            throw new SchemaException("Field '" + field.name + "' is not a container type, it is of type " + field.type());
        }
    }

    /**
     * Create a struct instance for the given field which must be a container type (struct or array)
     *
     * @param field The name of the field to create (field must be a schema type)
     * @return The struct
     * @throws SchemaException If the given field is not a container type
     */
    public Struct instance(String field) {
        return instance(schema.get(field));
    }

    /**
     * Empty all the values from this record
     */
    public void clear() {
        Arrays.fill(this.values, null);
    }

    /**
     * Get the serialized size of this object
     */
    public int sizeOf() {
        return this.schema.sizeOf(this);
    }

    /**
     * Write this struct to a buffer
     */
    public void writeTo(ByteBuffer buffer) {
        this.schema.write(buffer, this);
    }

    /**
     * Ensure the user doesn't try to access fields from the wrong schema
     *
     * @throws SchemaException If validation fails
     */
    private void validateField(Field field) {
        if (this.schema != field.schema)
            throw new SchemaException("Attempt to access field '" + field.name + "' from a different schema instance.");
        if (field.index > values.length)
            throw new SchemaException("Invalid field index: " + field.index);
    }

    /**
     * Validate the contents of this struct against its schema
     *
     * @throws SchemaException If validation fails
     */
    public void validate() {
        this.schema.validate(this);
    }

    /**
     * Create a byte buffer containing the serialized form of the values in this struct. This method can choose to break
     * the struct into multiple ByteBuffers if need be.
     */
    public ByteBuffer[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeOf());
        writeTo(buffer);
        return new ByteBuffer[] {buffer};
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            b.append(f.name);
            b.append('=');
            if (f.type() instanceof ArrayOf && this.values[i] != null) {
                Object[] arrayValue = (Object[]) this.values[i];
                b.append('[');
                for (int j = 0; j < arrayValue.length; j++) {
                    b.append(arrayValue[j]);
                    if (j < arrayValue.length - 1)
                        b.append(',');
                }
                b.append(']');
            } else
                b.append(this.values[i]);
            if (i < this.values.length - 1)
                b.append(',');
        }
        b.append('}');
        return b.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            if (f.type() instanceof ArrayOf) {
                if (this.get(f) != null) {
                    Object[] arrayObject = (Object []) this.get(f);
                    for (Object arrayItem: arrayObject)
                        result = prime * result + arrayItem.hashCode();
                }
            } else {
                Object field = this.get(f);
                if (field != null) {
                    result = prime * result + field.hashCode();
                }
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Struct other = (Struct) obj;
        if (schema != other.schema)
            return false;
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            boolean result;
            if (f.type() instanceof ArrayOf) {
                result = Arrays.equals((Object[]) this.get(f), (Object[]) other.get(f));
            } else {
                Object thisField = this.get(f);
                Object otherField = other.get(f);
                result = (thisField == null && otherField == null) || thisField.equals(otherField);
            }
            if (!result)
                return false;
        }
        return true;
    }

}
