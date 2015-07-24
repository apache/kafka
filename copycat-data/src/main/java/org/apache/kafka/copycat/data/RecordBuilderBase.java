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
 **/



package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.data.Schema.Field;
import org.apache.kafka.copycat.data.Schema.Type;

import java.io.IOException;
import java.util.Arrays;

/** Abstract base class for RecordBuilder implementations.  Not thread-safe. */
public abstract class RecordBuilderBase<T extends IndexedRecord>
        implements RecordBuilder<T> {
    private static final Field[] EMPTY_FIELDS = new Field[0];
    private final Schema schema;
    private final Field[] fields;
    private final boolean[] fieldSetFlags;
    private final GenericData data;

    protected final Schema schema() {
        return schema;
    }

    protected final Field[] fields() {
        return fields;
    }

    protected final boolean[] fieldSetFlags() {
        return fieldSetFlags;
    }

    protected final GenericData data() {
        return data;
    }

    /**
     * Creates a RecordBuilderBase for building records of the given type.
     * @param schema the schema associated with the record class.
     */
    protected RecordBuilderBase(Schema schema, GenericData data) {
        this.schema = schema;
        this.data = data;
        fields = (Field[]) schema.getFields().toArray(EMPTY_FIELDS);
        fieldSetFlags = new boolean[fields.length];
    }

    /**
     * RecordBuilderBase copy constructor.
     * Makes a deep copy of the values in the other builder.
     * @param other RecordBuilderBase instance to copy.
     */
    protected RecordBuilderBase(RecordBuilderBase<T> other, GenericData data) {
        this.schema = other.schema;
        this.data = data;
        fields = (Field[]) schema.getFields().toArray(EMPTY_FIELDS);
        fieldSetFlags = new boolean[other.fieldSetFlags.length];
        System.arraycopy(
                other.fieldSetFlags, 0, fieldSetFlags, 0, fieldSetFlags.length);
    }

    /**
     * Validates that a particular value for a given field is valid according to
     * the following algorithm:
     * 1. If the value is not null, or the field type is null, or the field type
     * is a union which accepts nulls, returns.
     * 2. Else, if the field has a default value, returns.
     * 3. Otherwise throws AvroRuntimeException.
     * @param field the field to validate.
     * @param value the value to validate.
     * @throws NullPointerException if value is null and the given field does
     * not accept null values.
     */
    protected void validate(Field field, Object value) {
        if (isValidValue(field, value)) {
            return;
        } else if (field.defaultValue() != null) {
            return;
        } else {
            throw new DataRuntimeException(
                    "Field " + field + " does not accept null values");
        }
    }

    /**
     * Tests whether a value is valid for a specified field.
     * @param f the field for which to test the value.
     * @param value the value to test.
     * @return true if the value is valid for the given field; false otherwise.
     */
    protected static boolean isValidValue(Field f, Object value) {
        if (value != null) {
            return true;
        }

        Schema schema = f.schema();
        Type type = schema.getType();

        // If the type is null, any value is valid
        if (type == Type.NULL) {
            return true;
        }

        // If the type is a union that allows nulls, any value is valid
        if (type == Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() == Type.NULL) {
                    return true;
                }
            }
        }

        // The value is null but the type does not allow nulls
        return false;
    }

    /**
     * Gets the default value of the given field, if any.
     * @param field the field whose default value should be retrieved.
     * @return the default value associated with the given field,
     * or null if none is specified in the schema.
     * @throws IOException
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Object defaultValue(Field field) throws IOException {
        return data.deepCopy(field.schema(), data.getDefaultValue(field));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(fieldSetFlags);
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
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
        @SuppressWarnings("rawtypes")
        RecordBuilderBase other = (RecordBuilderBase) obj;
        if (!Arrays.equals(fieldSetFlags, other.fieldSetFlags))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        return true;
    }
}
