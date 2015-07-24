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

import org.apache.kafka.copycat.data.GenericData.Record;
import org.apache.kafka.copycat.data.Schema.Field;

import java.io.IOException;

/** A RecordBuilder for generic records. GenericRecordBuilder fills in default values
 * for fields if they are not specified.  */
public class GenericRecordBuilder extends RecordBuilderBase<Record> {
    private final GenericData.Record record;

    /**
     * Creates a GenericRecordBuilder for building Record instances.
     * @param schema the schema associated with the record class.
     */
    public GenericRecordBuilder(Schema schema) {
        super(schema, GenericData.get());
        record = new GenericData.Record(schema);
    }

    /**
     * Creates a GenericRecordBuilder by copying an existing GenericRecordBuilder.
     * @param other the GenericRecordBuilder to copy.
     */
    public GenericRecordBuilder(GenericRecordBuilder other) {
        super(other, GenericData.get());
        record = new GenericData.Record(other.record, /* deepCopy = */ true);
    }

    /**
     * Creates a GenericRecordBuilder by copying an existing record instance.
     * @param other the record instance to copy.
     */
    public GenericRecordBuilder(Record other) {
        super(other.getSchema(), GenericData.get());
        record = new GenericData.Record(other, /* deepCopy = */ true);

        // Set all fields in the RecordBuilder that are set in the record
        for (Field f : schema().getFields()) {
            Object value = other.get(f.pos());
            // Only set the value if it is not null, if the schema type is null,
            // or if the schema type is a union that accepts nulls.
            if (isValidValue(f, value)) {
                set(f, data().deepCopy(f.schema(), value));
            }
        }
    }

    /**
     * Gets the value of a field.
     * @param fieldName the name of the field to get.
     * @return the value of the field with the given name, or null if not set.
     */
    public Object get(String fieldName) {
        return get(schema().getField(fieldName));
    }

    /**
     * Gets the value of a field.
     * @param field the field to get.
     * @return the value of the given field, or null if not set.
     */
    public Object get(Field field) {
        return get(field.pos());
    }

    /**
     * Gets the value of a field.
     * @param pos the position of the field to get.
     * @return the value of the field with the given position, or null if not set.
     */
    protected Object get(int pos) {
        return record.get(pos);
    }

    /**
     * Sets the value of a field.
     * @param fieldName the name of the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    public GenericRecordBuilder set(String fieldName, Object value) {
        return set(schema().getField(fieldName), value);
    }

    /**
     * Sets the value of a field.
     * @param field the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    public GenericRecordBuilder set(Field field, Object value) {
        return set(field, field.pos(), value);
    }

    /**
     * Sets the value of a field.
     * @param pos the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    protected GenericRecordBuilder set(int pos, Object value) {
        return set(fields()[pos], pos, value);
    }

    /**
     * Sets the value of a field.
     * @param field the field to set.
     * @param pos the position of the field.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    private GenericRecordBuilder set(Field field, int pos, Object value) {
        validate(field, value);
        record.put(pos, value);
        fieldSetFlags()[pos] = true;
        return this;
    }

    /**
     * Checks whether a field has been set.
     * @param fieldName the name of the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    public boolean has(String fieldName) {
        return has(schema().getField(fieldName));
    }

    /**
     * Checks whether a field has been set.
     * @param field the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    public boolean has(Field field) {
        return has(field.pos());
    }

    /**
     * Checks whether a field has been set.
     * @param pos the position of the field to check.
     * @return true if the given field is non-null; false otherwise.
     */
    protected boolean has(int pos) {
        return fieldSetFlags()[pos];
    }

    /**
     * Clears the value of the given field.
     * @param fieldName the name of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    public GenericRecordBuilder clear(String fieldName) {
        return clear(schema().getField(fieldName));
    }

    /**
     * Clears the value of the given field.
     * @param field the field to clear.
     * @return a reference to the RecordBuilder.
     */
    public GenericRecordBuilder clear(Field field) {
        return clear(field.pos());
    }

    /**
     * Clears the value of the given field.
     * @param pos the position of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    protected GenericRecordBuilder clear(int pos) {
        record.put(pos, null);
        fieldSetFlags()[pos] = false;
        return this;
    }

    @Override
    public Record build() {
        Record record;
        try {
            record = new GenericData.Record(schema());
        } catch (Exception e) {
            throw new DataRuntimeException(e);
        }

        for (Field field : fields()) {
            Object value;
            try {
                value = getWithDefault(field);
            } catch (IOException e) {
                throw new DataRuntimeException(e);
            }
            if (value != null) {
                record.put(field.pos(), value);
            }
        }

        return record;
    }

    /**
     * Gets the value of the given field.
     * If the field has been set, the set value is returned (even if it's null).
     * If the field hasn't been set and has a default value, the default value
     * is returned.
     * @param field the field whose value should be retrieved.
     * @return the value set for the given field, the field's default value,
     * or null.
     * @throws IOException
     */
    private Object getWithDefault(Field field) throws IOException {
        return fieldSetFlags()[field.pos()] ?
                record.get(field.pos()) : defaultValue(field);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((record == null) ? 0 : record.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        GenericRecordBuilder other = (GenericRecordBuilder) obj;
        if (record == null) {
            if (other.record != null)
                return false;
        } else if (!record.equals(other.record))
            return false;
        return true;
    }
}
