/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.data.Schema.Type;
import org.apache.kafka.copycat.errors.SchemaProjectorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *     SchemaProjector is utility to project a value between compatible schemas and throw exceptions
 *     when non compatible schemas are provided.
 * </p>
 */

public class SchemaProjector {

    /**
     * @param writer the schema that is used to write the record
     * @param reader the schema that is used to read the record
     * @param record the value to project from writer schema to reader schema
     * @return the projected value with reader schema
     * @throws SchemaProjectorException
     */
    public static Object project(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        if (writer.isOptional() && !reader.isOptional()) {
            if (reader.defaultValue() != null) {
                if (record != null) {
                    return projectRequiredSchema(writer, reader, record);
                } else {
                    return reader.defaultValue();
                }
            } else {
                throw new SchemaProjectorException("Writer schema is optional, however, reader schema does not provide a default value.");
            }
        } else {
            if (record != null) {
                return projectRequiredSchema(writer, reader, record);
            }
            return null;
        }
    }

    private static Object projectRequiredSchema(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        Object result = null;
        // handle logical types
        if (reader.name() != null) {
            return projectLogical(writer, reader, record);
        }
        switch (reader.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case BYTES:
            case STRING:
                result = projectPrimitive(writer, reader, record);
                break;
            case STRUCT:
                result = projectStruct(writer, reader, (Struct) record);
                break;
            case ARRAY:
                result = projectArray(writer, reader, record);
                break;
            case MAP:
                result = projectMap(writer, reader, record);
        }
        return result;
    }

    private static Object projectStruct(Schema writer, Schema reader, Struct writerStruct) throws SchemaProjectorException {
        Struct readerStruct = new Struct(reader);
        for (Field readerField : reader.fields()) {
            String fieldName = readerField.name();
            Field writerField = writer.field(fieldName);
            if (writerField != null) {
                Object writerFieldValue = writerStruct.get(fieldName);
                Object readerFieldValue = project(writerField.schema(), readerField.schema(), writerFieldValue);
                readerStruct.put(fieldName, readerFieldValue);
            } else {
                Object readerDefault;
                if (readerField.schema().defaultValue() != null) {
                    readerDefault = readerField.schema().defaultValue();
                } else {
                    throw new SchemaProjectorException("Cannot project " + writer.schema() + " to " + reader.schema());
                }
                readerStruct.put(fieldName, readerDefault);
            }
        }
        return readerStruct;
    }

    private static Object projectArray(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        if (writer.type() != reader.type()) {
            throw new SchemaProjectorException("Schema type mismatch. Writer type: " + writer.type() + " and Reader type: " + reader.type());
        }
        Type writerValueType = writer.valueSchema().type();
        Type readerValueType = reader.valueSchema().type();
        if (writerValueType != readerValueType && !promote(writerValueType, readerValueType)) {
            throw new SchemaProjectorException("Value schema type mismatch for array. Writer value schema type: " + writer.type() + " and Reader value schema type: " + reader.type());
        }
        List<?> array = (List<?>) record;
        List<Object> retArray = new ArrayList<>();
        for (Object entry : array) {
            retArray.add(project(writer.valueSchema(), reader.valueSchema(), entry));
        }
        return retArray;
    }

    private static Object projectMap(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        if (writer.type() != reader.type()) {
            throw new SchemaProjectorException("Schema type mismatch. Writer type: " + writer.type() + " and Reader type: " + reader.type());
        }
        Type writerKeyType = writer.keySchema().type();
        Type readerKeyType = reader.keySchema().type();
        if (writerKeyType != readerKeyType && !promote(writerKeyType, readerKeyType)) {
            throw new SchemaProjectorException("Key schema type mismatch for array. Writer key schema type: " + writer.type() + " and Reader key schema type: " + reader.type());
        }
        Type writerValueType = writer.valueSchema().type();
        Type readerValueType = reader.valueSchema().type();
        if (writerValueType != readerValueType && !promote(writerValueType, readerValueType)) {
            throw new SchemaProjectorException("Value schema type mismatch for array. Writer value schema type: " + writer.type() + " and Reader value schema type: " + reader.type());
        }
        Map<?, ?> map = (Map<?, ?>) record;
        Map<Object, Object> retMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Object retKey = project(writer.keySchema(), reader.keySchema(), key);
            Object retValue = project(writer.valueSchema(), writer.valueSchema(), value);
            retMap.put(retKey, retValue);
        }
        return retMap;
    }

    private static Object projectLogical(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        if (writer.name() == null || !writer.name().equals(reader.name())) {
            throw new SchemaProjectorException("Schema does not match for " + writer.name() + " and " + reader.name());
        }

        switch (reader.name()) {
            case Decimal.LOGICAL_NAME:
            case Date.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
                return record;
            default:
                throw new SchemaProjectorException("Not a valid logical type: " + reader.name());
        }
    }

    private static Object projectPrimitive(Schema writer, Schema reader, Object record) throws SchemaProjectorException {
        if (writer.type() != reader.type() && !promote(writer.type(), reader.type())) {
            throw new SchemaProjectorException("Schema type mismatch. Writer type: " + writer.type() + " and Reader type: " + reader.type());
        }
        if (writer.isOptional() && !reader.isOptional()) {
            if (reader.defaultValue() != null) {
                if (record != null) {
                    return record;
                } else {
                    return reader.defaultValue();
                }
            } else {
                throw new SchemaProjectorException("Writer schema is optional, however, reader schema does not provide a default value.");
            }
        } else {
            return record;
        }
    }

    private static boolean promote(Type writerType, Type readerType) {
        Type[] numericTypes = {Type.INT8, Type.INT16, Type.INT32, Type.INT64, Type.FLOAT32, Type.FLOAT64};
        int index = -1;
        for (int i = 0; i < numericTypes.length; ++i) {
            if (writerType.equals(numericTypes[i])) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            return false;
        }
        boolean result = false;
        for (int i = index; i < numericTypes.length; ++i) {
            result = result || readerType.equals(numericTypes[i]);
        }
        return result;
    }
}
