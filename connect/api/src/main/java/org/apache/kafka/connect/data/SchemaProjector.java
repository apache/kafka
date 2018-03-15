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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.SchemaProjectorException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * <p>
 *     SchemaProjector is utility to project a value between compatible schemas and throw exceptions
 *     when non compatible schemas are provided.
 * </p>
 */

public class SchemaProjector {

    private static Set<AbstractMap.SimpleImmutableEntry<Type, Type>> promotable = new HashSet<>();

    static {
        Type[] promotableTypes = {Type.INT8, Type.INT16, Type.INT32, Type.INT64, Type.FLOAT32, Type.FLOAT64};
        for (int i = 0; i < promotableTypes.length; ++i) {
            for (int j = i; j < promotableTypes.length; ++j) {
                promotable.add(new AbstractMap.SimpleImmutableEntry<>(promotableTypes[i], promotableTypes[j]));
            }
        }
    }

    /**
     * This method project a value between compatible schemas and throw exceptions when non compatible schemas are provided
     * @param source the schema used to construct the record
     * @param record the value to project from source schema to target schema
     * @param target the schema to project the record to
     * @return the projected value with target schema
     * @throws SchemaProjectorException
     */
    public static Object project(Schema source, Object record, Schema target) throws SchemaProjectorException {
        checkMaybeCompatible(source, target);
        if (source.isOptional() && !target.isOptional()) {
            if (target.defaultValue() != null) {
                if (record != null) {
                    return projectRequiredSchema(source, record, target);
                } else {
                    return target.defaultValue();
                }
            } else {
                throw new SchemaProjectorException("Writer schema is optional, however, target schema does not provide a default value.");
            }
        } else {
            if (record != null) {
                return projectRequiredSchema(source, record, target);
            } else {
                return null;
            }
        }
    }

    private static Object projectRequiredSchema(Schema source, Object record, Schema target) throws SchemaProjectorException {
        switch (target.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case BYTES:
            case STRING:
                return projectPrimitive(source, record, target);
            case STRUCT:
                return projectStruct(source, (Struct) record, target);
            case ARRAY:
                return projectArray(source, record, target);
            case MAP:
                return projectMap(source, record, target);
        }
        return null;
    }

    private static Object projectStruct(Schema source, Struct sourceStruct, Schema target) throws SchemaProjectorException {
        Struct targetStruct = new Struct(target);
        for (Field targetField : target.fields()) {
            String fieldName = targetField.name();
            Field sourceField = source.field(fieldName);
            if (sourceField != null) {
                Object sourceFieldValue = sourceStruct.get(fieldName);
                try {
                    Object targetFieldValue = project(sourceField.schema(), sourceFieldValue, targetField.schema());
                    targetStruct.put(fieldName, targetFieldValue);
                } catch (SchemaProjectorException e) {
                    throw new SchemaProjectorException("Error projecting " + sourceField.name(), e);
                }
            } else if (targetField.schema().isOptional()) {
                // Ignore missing field
            } else if (targetField.schema().defaultValue() != null) {
                targetStruct.put(fieldName, targetField.schema().defaultValue());
            } else {
                throw new SchemaProjectorException("Required field `" +  fieldName + "` is missing from source schema: " + source);
            }
        }
        return targetStruct;
    }


    private static void checkMaybeCompatible(Schema source, Schema target) {
        if (source.type() != target.type() && !isPromotable(source.type(), target.type())) {
            throw new SchemaProjectorException("Schema type mismatch. source type: " + source.type() + " and target type: " + target.type());
        } else if (!Objects.equals(source.name(), target.name())) {
            throw new SchemaProjectorException("Schema name mismatch. source name: " + source.name() + " and target name: " + target.name());
        } else if (!Objects.equals(source.parameters(), target.parameters())) {
            throw new SchemaProjectorException("Schema parameters not equal. source parameters: " + source.parameters() + " and target parameters: " + target.parameters());
        }
    }

    private static Object projectArray(Schema source, Object record, Schema target) throws SchemaProjectorException {
        List<?> array = (List<?>) record;
        List<Object> retArray = new ArrayList<>();
        for (Object entry : array) {
            retArray.add(project(source.valueSchema(), entry, target.valueSchema()));
        }
        return retArray;
    }

    private static Object projectMap(Schema source, Object record, Schema target) throws SchemaProjectorException {
        Map<?, ?> map = (Map<?, ?>) record;
        Map<Object, Object> retMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Object retKey = project(source.keySchema(), key, target.keySchema());
            Object retValue = project(source.valueSchema(), value, target.valueSchema());
            retMap.put(retKey, retValue);
        }
        return retMap;
    }

    private static Object projectPrimitive(Schema source, Object record, Schema target) throws SchemaProjectorException {
        assert source.type().isPrimitive();
        assert target.type().isPrimitive();
        Object result;
        if (isPromotable(source.type(), target.type())) {
            Number numberRecord = (Number) record;
            switch (target.type()) {
                case INT8:
                    result = numberRecord.byteValue();
                    break;
                case INT16:
                    result = numberRecord.shortValue();
                    break;
                case INT32:
                    result = numberRecord.intValue();
                    break;
                case INT64:
                    result = numberRecord.longValue();
                    break;
                case FLOAT32:
                    result = numberRecord.floatValue();
                    break;
                case FLOAT64:
                    result = numberRecord.doubleValue();
                    break;
                default:
                    throw new SchemaProjectorException("Not promotable type.");
            }
        } else {
            result = record;
        }
        return result;
    }

    private static boolean isPromotable(Type sourceType, Type targetType) {
        return promotable.contains(new AbstractMap.SimpleImmutableEntry<>(sourceType, targetType));
    }
}
