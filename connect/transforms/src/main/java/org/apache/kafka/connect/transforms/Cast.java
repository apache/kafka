/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Cast<R extends ConnectRecord<R>> implements Transformation<R> {

    // TODO: Currently we only support top-level field casting. Ideally we could use a dotted notation in the spec to
    // allow casting nested fields.
    public static final String OVERVIEW_DOC =
            "Cast fields to a specific type, e.g. to force an integer field to a smaller width. Only simple primitive "
                    + "types are supported -- integers, floats, boolean, and string. "
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getCanonicalName() + "</code>) "
                    + "or value (<code>" + Value.class.getCanonicalName() + "</code>).";

    public static final String SPEC_CONFIG = "spec";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object valueObject) {
                        List<String> value = (List<String>) valueObject;
                        if (value.isEmpty()) {
                            throw new ConfigException("Must specify at least one field to cast.");
                        }
                        parseFieldTypes(value);
                    }

                    @Override
                    public String toString() {
                        return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                    }
            },
            ConfigDef.Importance.HIGH,
            "List of fields and the type to cast them to of the form field1:type,field2:type to cast fields of "
                    + "Maps or Structs. A single type to cast the entire value.");

    private static final String PURPOSE = "cast types";

    private static final Set<Schema.Type> SUPPORTED_CAST_TARGETS = new HashSet<>(
            Arrays.asList(Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
                    Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN, Schema.Type.STRING)
    );

    // As a special case for casting the entire value (e.g. the incoming key is a int64 but you know it could be an
    // int32 and want the smaller width), we use an otherwise invalid field name in the cast spec to track this.
    private static final String WHOLE_VALUE_CAST = "";

    private Map<String, Schema.Type> casts;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        casts = parseFieldTypes(config.getList(SPEC_CONFIG));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        Schema.Type recordCastType = casts.get(WHOLE_VALUE_CAST);
        if (recordCastType != null) {
            return newRecord(record, null, castType(operatingValue(record), recordCastType));
        }

        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (Map.Entry<String, Schema.Type> fieldSpec : casts.entrySet()) {
            String field = fieldSpec.getKey();
            updatedValue.put(field, castType(value.get(field), fieldSpec.getValue()));
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema.Type recordCastType = casts.get(WHOLE_VALUE_CAST);

        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        if (updatedSchema == null) {
            final SchemaBuilder builder;
            if (recordCastType != null) {
                builder = SchemaUtil.copySchemaBasics(valueSchema, convertFieldType(recordCastType));
            } else {
                builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
                for (Field field : valueSchema.fields()) {
                    SchemaBuilder fieldBuilder =
                            convertFieldType(casts.containsKey(field.name()) ? casts.get(field.name()) : field.schema().type());
                    if (field.schema().isOptional())
                        fieldBuilder.optional();
                    if (field.schema().defaultValue() != null)
                        fieldBuilder.defaultValue(castType(field.schema().defaultValue(), fieldBuilder.type()));
                    builder.field(field.name(), fieldBuilder.build());
                }
            }

            if (valueSchema.isOptional())
                builder.optional();
            if (valueSchema.defaultValue() != null)
                builder.defaultValue(valueSchema.defaultValue());

            updatedSchema = builder.build();
            schemaUpdateCache.put(valueSchema, updatedSchema);
        }

        // Whole-record casting
        if (recordCastType != null)
            return newRecord(record, updatedSchema, castType(operatingValue(record), recordCastType));

        // Casting within a struct
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            updatedValue.put(updatedSchema.field(field.name()), casts.containsKey(field.name()) ? castType(origFieldValue, casts.get(field.name())) : origFieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private SchemaBuilder convertFieldType(Schema.Type type) {
        switch (type) {
            case INT8:
                return SchemaBuilder.int8();
            case INT16:
                return SchemaBuilder.int16();
            case INT32:
                return SchemaBuilder.int32();
            case INT64:
                return SchemaBuilder.int64();
            case FLOAT32:
                return SchemaBuilder.float32();
            case FLOAT64:
                return SchemaBuilder.float64();
            case BOOLEAN:
                return SchemaBuilder.bool();
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new ConnectException("Unexpected type in Cast transformation: " + type);
        }

    }

    private static Object castType(Object value, Schema.Type targetType) {
        try {
            if (value == null) return null;

            Schema.Type inferredType = ConnectSchema.schemaType(value.getClass());
            if (inferredType == null) {
                throw new ConnectException("Cast transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            // Ensure the type we are trying to cast from is supported
            validCastType(inferredType);

            switch (targetType) {
                case INT8:
                    if (value instanceof Number)
                        return ((Number) value).byteValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? (byte) 1 : (byte) 0;
                    else if (value instanceof String)
                        return Byte.parseByte((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case INT16:
                    if (value instanceof Number)
                        return ((Number) value).shortValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? (short) 1 : (short) 0;
                    else if (value instanceof String)
                        return Short.parseShort((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case INT32:
                    if (value instanceof Number)
                        return ((Number) value).intValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? 1 : 0;
                    else if (value instanceof String)
                        return Integer.parseInt((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case INT64:
                    if (value instanceof Number)
                        return ((Number) value).longValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? (long) 1 : (long) 0;
                    else if (value instanceof String)
                        return Long.parseLong((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case FLOAT32:
                    if (value instanceof Number)
                        return ((Number) value).floatValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? 1.f : 0.f;
                    else if (value instanceof String)
                        return Float.parseFloat((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case FLOAT64:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof Boolean)
                        return ((boolean) value) ? 1. : 0.;
                    else if (value instanceof String)
                        return Double.parseDouble((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case BOOLEAN:
                    if (value instanceof Number)
                        return ((Number) value).longValue() > 0L;
                    else if (value instanceof Boolean)
                        return value;
                    else if (value instanceof String)
                        return Boolean.parseBoolean((String) value);
                    else
                        throw new ConnectException("Unexpected type in Cast transformation: " + value.getClass());
                case STRING:
                    return value.toString();
                default:
                    throw new ConnectException(targetType.toString() + " is not supported in the Cast transformation.");
            }
        } catch (NumberFormatException e) {
            throw new ConnectException("Value (" + value.toString() + ") was out of range for requested data type", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private static Map<String, Schema.Type> parseFieldTypes(List<String> mappings) {
        final Map<String, Schema.Type> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length > 2) {
                throw new ConfigException(ReplaceField.ConfigName.RENAME, mappings, "Invalid rename mapping: " + mapping);
            }
            if (parts.length == 1) {
                if (mappings.size() > 1) {
                    throw new ConfigException("Cast transformations that specify a type to cast the entire value to "
                            + "may ony specify a single cast in their spec");
                }
                m.put(WHOLE_VALUE_CAST, validCastType(Schema.Type.valueOf(parts[0].toUpperCase(Locale.ROOT))));
            } else {
                Schema.Type type;
                try {
                    type = Schema.Type.valueOf(parts[1].toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new ConfigException("Invalid type found in casting spec: " + parts[1], e);
                }
                m.put(parts[0], validCastType(type));
            }
        }
        return m;
    }

    private static Schema.Type validCastType(Schema.Type targetType) {
        if (!SUPPORTED_CAST_TARGETS.contains(targetType)) {
            throw new ConfigException("Cast transformation does not support casting to/from " + targetType
                    + "; supported types are " + SUPPORTED_CAST_TARGETS);
        }
        return targetType;
    }

    public static final class Key<R extends ConnectRecord<R>> extends Cast<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends Cast<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
