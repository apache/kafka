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

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ExtractFields<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Extract the specified fields from a Struct when schema present, or a Map in the case of schemaless data."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getCanonicalName() + "</code>) "
                    + "or value (<code>" + Value.class.getCanonicalName() + "</code>).";

    interface ConfigName {
        String FIELDS = "fields";
        String DELIM = "delimiter";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELDS, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "List of fields to extract.")
            .define(ConfigName.DELIM, ConfigDef.Type.STRING, ".".toString(), ConfigDef.Importance.LOW,
            "Delimiter for nested structure levels.");


    private static final char EFDELIM = '.';

    private static final String PURPOSE = "field extraction";

    private List<String> extractedFields;
    private char efDelim;

    private Cache<Schema, Schema> schemaUpdateCache;

    private Schema retrieveFieldSchema(String name, Schema schema) {
        for (Field field : schema.fields()) {
            if (name.equals(field.name())) {
                return field.schema();
            } else if (schema.type() == Schema.Type.STRUCT) {
                Integer dindex = name.indexOf(efDelim);
                if (dindex > 0 && dindex < (name.length() - 2)) {
                    Schema subSchema = schema.field(name.substring(0, dindex)).schema();
                    if (subSchema != null) {
                        return retrieveFieldSchema(name.substring(dindex + 1), subSchema);
                    }
                }
            }
        }
        return null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (String f : extractedFields) {
            Schema s = retrieveFieldSchema(f, schema);
            if (s != null) {
                builder.field(f, s);
            }
        }
        return builder.build();
    }

    private Schema makeUpdatedSchemaFlat(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (extractedFields.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        extractedFields = config.getList(ConfigName.FIELDS);
        efDelim = config.getString(ConfigName.DELIM).charAt(0);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    private Object retrieveField(String name, Map<String, Object> entries) {
        if (entries.get(name) != null) {
            return entries.get(name);
        } else {
            Integer dindex = name.indexOf(efDelim);
            if (dindex > 0 && dindex < (name.length() - 2)) {
                Map<String, Object> subEntries = (Map) entries.get(name.substring(0, dindex));
                if (subEntries != null) {
                    return retrieveField(name.substring(dindex + 1), subEntries);
                }
            }
        }

        return null;
    }

    private Object retrieveField(String name, Struct entries) {
        Integer dindex = name.indexOf(efDelim);
        if (dindex < 0) {
            return entries.get(name);
        } else if (dindex > 0 && dindex < (name.length() - 2)) {
            Struct subEntries = entries.getStruct(name.substring(0, dindex));
            if (subEntries != null) {
                return retrieveField(name.substring(dindex + 1), subEntries);
            }
        }

        return null;
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            // final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> value = requireMap(record.value(), PURPOSE);
            final Map<String, Object> updatedValue = new HashMap<>();

            for (String f : extractedFields) {
                // final Object fieldValue = value.get(f);
                final Object fieldValue = retrieveField(f, value);
                if (fieldValue != null) {
                    final String fieldName = f;
                    updatedValue.put(fieldName, fieldValue);
                }
            }
            // for (Map.Entry<String, Object> e : value.entrySet()) {
            //     final String fieldName = e.getKey();
            //     if (extractedFields.contains(fieldName)) {
            //         final Object fieldValue = e.getValue();
            //         updatedValue.put(fieldName, fieldValue);
            //     }
            // }

            return newRecord(record, null, updatedValue);
        } else {
            // final Struct value = requireStruct(operatingValue(record), PURPOSE);
            final Struct value = requireStruct(record.value(), PURPOSE);

            Schema updatedSchema = schemaUpdateCache.get(value.schema());
            if (updatedSchema == null) {
                updatedSchema = makeUpdatedSchema(value.schema());
                schemaUpdateCache.put(value.schema(), updatedSchema);
            }

            final Struct updatedValue = new Struct(updatedSchema);

            for (Field field : updatedSchema.fields()) {
                final Object fieldValue = retrieveField(field.name(), value);
                // final Object fieldValue = value.get(field.name());
                updatedValue.put(field.name(), fieldValue);
            }

            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtractFields<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends ExtractFields<R> {
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
