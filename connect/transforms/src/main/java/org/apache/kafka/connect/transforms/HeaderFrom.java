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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public abstract class HeaderFrom<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String FIELDS_FIELD = "fields";
    public static final String HEADERS_FIELD = "headers";
    public static final String OPERATION_FIELD = "operation";
    private static final String MOVE_OPERATION = "move";
    private static final String COPY_OPERATION = "copy";

    public static final String OVERVIEW_DOC =
            "Moves or copies fields in the key/value of a record into that record's headers. " +
                    "Corresponding elements of <code>" + FIELDS_FIELD + "</code> and " +
                    "<code>" + HEADERS_FIELD + "</code> together identify a field and the header it should be " +
                    "moved or copied to. " +
                    "Use the concrete transformation type designed for the record " +
                    "key (<code>" + Key.class.getName() + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_FIELD, ConfigDef.Type.LIST,
                    NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "Field names in the record whose values are to be copied or moved to headers.")
            .define(HEADERS_FIELD, ConfigDef.Type.LIST,
                    NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "Header names, in the same order as the field names listed in the fields configuration property.")
            .define(OPERATION_FIELD, ConfigDef.Type.STRING, NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(MOVE_OPERATION, COPY_OPERATION), ConfigDef.Importance.HIGH,
                    "Either <code>move</code> if the fields are to be moved to the headers (removed from the key/value), " +
                            "or <code>copy</code> if the fields are to be copied to the headers (retained in the key/value).");

    private final Cache<Schema, Schema> moveSchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    enum Operation {
        MOVE(MOVE_OPERATION),
        COPY(COPY_OPERATION);

        private final String name;

        Operation(String name) {
            this.name = name;
        }

        static Operation fromName(String name) {
            switch (name) {
                case MOVE_OPERATION:
                    return MOVE;
                case COPY_OPERATION:
                    return COPY;
                default:
                    throw new IllegalArgumentException();
            }
        }

        public String toString() {
            return name;
        }
    }

    private List<String> fields;

    private List<String> headers;

    private Operation operation;

    @Override
    public R apply(R record) {
        Object operatingValue = operatingValue(record);
        Schema operatingSchema = operatingSchema(record);

        if (operatingSchema == null) {
            return applySchemaless(record, operatingValue);
        } else {
            return applyWithSchema(record, operatingValue, operatingSchema);
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    private R applyWithSchema(R record, Object operatingValue, Schema operatingSchema) {
        Headers updatedHeaders = record.headers().duplicate();
        Struct value = Requirements.requireStruct(operatingValue, "header " + operation);
        final Schema updatedSchema;
        final Struct updatedValue;
        if (operation == Operation.MOVE) {
            updatedSchema = moveSchema(operatingSchema);
            updatedValue = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                updatedValue.put(field, value.get(field.name()));
            }
        } else {
            updatedSchema = operatingSchema;
            updatedValue = value;
        }
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i);
            String headerName = headers.get(i);
            Object fieldValue = value.schema().field(fieldName) != null ? value.get(fieldName) : null;
            Schema fieldSchema = operatingSchema.field(fieldName).schema();
            updatedHeaders.add(headerName, fieldValue, fieldSchema);
        }
        return newRecord(record, updatedSchema, updatedValue, updatedHeaders);
    }

    private Schema moveSchema(Schema operatingSchema) {
        Schema moveSchema = this.moveSchemaCache.get(operatingSchema);
        if (moveSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(operatingSchema, SchemaBuilder.struct());
            for (Field field : operatingSchema.fields()) {
                if (!fields.contains(field.name())) {
                    builder.field(field.name(), field.schema());
                }
            }
            moveSchema = builder.build();
            moveSchemaCache.put(operatingSchema, moveSchema);
        }
        return moveSchema;
    }

    private R applySchemaless(R record, Object operatingValue) {
        Headers updatedHeaders = record.headers().duplicate();
        Map<String, Object> value = Requirements.requireMap(operatingValue, "header " + operation);
        Map<String, Object> updatedValue = new HashMap<>(value);
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i);
            Object fieldValue = value.get(fieldName);
            String headerName = headers.get(i);
            if (operation == Operation.MOVE) {
                updatedValue.remove(fieldName);
            }
            updatedHeaders.add(headerName, fieldValue, null);
        }
        return newRecord(record, null, updatedValue, updatedHeaders);
    }

    protected abstract Object operatingValue(R record);
    protected abstract Schema operatingSchema(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders);

    public static class Key<R extends ConnectRecord<R>> extends HeaderFrom<R> {

        @Override
        public Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends HeaderFrom<R> {

        @Override
        public Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp(), updatedHeaders);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fields = config.getList(FIELDS_FIELD);
        headers = config.getList(HEADERS_FIELD);
        if (headers.size() != fields.size()) {
            throw new ConfigException(format("'%s' config must have the same number of elements as '%s' config.",
                    FIELDS_FIELD, HEADERS_FIELD));
        }
        operation = Operation.fromName(config.getString(OPERATION_FIELD));
    }
}
