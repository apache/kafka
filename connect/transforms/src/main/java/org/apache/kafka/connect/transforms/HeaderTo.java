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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class HeaderTo<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
        "Moves or copy headers on a record into fields in that record's key/value";

    private static final String OPERATION_COPY = "copy";
    private static final String OPERATION_MOVE = "move";

    private static final String HEADERS_CONFIG = "headers";
    private static final String FIELDS_CONFIG = "fields";
    private static final String OPERATION_CONFIG = "operation";
    private static final String OPERATION_DEFAULT_VALUE = OPERATION_COPY;
    private static final ConfigDef.ValidString OPERATION_VALIDATOR = ConfigDef.ValidString.in(OPERATION_COPY, OPERATION_MOVE);

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(HEADERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
            new NonEmptyListValidator(), ConfigDef.Importance.MEDIUM,
            "Comma-separated list of header names to copy/move.")
        .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
            new NonEmptyListValidator(), ConfigDef.Importance.MEDIUM,
            "Comma-separated list of field names to be updated with the corresponding header value, in the same order as the field names listed in the fields configuration property.")
        .define(OPERATION_CONFIG, ConfigDef.Type.STRING, OPERATION_DEFAULT_VALUE,
            OPERATION_VALIDATOR, ConfigDef.Importance.MEDIUM,
            "Either move if the headers are to be moved, or copy if the headers are to be just copied and left on the record.");
        
    private static final String PURPOSE = "insert field from header";

    private List<String> headersConfig;
    private List<String> fieldsConfig;
    private String operationConfig;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headersConfig = config.getList(HEADERS_CONFIG);
        fieldsConfig = config.getList(FIELDS_CONFIG);
        operationConfig = config.getString(OPERATION_CONFIG);
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

    private boolean isTombstoneRecord(R record) {
        return record.value() == null;
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value;

        Object rawValue = operatingValue(record);
        if (rawValue == null) {
            value = new HashMap<>();
        } else {
            value = requireMap(rawValue, PURPOSE);
        }
            
        final Map<String, Object> updatedValue = new HashMap<>(value);

        Headers updatedHeaders = new ConnectHeaders(record.headers());

        int length = Math.min(headersConfig.size(), fieldsConfig.size());

        for (int i = 0; i < length; i++) {
            String headerName = headersConfig.get(i);
            String fieldName = fieldsConfig.get(i);
            Header header = updatedHeaders.lastWithName(headerName);
            if (header != null) {
                updatedValue.put(fieldName, header.value());

                // Remove header if operation is move
                if (operationConfig == OPERATION_MOVE)
                    updatedHeaders.remove(headerName);
            }
        }

        return newRecord(record, null, updatedValue, updatedHeaders);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        Headers updatedHeaders = new ConnectHeaders(record.headers());

        int length = Math.min(headersConfig.size(), fieldsConfig.size());

        for (int i = 0; i < length; i++) {
            String headerName = headersConfig.get(i);
            String fieldName = fieldsConfig.get(i);
            Header header = updatedHeaders.lastWithName(headerName);
            if (header != null) {
                updatedValue.put(fieldName, header.value());

                // Remove header if operation is move
                if (operationConfig == OPERATION_MOVE)
                    updatedHeaders.remove(headerName);
            }
        }

        return newRecord(record, updatedSchema, updatedValue, updatedHeaders);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        for (String fieldName : fieldsConfig) {
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }

        return builder.build();
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Headers updatedHeaders);

    public static class Key<R extends ConnectRecord<R>> extends HeaderTo<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Headers updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends HeaderTo<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Headers updatedHeaders) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp(), updatedHeaders);
        }

    }
}
