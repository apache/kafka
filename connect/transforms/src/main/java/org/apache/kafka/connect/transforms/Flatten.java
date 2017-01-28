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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Flatten<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Flatten a nested data structure, generating names for each field by concatenating the field names at each "
                    + "level with a configurable delimiter character. Applies to Struct when schema present, or a Map "
                    + "in the case of schemaless data. The default delimiter is '.'."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getCanonicalName() + "</code>) "
                    + "or value (<code>" + Value.class.getCanonicalName() + "</code>).";

    private static final String DELIMITER_CONFIG = "delimiter";
    private static final String DELIMITER_DEFAULT = ".";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DELIMITER_CONFIG, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Delimiter to insert between field names from the input record when generating field names for the "
                            + "output record");

    private static final String PURPOSE = "flattening";

    private String delimiter;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        delimiter = config.getString(DELIMITER_CONFIG);
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

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> newValue = new LinkedHashMap<>();
        applySchemaless(value, "", newValue);
        return newRecord(record, null, newValue);
    }

    private void applySchemaless(Map<String, Object> originalRecord, String fieldNamePrefix, Map<String, Object> newRecord) {
        for (Map.Entry<String, Object> entry : originalRecord.entrySet()) {
            final String fieldName = fieldName(fieldNamePrefix, entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), null);
                return;
            }
            switch (ConnectSchema.schemaType(value.getClass())) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), entry.getValue());
                    break;
                case MAP:
                    final Map<String, Object> fieldValue = requireMap(entry.getValue(), PURPOSE);
                    applySchemaless(fieldValue, fieldName, newRecord);
                    break;
                default:
                    throw new ConnectException("Flatten transformation does not support " + entry.getValue().getClass()
                            + " for record without schemas (for field " + fieldName + ").");
            }
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct());
            buildUpdatedSchema(value.schema(), "", builder);
            updatedSchema = builder.build();
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        buildWithSchema(value, "", updatedValue);
        return newRecord(record, updatedSchema, updatedValue);

    }

    private void buildUpdatedSchema(Schema schema, String fieldNamePrefix, SchemaBuilder newSchema) {
        for (Field field : schema.fields()) {
            final String fieldName = fieldName(fieldNamePrefix, field.name());
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newSchema.field(fieldName, field.schema());
                    break;
                case STRUCT:
                    buildUpdatedSchema(field.schema(), fieldName, newSchema);
                    break;
                default:
                    throw new ConnectException("Flatten transformation does not support " + field.schema().type()
                            + " for record without schemas (for field " + fieldName + ").");
            }
        }
    }

    private void buildWithSchema(Struct record, String fieldNamePrefix, Struct newRecord) {
        for (Field field : record.schema().fields()) {
            final String fieldName = fieldName(fieldNamePrefix, field.name());
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newRecord.put(fieldName, record.get(field));
                    break;
                case STRUCT:
                    buildWithSchema(record.getStruct(field.name()), fieldName, newRecord);
                    break;
                default:
                    throw new ConnectException("Flatten transformation does not support " + field.schema().type()
                            + " for record without schemas (for field " + fieldName + ").");
            }
        }
    }

    private String fieldName(String prefix, String fieldName) {
        return prefix.isEmpty() ? fieldName : (prefix + delimiter + fieldName);
    }

    public static class Key<R extends ConnectRecord<R>> extends Flatten<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends Flatten<R> {
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
