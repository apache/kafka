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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class InsertField<R extends ConnectRecord<R>> implements Transformation<R> {

    public interface Keys {
        String TOPIC_FIELD = "topic.field";
        String PARTITION_FIELD = "partition.field";
        String OFFSET_FIELD = "offset.field";
        String TIMESTAMP_FIELD = "timestamp.field";
        String STATIC_FIELD = "static.field";
        String STATIC_VALUE = "static.value";
    }

    private static final String OPTIONALITY_DOC = "Suffix with '!' to make this a required field, or '?' to keep it optional (the default).";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(Keys.TOPIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka topic.\n" + OPTIONALITY_DOC)
            .define(Keys.PARTITION_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka partition.\n" + OPTIONALITY_DOC)
            .define(Keys.OFFSET_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka offset - only applicable to sink connectors.\n" + OPTIONALITY_DOC)
            .define(Keys.TIMESTAMP_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for record timestamp.\n" + OPTIONALITY_DOC)
            .define(Keys.STATIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for static data field.\n" + OPTIONALITY_DOC)
            .define(Keys.STATIC_VALUE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Static field value, if field name configured.");

    private static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build();

    private static final class InsertionSpec {
        final String name;
        final boolean optional;

        private InsertionSpec(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }

        public static InsertionSpec parse(String spec) {
            if (spec == null) return null;
            if (spec.endsWith("?")) {
                return new InsertionSpec(spec.substring(0, spec.length() - 1), true);
            }
            if (spec.endsWith("!")) {
                return new InsertionSpec(spec.substring(0, spec.length() - 1), false);
            }
            return new InsertionSpec(spec, true);
        }
    }

    private InsertionSpec topicField;
    private InsertionSpec partitionField;
    private InsertionSpec offsetField;
    private InsertionSpec timestampField;
    private InsertionSpec staticField;
    private String staticValue;
    private boolean applicable;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        topicField = InsertionSpec.parse(config.getString(Keys.TOPIC_FIELD));
        partitionField = InsertionSpec.parse(config.getString(Keys.PARTITION_FIELD));
        offsetField = InsertionSpec.parse(config.getString(Keys.OFFSET_FIELD));
        timestampField = InsertionSpec.parse(config.getString(Keys.TIMESTAMP_FIELD));
        staticField = InsertionSpec.parse(config.getString(Keys.STATIC_FIELD));
        staticValue = config.getString(Keys.STATIC_VALUE);
        applicable = topicField != null || partitionField != null || offsetField != null || timestampField != null;

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (!applicable) return record;

        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        if (value == null)
            throw new DataException("null value");

        if (schema == null) {
            if (!(value instanceof Map))
                throw new DataException("Can only operate on Map value in schemaless mode: " + value.getClass().getName());
            return applySchemaless(record, (Map<String, Object>) value);
        } else {
            if (schema.type() != Schema.Type.STRUCT)
                throw new DataException("Can only operate on Struct types: " + value.getClass().getName());
            return applyWithSchema(record, schema, (Struct) value);
        }
    }

    private R applySchemaless(R record, Map<String, Object> value) {
        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            if (!(record instanceof SinkRecord))
                throw new DataException("Offset insertion is only supported for sink connectors, record is of type: " + record.getClass());
            updatedValue.put(offsetField.name, ((SinkRecord) record).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, record.timestamp());
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record, Schema schema, Struct value) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(schema);
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        copyFields(value, updatedValue);

        insertFields(record, updatedValue);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaBuilder.struct();

        builder.name(schema.name());
        builder.version(schema.version());
        builder.doc(schema.doc());

        final Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        if (topicField != null) {
            builder.field(topicField.name, topicField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }
        if (partitionField != null) {
            builder.field(partitionField.name, partitionField.optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
        }
        if (offsetField != null) {
            builder.field(offsetField.name, offsetField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
        }
        if (timestampField != null) {
            builder.field(timestampField.name, timestampField.optional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA);
        }
        if (staticField != null) {
            builder.field(staticField.name, staticField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }

        return builder.build();
    }

    private void copyFields(Struct value, Struct updatedValue) {
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
    }

    private void insertFields(R record, Struct value) {
        if (topicField != null) {
            value.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            value.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            if (!(record instanceof SinkRecord)) {
                throw new DataException("Offset insertion is only supported for sink connectors, record is of type: " + record.getClass());
            }
            value.put(offsetField.name, ((SinkRecord) record).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            value.put(timestampField.name, new Date(record.timestamp()));
        }
        if (staticField != null && staticValue != null) {
            value.put(staticField.name, staticValue);
        }
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

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    /**
     * This transformation allows inserting configured attributes of the record metadata as fields in the record key.
     * It also allows adding a static data field.
     */
    public static class Key<R extends ConnectRecord<R>> extends InsertField<R> {

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

    /**
     * This transformation allows inserting configured attributes of the record metadata as fields in the record value.
     * It also allows adding a static data field.
     */
    public static class Value<R extends ConnectRecord<R>> extends InsertField<R> {

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
