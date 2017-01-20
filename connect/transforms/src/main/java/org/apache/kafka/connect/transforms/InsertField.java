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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Insert field(s) using attributes from the record metadata or a configured static value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getCanonicalName() + "</code>) "
                    + "or value (<code>" + Value.class.getCanonicalName() + "</code>).";

    private interface ConfigName {
        String TOPIC_FIELD = "topic.field";
        String PARTITION_FIELD = "partition.field";
        String OFFSET_FIELD = "offset.field";
        String TIMESTAMP_FIELD = "timestamp.field";
        String STATIC_FIELD = "static.field";
        String STATIC_VALUE = "static.value";
    }

    private static final String OPTIONALITY_DOC = "Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka topic. " + OPTIONALITY_DOC)
            .define(ConfigName.PARTITION_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka partition. " + OPTIONALITY_DOC)
            .define(ConfigName.OFFSET_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka offset - only applicable to sink connectors.<br/>" + OPTIONALITY_DOC)
            .define(ConfigName.TIMESTAMP_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for record timestamp. " + OPTIONALITY_DOC)
            .define(ConfigName.STATIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for static data field. " + OPTIONALITY_DOC)
            .define(ConfigName.STATIC_VALUE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Static field value, if field name configured.");

    private static final String PURPOSE = "field insertion";

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

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        topicField = InsertionSpec.parse(config.getString(ConfigName.TOPIC_FIELD));
        partitionField = InsertionSpec.parse(config.getString(ConfigName.PARTITION_FIELD));
        offsetField = InsertionSpec.parse(config.getString(ConfigName.OFFSET_FIELD));
        timestampField = InsertionSpec.parse(config.getString(ConfigName.TIMESTAMP_FIELD));
        staticField = InsertionSpec.parse(config.getString(ConfigName.STATIC_FIELD));
        staticValue = config.getString(ConfigName.STATIC_VALUE);

        if (topicField == null && partitionField == null && offsetField == null && timestampField == null && staticField == null) {
            throw new ConfigException("No field insertion configured");
        }

        if (staticField != null && staticValue == null) {
            throw new ConfigException(ConfigName.STATIC_VALUE, null, "No value specified for static field: " + staticField);
        }

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
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, record.timestamp());
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }

        return newRecord(record, null, updatedValue);
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

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, new Date(record.timestamp()));
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

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
