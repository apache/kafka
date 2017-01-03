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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

abstract class Insert<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String OPTIONALITY_DOC = "Prefix with '!' to make this a required field, or '?' to make it optional (the default).";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("topic", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka topic.\n" + OPTIONALITY_DOC)
            .define("partition", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka partition.\n" + OPTIONALITY_DOC)
            .define("offset", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka offset - only applicable to sinks.\n" + OPTIONALITY_DOC)
            .define("timestamp", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for record timestamp.\n" + OPTIONALITY_DOC);

    private static class Config extends AbstractConfig {
        public Config(Map<?, ?> originals) {
            super(CONFIG_DEF, originals, false);
        }
    }

    private static final class InsertionSpec {
        final String name;
        final boolean optional;

        private InsertionSpec(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }

        public static InsertionSpec parse(String spec) {
            if (spec == null) return null;
            if (spec.startsWith("?")) {
                return new InsertionSpec(spec.substring(1), true);
            }
            if (spec.startsWith("!")) {
                return new InsertionSpec(spec.substring(1), false);
            }
            return new InsertionSpec(spec, true);
        }
    }

    private static final class SchemaUpdateCacheEntry {
        final Schema base;
        final Schema updated;

        private SchemaUpdateCacheEntry(Schema base, Schema updated) {
            this.base = base;
            this.updated = updated;
        }
    }

    private InsertionSpec topicField;
    private InsertionSpec partitionField;
    private InsertionSpec offsetField;
    private InsertionSpec timestampField;
    private boolean applicable;

    // Expect this functionality to be refactored out as more transformations that operate on Schema's are added.
    private final ThreadLocal<SchemaUpdateCacheEntry> schemaUpdateCache = new ThreadLocal<>();

    @Override
    public void init(Map<String, Object> props) {
        final Config config = new Config(props);
        topicField = InsertionSpec.parse(config.getString("topic"));
        partitionField = InsertionSpec.parse(config.getString("partition"));
        offsetField = InsertionSpec.parse(config.getString("offset"));
        timestampField = InsertionSpec.parse(config.getString("timestamp"));
        applicable = topicField != null || partitionField != null || offsetField != null || timestampField != null;
    }

    @Override
    public R apply(R record) {
        if (!applicable) return record;

        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        if (schema == null)
            throw new DataException("Schemaless mode not supported by " + getClass().getName());

        if (schema.type() != Schema.Type.STRUCT)
            throw new DataException("Can only operate on Struct types: " + getClass().getName());

        final Schema updatedSchema;
        final SchemaUpdateCacheEntry schemaUpdateCacheEntry = schemaUpdateCache.get();
        if (schemaUpdateCacheEntry == null || schemaUpdateCacheEntry.base != schema) {
            updatedSchema = makeUpdatedSchema(schema);
            schemaUpdateCache.set(new SchemaUpdateCacheEntry(schema, updatedSchema));
        } else {
            updatedSchema = schemaUpdateCacheEntry.updated;
        }

        final Struct updatedValue = new Struct(updatedSchema);

        copyFields((Struct) value, updatedValue);

        insertFields(record, updatedValue);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaBuilder.struct();

        if (schema.name() != null) {
            builder.name(schema.name());
        }

        if (schema.version() != null) {
            builder.version(schema.version());
        }

        if (schema.doc() != null) {
            builder.doc(schema.doc());
        }

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
            builder.field(timestampField.name, timestampField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
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
                throw new DataException("Offset insertion is only supported for sink conectors, record is of type: " + record.getClass());
            }
            value.put(offsetField.name, ((SinkRecord) record).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            value.put(timestampField.name, record.timestamp());
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

}
