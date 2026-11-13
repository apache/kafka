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
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion;
import org.apache.kafka.connect.transforms.field.SingleFieldPath;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class HoistField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Wrap a field using the specified field name in a Struct when schema present, or a Map in the case of schemaless data. "
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";

    private static final String FIELD_CONFIG = "field";
    private static final String HOISTED_CONFIG = "hoisted";

    private static final String PURPOSE = "hoisting a field";

    public static final ConfigDef CONFIG_DEF = FieldSyntaxVersion.appendConfigTo(
            new ConfigDef()
                    .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
                            "Field name for the single field that will be created in the resulting Struct or Map.")
                    .define(HOISTED_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                            "Path to the element to be hoisted. If empty, the root struct/map is hoisted."));

    private Cache<Schema, Schema> schemaUpdateCache;

    private String wrapperName;
    private String hoisted;
    private SingleFieldPath fieldPath;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        wrapperName = config.getString(FIELD_CONFIG);
        final String hoistedConfig = config.getString(HOISTED_CONFIG);
        hoisted = hoistedConfig == null ? "" : hoistedConfig;
        final FieldSyntaxVersion fieldSyntaxVersion = FieldSyntaxVersion.fromConfig(config);
        fieldPath = new SingleFieldPath(hoisted, fieldSyntaxVersion);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    /**
     * Handles schemaless (Map) records. When {@code hoisted} is empty the whole value is wrapped
     * in a single-field Map named {@code wrapperName}; otherwise the element at the {@code hoisted}
     * path is hoisted in place.
     */
    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        final Object value = operatingValue(record);

        if (hoisted.isEmpty()) {
            Map<String, Object> wrapper = new HashMap<>();
            wrapper.put(wrapperName, value);
            return newRecord(record, null, wrapper);
        }

        final Map<String, Object> map = requireMap(value, PURPOSE);
        return newRecord(record, null, hoistSchemaless(map, 0));
    }

    /**
     * Recursively rebuilds a Map along the {@code hoisted} path, copying every sibling untouched.
     * At the leaf step the targeted entry is replaced by a new field {@code wrapperName} holding a
     * Map of {@code {leafName: leafValue}}. Throws if a path step is missing or not a Map.
     */
    private Map<String, Object> hoistSchemaless(Map<String, Object> current, int idx) {
        final List<String> steps = fieldPath.steps();
        final String step = steps.get(idx);
        if (!current.containsKey(step)) {
            throw new IllegalArgumentException("Unknown field: " + hoisted);
        }

        final Map<String, Object> updated = new LinkedHashMap<>(current);
        if (idx == steps.size() - 1) {
            final Object leafValue = updated.remove(step);
            final Map<String, Object> wrapper = new HashMap<>();
            wrapper.put(step, leafValue);
            updated.put(wrapperName, wrapper);
        } else {
            final Map<String, Object> child = requireMap(current.get(step), PURPOSE);
            updated.put(step, hoistSchemaless(child, idx + 1));
        }
        return updated;
    }

    /**
     * Handles schema-based (Struct) records. When {@code hoisted} is empty the whole value is wrapped
     * in a single-field Struct named {@code wrapperName}; otherwise the element at the {@code hoisted}
     * path is hoisted in place. Updated schemas are cached by their source schema.
     */
    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        if (hoisted.isEmpty()) {
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                updatedSchema = SchemaBuilder.struct().field(wrapperName, schema).build();
                schemaUpdateCache.put(schema, updatedSchema);
            }
            final Struct updatedValue = new Struct(updatedSchema).put(wrapperName, value);
            return newRecord(record, updatedSchema, updatedValue);
        }

        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = hoistSchema(schema, 0);
            schemaUpdateCache.put(schema, updatedSchema);
        }
        final Struct updatedValue = hoistValue(requireStruct(value, PURPOSE), updatedSchema, 0);
        return newRecord(record, updatedSchema, updatedValue);
    }

    /**
     * Recursively rebuilds a Struct schema along the {@code hoisted} path, preserving every other
     * field. At the leaf step the targeted field is replaced by {@code wrapperName}, whose schema is
     * a Struct wrapping the original leaf field. Throws if a path step is not found in the schema.
     */
    private Schema hoistSchema(Schema current, int idx) {
        final List<String> steps = fieldPath.steps();
        final String step = steps.get(idx);
        if (current.field(step) == null) {
            throw new IllegalArgumentException("Unknown field: " + hoisted);
        }

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(current, SchemaBuilder.struct());
        for (Field field : current.fields()) {
            if (!field.name().equals(step)) {
                builder.field(field.name(), field.schema());
            } else if (idx == steps.size() - 1) {
                final Schema wrapperSchema = SchemaBuilder.struct().field(step, field.schema()).build();
                builder.field(wrapperName, wrapperSchema);
            } else {
                builder.field(field.name(), hoistSchema(field.schema(), idx + 1));
            }
        }
        return builder.build();
    }

    /**
     * Recursively rebuilds a Struct against {@code updatedSchema} (the schema produced by
     * {@link #hoistSchema}), mirroring the traversal: siblings are copied as-is, and at the leaf step
     * the targeted field's value is nested under {@code wrapperName}.
     */
    private Struct hoistValue(Struct current, Schema updatedSchema, int idx) {
        final List<String> steps = fieldPath.steps();
        final String step = steps.get(idx);

        final Struct updated = new Struct(updatedSchema);
        for (Field field : current.schema().fields()) {
            final String name = field.name();
            if (!name.equals(step)) {
                updated.put(name, current.get(field));
            } else if (idx == steps.size() - 1) {
                final Schema wrapperSchema = updatedSchema.field(wrapperName).schema();
                final Struct wrapper = new Struct(wrapperSchema).put(step, current.get(field));
                updated.put(wrapperName, wrapper);
            } else {
                final Schema childSchema = updatedSchema.field(name).schema();
                updated.put(name, hoistValue(requireStruct(current.get(field), PURPOSE), childSchema, idx + 1));
            }
        }
        return updated;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
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

    public static class Key<R extends ConnectRecord<R>> extends HoistField<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends HoistField<R> {
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
