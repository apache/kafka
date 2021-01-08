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
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ReplaceField<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ReplaceField.class);

    public static final String OVERVIEW_DOC = "Filter or rename fields. Recursion through nested values is also "
            + "possible by setting <code>recursive</code> to <code>true</code>."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    public static interface ConfigName {
        String EXCLUDE = "exclude";
        String INCLUDE = "include";

        // for backwards compatibility
        String INCLUDE_ALIAS = "whitelist";
        String EXCLUDE_ALIAS = "blacklist";

        String RENAME = "renames";

        String RECURSIVE = "recursive";
    }

    public static interface ConfigDefault {
        boolean RECURSIVE = false;
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.EXCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to exclude. This takes precedence over the fields to include.")
            .define("blacklist", ConfigDef.Type.LIST, null, Importance.LOW,
                    "Deprecated. Use " + ConfigName.EXCLUDE + " instead.")
            .define(ConfigName.INCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to include. If specified, only these fields will be used.")
            .define("whitelist", ConfigDef.Type.LIST, null, Importance.LOW,
                    "Deprecated. Use " + ConfigName.INCLUDE + " instead.")
            .define(ConfigName.RENAME, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
                @SuppressWarnings("unchecked")
                @Override
                public void ensureValid(String name, Object value) {
                    parseRenameMappings((List<String>) value);
                }

                @Override
                public String toString() {
                    return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                }
            }, ConfigDef.Importance.MEDIUM, "Field rename mappings.")
            .define(ConfigName.RECURSIVE, ConfigDef.Type.BOOLEAN, ConfigDefault.RECURSIVE, ConfigDef.Importance.MEDIUM, 
                    "Boolean which indicates if the ReplaceField should recursively replace child fields of nested complex types, "
                    + "if any nested children fields exist with the same names as given in the configuration.");

    private static final String PURPOSE = "field replacement";

    private List<String> exclude;
    private List<String> include;
    private Map<String, String> renames;
    private Map<String, String> reverseRenames;
    private boolean isRecursive;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, ConfigUtils.translateDeprecatedConfigs(configs, new String[][]{
            {ConfigName.INCLUDE, "whitelist"},
            {ConfigName.EXCLUDE, "blacklist"},
        }));

        exclude = config.getList(ConfigName.EXCLUDE);
        include = config.getList(ConfigName.INCLUDE);
        renames = parseRenameMappings(config.getList(ConfigName.RENAME));
        reverseRenames = invert(renames);
        isRecursive = config.getBoolean(ConfigName.RECURSIVE);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ConfigName.RENAME, mappings, "Invalid rename mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    static Map<String, String> invert(Map<String, String> source) {
        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    boolean filter(String fieldName) {
        if (exclude.contains(fieldName)) {
            log.debug("Excluded field '{}' will be removed.", fieldName);
            return false;
        } else if (include.contains(fieldName)) {
            log.debug("Included field '{}' will be added.", fieldName);
            return true;
        } else if (include.isEmpty()) {
            return true;
        } else {
            log.debug("Field '{}' will be removed (missing from the include list or otherwise incompatible configuration).", fieldName);
            return false;
        }
    }

    String renamed(String fieldName) {
        final String mapping = renames.get(fieldName);
        if (mapping == null) {
            return fieldName;
        } else {
            log.debug("Renamed field '{}' to '{}'.", fieldName, mapping);
            return mapping;
        }
    }

    String reverseRenamed(String fieldName) {
        final String mapping = reverseRenames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        Map<Object, Object> valueToConvert = new HashMap<>(value);
        final Map<Object, Object> updatedValue = buildUpdatedMapValue(valueToConvert);

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = getOrBuildUpdatedSchema(value.schema());
        final Struct updatedValue = (Struct) buildUpdatedSchemaValue(value, updatedSchema);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildUpdatedSchema(Schema schema) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder;
        builder = buildUpdatedSchema(schema);

        if (schema.isOptional())
            builder.optional();
        if (schema.defaultValue() != null)
            builder.defaultValue(schema.defaultValue());

        updatedSchema = builder.build();
        schemaUpdateCache.put(schema, updatedSchema);
        return updatedSchema;
    }

    /***
     * Method which recursively builds nested {@link SchemaBuilder}s based on the {@link ReplaceField} configuration. 
     * Each child schema is also added to the <code>schemaUpdateCache</code> and can be fetched afterwards instead 
     * of being built again.
     * @param schema
     * @return {@link SchemaBuilder} which can be used to build the final {@link Schema}
     */
    private SchemaBuilder buildUpdatedSchema(Schema schema) {

        // Perform different logic for different types of parent schemas.

        if (schema.type() == Schema.Type.STRUCT)
            return buildUpdatedStructSchema(schema);

        else if (isRecursive && schema.type() == Schema.Type.ARRAY)
            return buildUpdatedArraySchema(schema);

        else if (isRecursive && schema.type() == Schema.Type.MAP)
            return buildUpdatedMapSchema(schema);

        else
            throw new DataException(schema.type().toString() + " is not a supported parent Schema type for the ReplaceField transformation.");

    }

    private SchemaBuilder buildUpdatedStructSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (filter(field.name())) {
                String updatedName = renamed(field.name());

                // If field has already been added then skip adding it again. This will allow rename of multiple "can contain only one of" kind of fields into one common name.
                if (builder.field(updatedName) != null)
                    continue;

                if (isRecursive && 
                        (field.schema().type() == Schema.Type.STRUCT 
                        || field.schema().type() == Schema.Type.ARRAY 
                        || field.schema().type() == Schema.Type.MAP)
                ) { 
                    Schema updatedChildSchema = getOrBuildUpdatedSchema(field.schema());
                    builder.field(updatedName, updatedChildSchema);

                } else // This is where all non-parent Schema fields should be added (is not recursive, or we are at the bottom of a recursion) 
                    builder.field(updatedName, field.schema());
            }
        }

        return builder;
    }

    private SchemaBuilder buildUpdatedArraySchema(Schema schema) {
        // For complex types, just go one level lower to more detail and then return a new Array schema with the updated child value schema
        if (schema.valueSchema().type() == Schema.Type.STRUCT 
                || schema.valueSchema().type() == Schema.Type.ARRAY 
                || schema.valueSchema().type() == Schema.Type.MAP) {
            Schema updatedChildSchema = getOrBuildUpdatedSchema(schema.valueSchema());
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.array(updatedChildSchema));

        } else // Otherwise we will just assume to pass it along since the Array itself was already allowed by an upstream filter()
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.array(schema.valueSchema()));
    }

    private SchemaBuilder buildUpdatedMapSchema(Schema schema) {
        // For complex type values, just go one level lower to more detail and then return a new Map schema with the updated child value schema
        if (schema.valueSchema().type() == Schema.Type.STRUCT 
                || schema.valueSchema().type() == Schema.Type.ARRAY 
                || schema.valueSchema().type() == Schema.Type.MAP) {
            Schema updatedChildSchema = getOrBuildUpdatedSchema(schema.valueSchema());
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.map(schema.keySchema(), updatedChildSchema));

        } else if (schema.keySchema().type() == Schema.Type.STRING) {
            // Otherwise, if the Map key is a string, then we will also perform ReplaceField based on the key names within the Map fields

            SchemaBuilder childBuilder = SchemaUtil.copySchemaBasics(schema.valueSchema());

            for (Field field : schema.valueSchema().fields()) {
                String fieldName = field.name();

                if (filter(fieldName)) {

                    if (schema.valueSchema().type() == Schema.Type.STRUCT 
                            || schema.valueSchema().type() == Schema.Type.ARRAY 
                            || schema.valueSchema().type() == Schema.Type.MAP) {
                        Schema updatedChildSchema = getOrBuildUpdatedSchema(field.schema());
                        childBuilder.field(renamed(field.name()), updatedChildSchema);

                    } else
                        childBuilder.field(renamed(fieldName), field.schema());
                }
            }

            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.map(schema.keySchema(), childBuilder.build()));

        } else // And in the last-case scenario (not a complex value and not a String key type) then we will just assume to pass it along since the Map itself was already allowed by an upstream filter()
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.map(schema.keySchema(), schema.valueSchema()));
    }

    @SuppressWarnings("unchecked")
    private Object buildUpdatedSchemaValue(Object value, Schema updatedSchema) {

        if (value == null)
            return null;

        if (updatedSchema.type() == Schema.Type.STRUCT) {
            Struct struct = (Struct) value;
            Struct updatedStruct = new Struct(updatedSchema);

            for (Field field : struct.schema().fields()) {
                Object fieldValue = struct.get(field);

                // If the value is empty, skip it. It will still be in the value Struct at the end if the field exists in the schema, 
                //  but by doing this then this will allow rename of multiple "can contain only one of" kind of fields into one common name
                if (fieldValue == null)
                    continue;

                if (filter(field.name())) {
                    String updatedName = renamed(field.name());

                    // If field value has already been added then throw an exception.
                    if (updatedStruct.get(updatedName) != null)
                        throw new DataException("Duplicate target field '" + updatedName + "' already exists in the transformed value.");

                    if (isRecursive && 
                            (field.schema().type() == Schema.Type.STRUCT 
                            || field.schema().type() == Schema.Type.ARRAY 
                            || field.schema().type() == Schema.Type.MAP)
                    ) {
                        Schema updatedChildSchema = getOrBuildUpdatedSchema(field.schema());
                        Object updatedChildValue = buildUpdatedSchemaValue(fieldValue, updatedChildSchema);
                        updatedStruct.put(updatedSchema.field(renamed(field.name())), updatedChildValue);

                    } else // This is where most non-parent Value fields should be added (Struct fields that match the filter(), and either we are not using recursion or we are at the bottom of any nested Struct)
                        updatedStruct.put(updatedSchema.field(renamed(field.name())), fieldValue);
                }
            }
            return updatedStruct;

        } else if (isRecursive && updatedSchema.type() == Schema.Type.ARRAY) {
            return buildUpdatedArrayValue((List<Object>) value);

        } else if (isRecursive && updatedSchema.type() == Schema.Type.MAP) {
            return buildUpdatedMapValue((Map<Object, Object>) value);

        } else
            throw new DataException(updatedSchema.type().toString() + " is not a supported schema type for the ReplaceField transformation.");
    }

    @SuppressWarnings("unchecked")
    private List<Object> buildUpdatedArrayValue(List<Object> array) {
        List<Object> updatedArray = new ArrayList<Object>(array.size());
        for (Object arrayElement : array) {
            if (isRecursive && arrayElement instanceof Struct) {
                Struct struct = (Struct) arrayElement;
                Schema updatedSchema = getOrBuildUpdatedSchema(struct.schema()); 
                Object updatedStruct = buildUpdatedSchemaValue(struct, updatedSchema);
                updatedArray.add(updatedStruct);

            } else if (isRecursive && arrayElement instanceof List<?>) {
                updatedArray.add(buildUpdatedArrayValue((List<Object>) arrayElement));

            } else if (isRecursive && arrayElement instanceof Map<?, ?>) {
                updatedArray.add(buildUpdatedMapValue((Map<Object, Object>) arrayElement));

            } else
                updatedArray.add(arrayElement);
        }
        return updatedArray;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Object> buildUpdatedMapValue(Map<Object, Object> map) {
        Map<Object, Object> updatedMap = new HashMap<>(map.size());

        for (Map.Entry<Object, Object> mapEntry : map.entrySet()) {
            Object mapEntryValue = mapEntry.getValue();

            // If the Map key is a string, then we will also perform ReplaceField based on the key names within the Map fields
            if (mapEntry.getKey() instanceof String) {
                String mapEntryKey = mapEntry.getKey().toString();

                if (filter(mapEntryKey)) {

                    if (isRecursive && mapEntryValue instanceof Struct) {
                        Struct struct = (Struct) mapEntry.getValue();
                        Schema updatedSchema = getOrBuildUpdatedSchema(struct.schema()); 
                        Object updatedStruct = buildUpdatedSchemaValue(struct, updatedSchema);
                        updatedMap.put(renamed(mapEntryKey), updatedStruct);
                    } else if (isRecursive && mapEntryValue instanceof List<?>) {
                        updatedMap.put(renamed(mapEntryKey), buildUpdatedArrayValue((List<Object>) mapEntryValue));
                    } else if (isRecursive && mapEntryValue instanceof Map<?, ?>) {
                        updatedMap.put(renamed(mapEntryKey), buildUpdatedMapValue((Map<Object, Object>) mapEntryValue));
                    } else // This is not a complex type that we can drill down. Send it through because we already know that this entry's parent map was allowed from filter()
                        updatedMap.put(renamed(mapEntryKey), mapEntry.getValue());
                }

            } else if (isRecursive && mapEntryValue instanceof Struct) {
                // Otherwise the key is not a string, and we cannot do a filter() check on the Map keys. So go ahead with passing it through since 
                //  an upstream filter() allowed us to be here.

                // However, if the map value is another complex type, then we can go one level deeper into the value to continue performing ReplaceField 
                //  logic on the children.

                // The logic is the same above, except we cannot check against the filter() or rename the keys
                Struct struct = (Struct) mapEntry.getValue();
                Schema updatedSchema = getOrBuildUpdatedSchema(struct.schema()); 
                Object updatedStruct = buildUpdatedSchemaValue(struct, updatedSchema);
                updatedMap.put(mapEntry.getKey(), updatedStruct);
            } else if (isRecursive && mapEntryValue instanceof List<?>) {
                updatedMap.put(mapEntry.getKey(), buildUpdatedArrayValue((List<Object>) mapEntryValue));
            } else if (isRecursive && mapEntryValue instanceof Map<?, ?>) {
                updatedMap.put(mapEntry.getKey(), buildUpdatedMapValue((Map<Object, Object>) mapEntryValue));
            } else // This is not a complex type that we can drill down. Send it through because we already know that this entry's parent map was allowed from filter()
                updatedMap.put(mapEntry.getKey(), mapEntry.getValue());
        }

        return updatedMap;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ReplaceField<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends ReplaceField<R> {

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
