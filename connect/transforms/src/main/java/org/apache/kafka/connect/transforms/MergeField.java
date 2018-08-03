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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MergeField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
        "Merge a list of field under one root field, composing a nested field, with the possibility " +
                "to keep the field or remove it. "
                + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                + "or value (<code>" + Value.class.getName() + "</code>).";
    public static final String FIELD_ROOT_CONFIG = "field.root";
    public static final String FIELD_LIST_CONFIG = "field.list";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_ROOT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,  ConfigDef.Importance.MEDIUM,
                    "The root field")
            .define(FIELD_LIST_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
                    "The list of fields to merge");

    private static final String PURPOSE = "merging";

    private String fieldRoot;
    private List<MergeSpec> fieldList;

    private Cache<Object, Schema> schemaUpdateCache;

    private enum SpecTypes {
        KEEP_OPTIONAL,
        REMOVE_OPTIONAL,
        KEEP_REQUIRED,
        REMOVE_REQUIRED
    }

    private static final Map<String, String> REGEX_SPEC_MAP = new HashMap<String, String>() {{
            put(SpecTypes.KEEP_OPTIONAL.name(), "\\*(?<fieldname>.*?)\\?");
            put(SpecTypes.REMOVE_OPTIONAL.name(), "^(?!\\*)(?<fieldname>.*?)\\?");
            put(SpecTypes.KEEP_REQUIRED.name(), "\\*(?<fieldname>.*?)(?<!\\?)$");
            put(SpecTypes.REMOVE_REQUIRED.name(), "^(?!\\*)(?<fieldname>(?s)^.*+)(?<!\\?)$");
        }};

    /**
     * For each
     */
    public static final class MergeSpec {
        final String name;
        final boolean optional;
        final boolean keepIt;

        public MergeSpec(String name, boolean keepIt, boolean optional) {
            this.name = name;
            this.optional = optional;
            this.keepIt = keepIt;
        }

        /**
         * @param spec : field's specification
         * @return an instance of {@link #MergeSpec}
         */
        public static MergeSpec parse(String spec) {

            if (spec == null) return null;


            SpecPair mergeSpec = REGEX_SPEC_MAP.entrySet().stream()
                    .map(keyValue -> {
                        Optional<String> extracted = applyRegex(keyValue.getValue(), spec);
                        return new SpecPair(SpecTypes.valueOf(keyValue.getKey()), extracted.isPresent() ? extracted.get() : null);
                    })
                    .filter(x -> Optional.ofNullable(x.value).isPresent())
                    .findFirst()
                    .get();

            switch (mergeSpec.spec) {
                case KEEP_OPTIONAL: return new MergeSpec(mergeSpec.value, true, true);
                case REMOVE_OPTIONAL: return new MergeSpec(mergeSpec.value, false, true);
                case KEEP_REQUIRED: return new MergeSpec(mergeSpec.value, true, false);
                case REMOVE_REQUIRED: return new MergeSpec(mergeSpec.value, false, false);
                default: return null;
            }

        }

        private static String removeSpecs(String spec) {
            return spec.replace("?", "").replace("*", "");
        }


        public static Optional<String> applyRegex(String regex, String field) {
            Matcher matcher = Pattern.compile(regex).matcher(field);
            Optional<String> fieldName = Optional.empty();
            if (matcher.find()) {
                fieldName =  Optional.of(matcher.group("fieldname"));
            }
            return fieldName;
        }

        static class SpecPair {
            SpecTypes spec;
            String value;

            public SpecPair(SpecTypes spec, String value) {
                this.spec = spec;
                this.value = value;
            }
        }


    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null)
            return applySchemaless(record);
        else
            return applyWithSchema(record);
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> newValue = buildNewSchemalessValue(value);

        return newRecord(record, null, newValue);
    }

    private R applyWithSchema(R record) {
        Schema newSchema = makeUpdatedSchema(operatingSchema(record), fieldRoot, fieldList);
        Schema structSchema = buildNewStructSchema(operatingSchema(record), fieldList);
        Struct newValue = buildNewValue(record, newSchema, structSchema);

        return newRecord(record, newSchema, newValue);
    }


    private Map<String, Object> buildNewSchemalessValue(Map<String, Object> value) {


        Map<String, Object> newValue = value;
        Map<String, Object> nestedValue = new LinkedHashMap<>();

        fieldList.stream().forEach(field -> {
            nestedValue.put(field.name, value.get(field.name));

            if (!field.keepIt)
                newValue.remove(field.name);
        });

        newValue.put(fieldRoot, nestedValue);

        return newValue;

    }


    private Struct buildNewValue(R record, Schema newSchema, Schema rootSchema) {

        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(newSchema);
        final Struct nestedValue = new Struct(rootSchema);

        rootSchema.fields().stream().forEach(field -> nestedValue.put(field.name(), value.get(field.name())));

        newSchema.fields().stream().filter(field -> !field.name().equals(fieldRoot)).forEach(field -> updatedValue.put(field, value.get(field)));

        updatedValue.put(fieldRoot, nestedValue);

        return updatedValue;
    }

    public Schema buildNewStructSchema(Schema schema, List<MergeSpec> fieldList) {

        return Optional.ofNullable(schemaUpdateCache.get(fieldRoot)).orElseGet(() -> {
            SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);

            fieldList.stream().forEach(fieldSpec -> builder.field(
                    schema.field(fieldSpec.name).name(),
                    convertFieldSchema(schema.field(fieldSpec.name).schema(), fieldSpec.optional)
            ));

            Schema newStructSchema = builder.build();
            schemaUpdateCache.put(fieldRoot, newStructSchema);
            return newStructSchema;

        });
    }

    private Schema convertFieldSchema(Schema orig, boolean optional) {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(orig);
        if (optional)
            builder.optional();
        return builder.build();
    }

    public Schema makeUpdatedSchema(Schema schema, String fieldRoot, List<MergeSpec> fieldList) {

        return Optional.ofNullable(schemaUpdateCache.get(schema)).orElseGet(() -> {

            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema);

            schema.fields().stream().forEach(field -> {
                Optional<Boolean> shouldKeepIt = fieldList.stream()
                        .filter(fieldSpec -> fieldSpec.name.equals(field.name()))
                        .findFirst().map(fieldSpec -> fieldSpec.keepIt);

                if (shouldKeepIt.orElse(true)) {
                    builder.field(field.name(), field.schema());
                }

            });

            Schema fieldListSchema = buildNewStructSchema(schema, fieldList);

            builder.field(fieldRoot, fieldListSchema);

            Schema newSchema = builder.build();

            schemaUpdateCache.put(schema, newSchema);

            return newSchema;

        });
    }

    public static class Key<R extends ConnectRecord<R>> extends MergeField<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends MergeField<R> {
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


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldRoot = config.getString(FIELD_ROOT_CONFIG);
        fieldList = new ArrayList<MergeSpec>();

        config.getList(FIELD_LIST_CONFIG).stream().forEach(field -> fieldList.add(MergeSpec.parse(field)));

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Object, Schema>(16));
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }
}
