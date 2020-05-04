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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class HeaderTo<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Moves or copies headers on a record into fields in on that record's key/value"
                    + "<p/>Use the concrete transformation type designed for the record key"
                    + "(<code>" + InsertField.Key.class.getName() + "</code>) or value "
                    + "(<code>" + InsertField.Value.class.getName() + "</code>).";

    private static final String DEFAULT_OPERATION = "copy";

    private static final String PURPOSE = "Key/value field creation from headers";

    private static final ConfigDef.Validator OPERATION_VALIDATOR = (name, value) -> {
        try {
            Operation.valueOf(value.toString().toLowerCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new ConfigException("Only two operations are supported: copy and move");
        }
    };

    private static final ConfigDef.Validator LIST_VALIDATOR = (name, value) -> {
        if (!Pattern.compile("(\\w+)(,\\w+)*").matcher(value.toString().replaceAll("\\s+", "")).matches()) {
            throw new ConfigException(
                    String.format("Config %s is suppose to be a comma-separated list of fields", name)
            );
        }
    };

    private static final ConfigDef CONFIG_DEF = new ConfigDef()

            .define(ConfigName.FIELDS,
                    ConfigDef.Type.STRING,
                    "transformation, not, configured",
                    LIST_VALIDATOR,
                    ConfigDef.Importance.HIGH,
                    "Field names, in the same order as the header names listed in the headers configuration property")

            .define(ConfigName.HEADERS,
                    ConfigDef.Type.STRING,
                    "transformation, not, configured",
                    LIST_VALIDATOR,
                    ConfigDef.Importance.HIGH,
                    "Header names whose latest values are to be copied/moved to key or value.")

            .define(ConfigName.OPERATION,
                    ConfigDef.Type.STRING,
                    DEFAULT_OPERATION,
                    OPERATION_VALIDATOR,
                    ConfigDef.Importance.HIGH,
                    "Operation applied on the header (can be either move or copy)");

    private static Struct structWithAddedFields(Struct struct,
                                                Headers headers,
                                                List<Map.Entry<String, String>> headerFieldPairs) {

        SchemaBuilder newSchema = SchemaBuilder.struct();
        newSchema.doc(struct.schema().doc());

        struct.schema().fields().forEach(filed -> newSchema.field(filed.name(), filed.schema()));
        headerFieldPairs.forEach(pair ->
                newSchema.field(pair.getValue(), headers.lastWithName(pair.getKey()).schema())
        );

        Struct newStruct = new Struct(newSchema.build());
        struct.schema().fields().forEach(filed -> newStruct.put(filed.name(), struct.get(filed)));
        headerFieldPairs.forEach(pair ->
                newStruct.put(pair.getValue(), headers.lastWithName(pair.getKey()).value())
        );

        return newStruct;
    }

    private enum Operation {
        move,
        copy
    }

    private interface ConfigName {
        String FIELDS = "fields";
        String HEADERS = "headers";
        String OPERATION = "operation";
    }

    protected String[] fields;
    protected String[] headers;
    protected Operation operation;

    protected List<Map.Entry<String, String>> headersFieldsZipped;

    @Override
    public R apply(R record) {
        if (record == null || operatingSchema(record) == null) return record;

        R newRecord = addFields(record);

        if (operation == Operation.move) {
            Arrays.stream(headers).forEach(header -> newRecord.headers().remove(header));
        }
        return newRecord;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = HeaderFrom.trimAll(config.getString(ConfigName.FIELDS).split(","));
        headers = HeaderFrom.trimAll(config.getString(ConfigName.HEADERS).split(","));
        operation = Operation.valueOf(config.getString(ConfigName.OPERATION).toLowerCase(Locale.ROOT));

        if (fields.length != headers.length) {
            throw new ConfigException(
                    String.format(
                            "The fields and headers should have the same number of elements. " +
                                    "Found: %s fields and %s headers",
                            fields.length,
                            headers.length
                    )
            );
        }

        headersFieldsZipped = IntStream
                .range(0, headers.length)
                .mapToObj(i -> new AbstractMap.SimpleImmutableEntry<>(headers[i], fields[i]))
                .collect(Collectors.toList());
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R addFields(R record);

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
        protected R addFields(R record) {
            Struct oldKey = requireStructOrNull(operatingValue(record), PURPOSE);
            Struct newKey = structWithAddedFields(oldKey, record.headers(), headersFieldsZipped);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    newKey.schema(),
                    newKey,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp(),
                    record.headers()
            );
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
        protected R addFields(R record) {
            Struct oldValue = requireStructOrNull(operatingValue(record), PURPOSE);
            Struct newValue = structWithAddedFields(oldValue, record.headers(), headersFieldsZipped);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    newValue.schema(),
                    newValue,
                    record.timestamp(),
                    record.headers()
            );
        }
    }
}
