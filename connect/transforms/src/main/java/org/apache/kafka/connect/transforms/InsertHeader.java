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
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Add a header to each record.";

    public static final String HEADER_FIELD = "header";
    public static final String VALUE_LITERAL_FIELD = "value.literal";
    public static final String VALUE_TYPE_FIELD = "value.type";

    private static final String TYPE_INT8 = "int8";
    private static final String TYPE_INT16 = "int16";
    private static final String TYPE_INT32 = "int32";
    private static final String TYPE_INT64 = "int64";
    private static final String TYPE_FLOAT32 = "float32";
    private static final String TYPE_FLOAT64 = "float64";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_BYTES = "bytes";

    private static final ConfigDef.ValidString VALID_TYPES = ConfigDef.ValidString.in(
            TYPE_INT8, TYPE_INT16, TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64,
            TYPE_BOOLEAN, TYPE_STRING, TYPE_BYTES);

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The name of the header.")
            .define(VALUE_LITERAL_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The literal value that is to be set as the header value on all records.")
            .define(VALUE_TYPE_FIELD, ConfigDef.Type.STRING,
                    null, ConfigDef.LambdaValidator.with(
                            (name, value) -> {
                                if (value != null) {
                                    VALID_TYPES.ensureValid(name, value);
                                }
                            },
                            VALID_TYPES::toString),
                    ConfigDef.Importance.MEDIUM,
                    "The schema type for the header value. Valid types are int8, int16, int32, int64, float32, "
                            + "float64, boolean, string, and bytes.");

    private String header;

    private SchemaAndValue literalValue;

    @Override
    public R apply(R record) {
        Headers updatedHeaders = record.headers().duplicate();
        updatedHeaders.add(header, literalValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
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
        header = config.getString(HEADER_FIELD);
        String valueLiteral = config.getString(VALUE_LITERAL_FIELD);
        String valueType = config.getString(VALUE_TYPE_FIELD);
        literalValue = valueType == null ? Values.parseString(valueLiteral) : typeAndValue(valueType, valueLiteral);
    }

    private static SchemaAndValue typeAndValue(String valueType, String valueLiteral) {
        Schema schema = typeToSchema(valueType);
        Object value = switch (schema.type()) {
            case INT8 -> Values.convertToByte(null, valueLiteral);
            case INT16 -> Values.convertToShort(null, valueLiteral);
            case INT32 -> Values.convertToInteger(null, valueLiteral);
            case INT64 -> Values.convertToLong(null, valueLiteral);
            case FLOAT32 -> Values.convertToFloat(null, valueLiteral);
            case FLOAT64 -> Values.convertToDouble(null, valueLiteral);
            case BOOLEAN -> Values.convertToBoolean(null, valueLiteral);
            case STRING -> Values.convertToString(null, valueLiteral);
            case BYTES -> valueLiteral.getBytes(StandardCharsets.UTF_8);
            default -> throw new IllegalArgumentException("Unsupported header value type: " + valueType);
        };
        return new SchemaAndValue(schema, value);
    }

    private static Schema typeToSchema(String valueType) {
        return switch (valueType) {
            case TYPE_INT8 -> Schema.INT8_SCHEMA;
            case TYPE_INT16 -> Schema.INT16_SCHEMA;
            case TYPE_INT32 -> Schema.INT32_SCHEMA;
            case TYPE_INT64 -> Schema.INT64_SCHEMA;
            case TYPE_FLOAT32 -> Schema.FLOAT32_SCHEMA;
            case TYPE_FLOAT64 -> Schema.FLOAT64_SCHEMA;
            case TYPE_BOOLEAN -> Schema.BOOLEAN_SCHEMA;
            case TYPE_STRING -> Schema.STRING_SCHEMA;
            case TYPE_BYTES -> Schema.BYTES_SCHEMA;
            default -> throw new IllegalArgumentException("Unsupported header value type: " + valueType);
        };
    }
}
