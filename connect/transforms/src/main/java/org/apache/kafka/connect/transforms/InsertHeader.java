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

import java.math.BigDecimal;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Add a header to each record.";

    public static final String HEADER_FIELD = "header";
    public static final String VALUE_LITERAL_FIELD = "value.literal";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The name of the header.")
            .define(VALUE_LITERAL_FIELD, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "The literal value that is to be set as the header value on all records.");

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
        String rawValue = config.getString(VALUE_LITERAL_FIELD);
        SchemaAndValue parsed = Values.parseString(rawValue);
        if (parsed != null && parsed.schema() != null) {
            Schema.Type type = parsed.schema().type();
            boolean isIntegral = type == Schema.Type.INT8 || type == Schema.Type.INT16 || type == Schema.Type.INT32 || type == Schema.Type.INT64;
            if (isIntegral && rawValue.contains(".")) {
                try {
                    BigDecimal decimal = new BigDecimal(rawValue);
                    float fValue = decimal.floatValue();
                    double dValue = decimal.doubleValue();
                    if (fValue != Float.NEGATIVE_INFINITY && fValue != Float.POSITIVE_INFINITY) {
                        literalValue = new SchemaAndValue(Schema.FLOAT32_SCHEMA, fValue);
                    } else if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY) {
                        literalValue = new SchemaAndValue(Schema.FLOAT64_SCHEMA, dValue);
                    } else {
                        literalValue = new SchemaAndValue(org.apache.kafka.connect.data.Decimal.schema(decimal.scale()), decimal);
                    }
                } catch (NumberFormatException e) {
                    literalValue = parsed;
                }
            } else {
                literalValue = parsed;
            }
        } else {
            literalValue = parsed;
        }
    }
}
