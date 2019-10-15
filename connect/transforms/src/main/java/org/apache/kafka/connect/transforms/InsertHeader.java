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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
        "Insert in every record a header with a constant literal value.";


    private static final String HEADER_NAME_CONFIG = "header";
    private static final String HEADER_VALUE_CONFIG = "literal.value";

    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(HEADER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
            "Name of the header to add.")
        .define(HEADER_VALUE_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.MEDIUM, "Value of the header to add.");

    private String headerName;
    private String headerValue;
    private SchemaAndValue schemaAndValue;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerName = config.getString(HEADER_NAME_CONFIG);
        headerValue = config.getString(HEADER_VALUE_CONFIG);
        schemaAndValue = Values.parseString(headerValue);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return record;
        }
        // Copy the existing headers
        Headers newHeaders = new ConnectHeaders(record.headers());
        // Always add the new header, even if there is already a header with the same name
        newHeaders.add(headerName, schemaAndValue);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
            record.key(), record.valueSchema(), record.value(), record.timestamp(), newHeaders);
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
