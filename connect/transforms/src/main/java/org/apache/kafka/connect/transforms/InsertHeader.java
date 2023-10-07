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
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

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
        literalValue = Values.parseString(config.getString(VALUE_LITERAL_FIELD));
    }
}
