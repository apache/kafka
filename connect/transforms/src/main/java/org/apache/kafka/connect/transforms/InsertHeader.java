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
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertHeader<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
        "Insert header as a literal value.";

    private interface ConfigName {
        String HEADER_CONFIG = "header";
        String HEADER_VALUE_CONFIG = "value";
    }

    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.HEADER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
            "Name of the header to add.")
        .define(ConfigName.HEADER_VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
            "Value of the header to add.");

    private String headerName;
    private String headerValue;
    private String headerType;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerName = config.getString(ConfigName.HEADER_CONFIG);
        headerValue = config.getString(ConfigName.HEADER_VALUE_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (headerValue == null) return null;
        SchemaAndValue schemaAndValue = Values.parseString(headerValue);
        record.headers().add(headerName, schemaAndValue);
        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
