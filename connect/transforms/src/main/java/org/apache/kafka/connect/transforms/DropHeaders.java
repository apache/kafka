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
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class DropHeaders<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC =
            "Removes one or more headers from each record.";

    public static final String HEADERS_FIELD = "headers";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADERS_FIELD, ConfigDef.Type.LIST,
                    NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH,
                    "The names or regular expressions to match of the headers to be removed.");

    private List<Pattern> headersMatchers;

    @Override
    public R apply(R record) {
        Headers updatedHeaders = new ConnectHeaders();
        for (Header header : record.headers()) {
            if (!headersMatchAnyPattern(header.key())) {
                updatedHeaders.add(header);
            }
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
    }

    private boolean headersMatchAnyPattern(String key) {
        return headersMatchers.stream().anyMatch(pattern ->
            pattern.matcher(key).matches()
        );
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
        final List<String> headers = config.getList(HEADERS_FIELD);
        headersMatchers = headers.stream().map(entry -> Pattern.compile(entry)).collect(Collectors.toList());
    }
}
