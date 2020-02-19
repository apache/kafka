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
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map;

public class DropHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
        "Drop all header(s) whose names are specified in a comma-separated list.";

    public static final String HEADER_NAMES_CONFIG = "names";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(HEADER_NAMES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
            @SuppressWarnings("unchecked")
            @Override
            public void ensureValid(String name, Object valueObject) {
                final NonEmptyListValidator nonEmptyListValidator = new NonEmptyListValidator();
                nonEmptyListValidator.ensureValid(name, valueObject);
                List<String> lst = (List<String>) valueObject;

                long emptyValues = lst.stream().filter(v -> v.isEmpty()).count();
                if (emptyValues > 0) {
                    throw new ConfigException("String must be non-blank");
                }
            }

            @Override
            public String toString() {
                return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
            }
            }, ConfigDef.Importance.MEDIUM,
            "Header name(s) to remove.");

    private List<String> headerNames;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerNames = config.getList(HEADER_NAMES_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }
        Headers recordHeaders = record.headers();
        if (recordHeaders.isEmpty()) {
            return record;
        }
        Headers newHeaders = new ConnectHeaders();
        recordHeaders.forEach(header -> {
                if (!headerNames.contains(header.key())) {
                    newHeaders.add(header);
                }
            }
        );
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
