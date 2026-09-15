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
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Routes records to a destination topic based on the value of a Kafka record header.
 * Multiple header names can be specified in priority order; the first header found on
 * the record determines the destination topic. An optional fallback topic can be
 * configured for records with none of the specified headers present.
 */
public class HeaderRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(HeaderRouter.class);

    public static final String OVERVIEW_DOC =
            "Route records to a destination topic based on the value of a Kafka record header. " +
            "Multiple header names can be specified in priority order via <code>" + ConfigName.HEADER_NAMES + "</code>; " +
            "the first header found on the record determines the destination topic. " +
            "An optional <code>" + ConfigName.FALLBACK_TOPIC + "</code> can be configured for records " +
            "with none of the specified headers present. If no header matches and no fallback is set, " +
            "the record passes through with its original topic unchanged.";

    private interface ConfigName {
        String HEADER_NAMES = "header.names";
        String FALLBACK_TOPIC = "fallback.topic";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.HEADER_NAMES, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                    new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Priority-ordered list of header names to inspect. The value of the first header " +
                    "found on the record is used as the destination topic name.")
            .define(ConfigName.FALLBACK_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Topic to route records to when none of the configured headers are present. " +
                    "If not set, the record's original topic is preserved.");

    private List<String> headerNames;
    private String fallbackTopic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerNames = config.getList(ConfigName.HEADER_NAMES);
        final String fallback = config.getString(ConfigName.FALLBACK_TOPIC);
        fallbackTopic = (fallback != null && !fallback.isBlank()) ? fallback : null;
    }

    @Override
    public R apply(R record) {
        for (String name : headerNames) {
            final Header header = record.headers().lastWithName(name);
            if (header != null && header.value() != null && !headerValueAsString(header).isEmpty()) {
                final String newTopic = headerValueAsString(header);
                log.trace("Rerouting record from topic '{}' to '{}' via header '{}'",
                        record.topic(), newTopic, name);
                return record.newRecord(newTopic, record.kafkaPartition(),
                        record.keySchema(), record.key(),
                        record.valueSchema(), record.value(),
                        record.timestamp());
            }
        }
        if (fallbackTopic != null) {
            log.trace("No matching header found on record from topic '{}'; routing to fallback topic '{}'",
                    record.topic(), fallbackTopic);
            return record.newRecord(fallbackTopic, record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    record.valueSchema(), record.value(),
                    record.timestamp());
        }
        log.trace("No matching header found on record from topic '{}' and no fallback configured; passing through",
                record.topic());
        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private static String headerValueAsString(Header header) {
        final Object value = header.value();
        if (value instanceof String) return (String) value;
        if (value instanceof byte[]) return new String((byte[]) value, StandardCharsets.UTF_8);
        return value.toString();
    }
}
