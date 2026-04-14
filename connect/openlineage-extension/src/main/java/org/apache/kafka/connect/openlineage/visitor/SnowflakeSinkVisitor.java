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

package org.apache.kafka.connect.openlineage.visitor;

import org.apache.kafka.connect.openlineage.ConnectorLineage;
import org.apache.kafka.connect.openlineage.ConnectorVisitor;
import org.apache.kafka.connect.openlineage.util.KafkaDatasetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extracts lineage for the Snowflake Kafka Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are Snowflake tables.
 */
public final class SnowflakeSinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "com.snowflake.kafka.connector.SnowflakeSinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String urlName = config.getOrDefault("snowflake.url.name", "unknown.snowflakecomputing.com");
        String database = config.getOrDefault("snowflake.database.name", "unknown-db");
        String schema = config.getOrDefault("snowflake.schema.name", "PUBLIC");
        String namespace = "snowflake://" + urlName;

        // Each topic maps to a table. topic2table.map overrides default naming
        String topic2table = config.get("snowflake.topic2table.map");
        Map<String, String> tableMap = parseTableMap(topic2table);

        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        List<String> topics = KafkaDatasetUtils.parseTopics(config);
        for (String topic : topics) {
            String tableName = tableMap.getOrDefault(topic, topic);
            outputs.add(new ConnectorLineage.Dataset(namespace,
                database + "." + schema + "." + tableName));
        }

        return new ConnectorLineage(inputs, outputs, "SNOWFLAKE_SINK");
    }

    /**
     * Parse the {@code snowflake.topic2table.map} config value.
     * Format: {@code topic1:table1,topic2:table2}
     */
    private static Map<String, String> parseTableMap(String mapStr) {
        java.util.Map<String, String> result = new java.util.HashMap<>();
        if (mapStr == null || mapStr.isEmpty()) {
            return result;
        }
        for (String entry : mapStr.split(",")) {
            String[] parts = entry.split(":");
            if (parts.length == 2) {
                result.put(parts[0].trim(), parts[1].trim());
            }
        }
        return result;
    }
}
