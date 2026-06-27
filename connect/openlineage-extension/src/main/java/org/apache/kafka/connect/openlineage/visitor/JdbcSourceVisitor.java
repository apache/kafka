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
import org.apache.kafka.connect.openlineage.util.JdbcUrlParser;
import org.apache.kafka.connect.openlineage.util.KafkaDatasetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extracts lineage for the Confluent JDBC Source Connector.
 *
 * <p>Inputs are database tables; outputs are Kafka topics.  The JDBC URL is
 * parsed to derive the database namespace and the {@code table.whitelist} /
 * {@code table.include.list} property provides table names.
 */
public final class JdbcSourceVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "io.confluent.connect.jdbc.JdbcSourceConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        final String jdbcUrl = config.get("connection.url");
        final JdbcUrlParser.JdbcConnectionInfo info = JdbcUrlParser.parse(jdbcUrl);
        final String tables = resolveTableList(config);

        final List<ConnectorLineage.Dataset> inputs = buildInputs(config, info, tables);
        final List<ConnectorLineage.Dataset> outputs = buildOutputs(config, tables);

        return new ConnectorLineage(inputs, outputs, "JDBC_SOURCE");
    }

    /**
     * Resolve the table list from {@code table.include.list} (preferred) or
     * the legacy {@code table.whitelist}.
     */
    private static String resolveTableList(Map<String, String> config) {
        String tables = config.get("table.include.list");
        if (tables == null || tables.isEmpty()) {
            tables = config.get("table.whitelist");
        }
        return tables;
    }

    /**
     * Build input datasets from the JDBC connection info and table list.
     */
    private static List<ConnectorLineage.Dataset> buildInputs(
            Map<String, String> config,
            JdbcUrlParser.JdbcConnectionInfo info,
            String tables) {
        final List<ConnectorLineage.Dataset> inputs = new ArrayList<>();
        if (tables != null && !tables.isEmpty()) {
            addTableDatasets(inputs, info, tables);
        }
        // If using query mode, there might be no explicit tables
        if (inputs.isEmpty() && config.containsKey("query")) {
            inputs.add(new ConnectorLineage.Dataset(info.namespace(), info.qualify("query")));
        }
        return inputs;
    }

    private static void addTableDatasets(
            List<ConnectorLineage.Dataset> datasets,
            JdbcUrlParser.JdbcConnectionInfo info,
            String tables) {
        for (String table : tables.split(",")) {
            final String trimmed = table.trim();
            if (!trimmed.isEmpty()) {
                datasets.add(new ConnectorLineage.Dataset(info.namespace(), info.qualify(trimmed)));
            }
        }
    }

    /**
     * Build output Kafka topic datasets from the topic prefix and table list.
     */
    private static List<ConnectorLineage.Dataset> buildOutputs(
            Map<String, String> config,
            String tables) {
        final List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        final String topicPrefix = config.get("topic.prefix");
        if (topicPrefix != null && !topicPrefix.isEmpty() && tables != null) {
            final String kafkaNs = KafkaDatasetUtils.kafkaNamespace(config);
            for (String table : tables.split(",")) {
                final String trimmed = table.trim();
                if (!trimmed.isEmpty()) {
                    outputs.add(new ConnectorLineage.Dataset(kafkaNs, topicPrefix + trimmed));
                }
            }
        }
        if (outputs.isEmpty()) {
            return KafkaDatasetUtils.resolveTopicDatasets(config);
        }
        return outputs;
    }
}
