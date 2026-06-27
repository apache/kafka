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
import java.util.Locale;
import java.util.Map;

/**
 * Extracts lineage for all Debezium CDC source connectors
 * ({@code io.debezium.connector.*}).
 *
 * <p>Inputs are the source database tables identified by
 * {@code database.hostname}, {@code database.port}, and
 * {@code table.include.list}.  Outputs are Kafka topics derived from
 * {@code topic.prefix} and the table names.
 */
public final class DebeziumVisitor implements ConnectorVisitor {

    private static final String DEBEZIUM_PREFIX = "io.debezium.connector.";

    @Override
    public boolean matches(Map<String, String> config) {
        String cls = config.get("connector.class");
        return cls != null && cls.startsWith(DEBEZIUM_PREFIX);
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        final String connectorClass = config.getOrDefault("connector.class", "");
        final String dbType = extractDbType(connectorClass);
        final String namespace = buildNamespace(config, dbType);
        final String dbName = resolveDatabaseName(config);
        final String tableList = resolveTableList(config);

        final List<ConnectorLineage.Dataset> inputs = buildInputs(namespace, dbName, tableList, dbType);
        final List<ConnectorLineage.Dataset> outputs = buildOutputs(config, tableList);

        return new ConnectorLineage(inputs, outputs, "DEBEZIUM_" + dbType.toUpperCase(Locale.ROOT));
    }

    private static String buildNamespace(Map<String, String> config, String dbType) {
        final String hostname = config.getOrDefault("database.hostname", "unknown");
        final String port = config.getOrDefault("database.port", defaultPort(dbType));
        // Normalize scheme per OL spec (e.g., postgresql → postgres)
        final String scheme = "postgresql".equals(dbType) ? "postgres" : dbType;
        return scheme + "://" + hostname + ":" + port;
    }

    private static String resolveDatabaseName(Map<String, String> config) {
        final String dbName = config.get("database.dbname");
        if (dbName == null || dbName.isEmpty()) {
            return config.getOrDefault("database.names", "");
        }
        return dbName;
    }

    private static String resolveTableList(Map<String, String> config) {
        final String tableList = config.get("table.include.list");
        if (tableList == null || tableList.isEmpty()) {
            return config.get("table.whitelist");
        }
        return tableList;
    }

    private static List<ConnectorLineage.Dataset> buildInputs(
            String namespace,
            String dbName,
            String tableList,
            String dbType) {
        final List<ConnectorLineage.Dataset> inputs = new ArrayList<>();
        if (tableList != null && !tableList.isEmpty()) {
            for (String table : tableList.split(",")) {
                final String trimmed = table.trim();
                if (!trimmed.isEmpty()) {
                    inputs.add(new ConnectorLineage.Dataset(namespace,
                        qualifyTable(dbType, dbName, trimmed)));
                }
            }
        } else if (!dbName.isEmpty()) {
            inputs.add(new ConnectorLineage.Dataset(namespace, dbName));
        }
        return inputs;
    }

    /**
     * Apply OpenLineage naming to a Debezium source table.  Debezium's
     * {@code table.include.list} is {@code schema.table} for Postgres/SQL Server
     * and {@code database.table} for MySQL.  Postgres/SQL Server names are
     * promoted to the three-part {@code database.schema.table} when the database
     * name is known; MySQL (already {@code database.table}) is left unchanged.
     */
    private static String qualifyTable(String dbType, String dbName, String table) {
        final boolean threePart = "postgresql".equals(dbType)
            || "postgres".equals(dbType)
            || "sqlserver".equals(dbType);
        if (threePart && !dbName.isEmpty() && table.indexOf('.') >= 0) {
            return dbName + "." + table;
        }
        return table;
    }

    private static List<ConnectorLineage.Dataset> buildOutputs(
            Map<String, String> config,
            String tableList) {
        final String topicPrefix = resolveTopicPrefix(config);
        final List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        if (topicPrefix != null && !topicPrefix.isEmpty() && tableList != null) {
            final String kafkaNs = KafkaDatasetUtils.kafkaNamespace(config);
            for (String table : tableList.split(",")) {
                final String trimmed = table.trim();
                if (!trimmed.isEmpty()) {
                    outputs.add(new ConnectorLineage.Dataset(kafkaNs,
                        topicPrefix + "." + trimmed));
                }
            }
        }
        if (outputs.isEmpty() && topicPrefix != null && !topicPrefix.isEmpty()) {
            final String kafkaNs = KafkaDatasetUtils.kafkaNamespace(config);
            outputs.add(new ConnectorLineage.Dataset(kafkaNs, topicPrefix + ".*"));
        }
        return outputs;
    }

    private static String resolveTopicPrefix(Map<String, String> config) {
        final String topicPrefix = config.get("topic.prefix");
        if (topicPrefix == null || topicPrefix.isEmpty()) {
            return config.get("database.server.name");
        }
        return topicPrefix;
    }

    /**
     * Extract the database type from the Debezium connector class name.
     * For example, {@code io.debezium.connector.mysql.MySqlConnector} yields
     * {@code mysql}.
     */
    private static String extractDbType(String connectorClass) {
        // io.debezium.connector.<type>.<Class>
        String afterPrefix = connectorClass.substring(DEBEZIUM_PREFIX.length());
        int dot = afterPrefix.indexOf('.');
        if (dot > 0) {
            return afterPrefix.substring(0, dot).toLowerCase(Locale.ROOT);
        }
        return "unknown";
    }

    private static String defaultPort(String dbType) {
        switch (dbType) {
            case "mysql":
                return "3306";
            case "postgresql":
            case "postgres":
                return "5432";
            case "sqlserver":
                return "1433";
            case "oracle":
                return "1521";
            case "mongodb":
                return "27017";
            case "db2":
                return "50000";
            default:
                return "0";
        }
    }
}
