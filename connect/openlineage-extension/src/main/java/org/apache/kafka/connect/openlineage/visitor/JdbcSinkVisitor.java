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
 * Extracts lineage for the Confluent JDBC Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are database tables.  The JDBC URL is
 * parsed to derive the database namespace and dialect-aware naming (three-part
 * {@code database.schema.table} for Postgres/SQL Server, two-part
 * {@code database.table} for MySQL).  The table name mapping is determined from
 * {@code table.name.format} or defaults to the topic name.
 */
public final class JdbcSinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "io.confluent.connect.jdbc.JdbcSinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        String jdbcUrl = config.get("connection.url");
        JdbcUrlParser.JdbcConnectionInfo info = JdbcUrlParser.parse(jdbcUrl);

        // Inputs are Kafka topics
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        // Outputs are database tables
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        String tableFormat = config.get("table.name.format");
        List<String> topics = KafkaDatasetUtils.parseTopics(config);

        for (String topic : topics) {
            String tableName;
            if (tableFormat != null && !tableFormat.isEmpty()) {
                tableName = tableFormat.replace("${topic}", topic);
            } else {
                tableName = topic;
            }
            outputs.add(new ConnectorLineage.Dataset(info.namespace(), info.qualify(tableName)));
        }

        return new ConnectorLineage(inputs, outputs, "JDBC_SINK");
    }
}
