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
 * Extracts lineage for the DataStax Apache Cassandra Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are Cassandra tables.
 *
 * <p>The connector maps topics to tables via properties like
 * {@code topic.my_topic.my_ks.my_table.mapping}.
 */
public final class CassandraSinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "com.datastax.oss.kafka.sink.CassandraSinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String contactPoints = config.getOrDefault("contactPoints",
            config.getOrDefault("datastax-java-driver.basic.contact-points.0",
                "localhost:9042"));
        String namespace = "cassandra://" + contactPoints.split(",")[0].trim();

        // Extract table mappings from topic.<topic>.<keyspace>.<table>.mapping
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        String mappingPrefix = "topic.";
        String mappingSuffix = ".mapping";
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(mappingPrefix) && key.endsWith(mappingSuffix)) {
                // key format: topic.<topic_name>.<keyspace>.<table>.mapping
                String withoutPrefix = key.substring(mappingPrefix.length());
                String withoutSuffix = withoutPrefix.substring(0,
                    withoutPrefix.length() - mappingSuffix.length());
                // We need at least topic.keyspace.table
                int firstDot = withoutSuffix.indexOf('.');
                if (firstDot > 0) {
                    String ksAndTable = withoutSuffix.substring(firstDot + 1);
                    outputs.add(new ConnectorLineage.Dataset(namespace, ksAndTable));
                }
            }
        }

        return new ConnectorLineage(inputs, outputs, "CASSANDRA_SINK");
    }
}
