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
 * Extracts lineage for the WePay BigQuery Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are BigQuery tables. Per the OpenLineage
 * naming spec the namespace is the constant {@code bigquery} and the name is the
 * fully-qualified {@code {project}.{dataset}.{table}}.
 */
public final class BigQuerySinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String project = config.getOrDefault("project", "unknown-project");
        String defaultDataset = config.getOrDefault("defaultDataset",
            config.getOrDefault("datasets", "unknown-dataset"));
        // OpenLineage BigQuery naming: namespace is the literal "bigquery",
        // name is {project}.{dataset}.{table}.
        String namespace = "bigquery";

        // Each topic maps to a table; by default the table name is the topic name
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        List<String> topics = KafkaDatasetUtils.parseTopics(config);
        for (String topic : topics) {
            outputs.add(new ConnectorLineage.Dataset(namespace,
                project + "." + defaultDataset + "." + topic));
        }

        return new ConnectorLineage(inputs, outputs, "BIGQUERY_SINK");
    }
}
