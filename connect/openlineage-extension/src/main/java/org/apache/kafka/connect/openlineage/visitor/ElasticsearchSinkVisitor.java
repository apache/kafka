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
 * Extracts lineage for the Confluent Elasticsearch Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are Elasticsearch indices.
 */
public final class ElasticsearchSinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String connectionUrl = config.getOrDefault("connection.url", "http://localhost:9200");
        // Convert http(s)://host:port to elasticsearch://host:port per OL naming
        String namespace = toElasticsearchNamespace(connectionUrl);

        // Outputs: each topic maps to an index (by default the index name
        // equals the topic name)
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        List<String> topics = KafkaDatasetUtils.parseTopics(config);
        for (String topic : topics) {
            outputs.add(new ConnectorLineage.Dataset(namespace, topic));
        }

        return new ConnectorLineage(inputs, outputs, "ELASTICSEARCH_SINK");
    }

    private static String toElasticsearchNamespace(String connectionUrl) {
        // Strip scheme (http:// or https://) and replace with elasticsearch://
        String stripped = connectionUrl.replaceFirst("^https?://", "");
        // Remove trailing slash
        if (stripped.endsWith("/")) {
            stripped = stripped.substring(0, stripped.length() - 1);
        }
        return "elasticsearch://" + stripped;
    }
}
