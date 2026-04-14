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
 * Extracts lineage for the MongoDB Kafka Source Connector.
 *
 * <p>Inputs are MongoDB collections; outputs are Kafka topics.
 */
public final class MongoDbSourceVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "com.mongodb.kafka.connect.MongoSourceConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        String connectionUri = config.getOrDefault("connection.uri", "mongodb://localhost:27017");
        String database = config.getOrDefault("database", "");
        String collection = config.getOrDefault("collection", "");

        // Extract host from connection URI for namespace
        String namespace = extractNamespace(connectionUri);

        List<ConnectorLineage.Dataset> inputs = new ArrayList<>();
        if (!database.isEmpty()) {
            String name = collection.isEmpty() ? database : database + "." + collection;
            inputs.add(new ConnectorLineage.Dataset(namespace, name));
        }

        // Output topics
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        String topicPrefix = config.get("topic.prefix");
        if (topicPrefix != null && !topicPrefix.isEmpty()) {
            String kafkaNs = KafkaDatasetUtils.kafkaNamespace(config);
            String topicName = topicPrefix;
            if (!database.isEmpty()) {
                topicName += "." + database;
                if (!collection.isEmpty()) {
                    topicName += "." + collection;
                }
            }
            outputs.add(new ConnectorLineage.Dataset(kafkaNs, topicName));
        }
        if (outputs.isEmpty()) {
            outputs = KafkaDatasetUtils.resolveTopicDatasets(config);
        }

        return new ConnectorLineage(inputs, outputs, "MONGODB_SOURCE");
    }

    private static String extractNamespace(String uri) {
        // mongodb://host:port/... -> mongodb://host:port
        if (uri.startsWith("mongodb+srv://")) {
            int pathStart = uri.indexOf('/', "mongodb+srv://".length());
            return pathStart > 0 ? uri.substring(0, pathStart) : uri;
        }
        if (uri.startsWith("mongodb://")) {
            int pathStart = uri.indexOf('/', "mongodb://".length());
            return pathStart > 0 ? uri.substring(0, pathStart) : uri;
        }
        return uri;
    }
}
