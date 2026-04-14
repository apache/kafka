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

package org.apache.kafka.connect.openlineage.util;

import org.apache.kafka.connect.openlineage.ConnectorLineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility methods for building Kafka topic datasets from connector
 * configurations.
 *
 * <p>The OpenLineage namespace for Kafka topics follows the convention
 * {@code kafka://<bootstrap.servers>}.  When multiple bootstrap servers are
 * configured, only the first one is used for the namespace.
 */
public final class KafkaDatasetUtils {

    /** Default Kafka namespace when bootstrap servers are not available. */
    public static final String DEFAULT_NAMESPACE = "kafka://localhost:9092";

    private KafkaDatasetUtils() {
        // utility class
    }

    /**
     * Derive a Kafka namespace from the connector configuration.
     *
     * @param config the connector configuration
     * @return a namespace string like {@code kafka://broker:9092}
     */
    public static String kafkaNamespace(Map<String, String> config) {
        // Try common bootstrap server config keys
        String servers = config.get("bootstrap.servers");
        if (servers == null || servers.isEmpty()) {
            servers = config.get("consumer.bootstrap.servers");
        }
        if (servers == null || servers.isEmpty()) {
            servers = config.get("producer.bootstrap.servers");
        }
        if (servers == null || servers.isEmpty()) {
            return DEFAULT_NAMESPACE;
        }
        // Use the first server for the namespace
        String firstServer = servers.split(",")[0].trim();
        return "kafka://" + firstServer;
    }

    /**
     * Parse the {@code topics} configuration value (comma-separated) into a
     * list of topic names.
     *
     * @param config the connector configuration
     * @return list of topic names; may be empty
     */
    public static List<String> parseTopics(Map<String, String> config) {
        String topics = config.get("topics");
        if (topics == null || topics.isEmpty()) {
            // Debezium uses topic.prefix, not topics
            return Collections.emptyList();
        }
        return Arrays.stream(topics.split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    }

    /**
     * Build a list of Kafka topic datasets from the connector configuration.
     *
     * @param config the connector configuration
     * @return datasets for each configured topic
     */
    public static List<ConnectorLineage.Dataset> topicDatasets(Map<String, String> config) {
        String namespace = kafkaNamespace(config);
        List<String> topics = parseTopics(config);
        List<ConnectorLineage.Dataset> datasets = new ArrayList<>();
        for (String topic : topics) {
            datasets.add(new ConnectorLineage.Dataset(namespace, topic));
        }
        return datasets;
    }

    /**
     * Build a list of Kafka topic datasets for a topics regex pattern.
     * Since we cannot resolve the actual topic names at config time, we
     * create a single dataset entry with the regex pattern as the name.
     *
     * @param config the connector configuration
     * @return a single-element list with the regex pattern, or empty
     */
    public static List<ConnectorLineage.Dataset> topicRegexDatasets(Map<String, String> config) {
        String namespace = kafkaNamespace(config);
        String regex = config.get("topics.regex");
        if (regex == null || regex.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
            new ConnectorLineage.Dataset(namespace, "regex:" + regex)
        );
    }

    /**
     * Build topic datasets, trying {@code topics} first, then
     * {@code topics.regex}.
     *
     * @param config the connector configuration
     * @return topic datasets
     */
    public static List<ConnectorLineage.Dataset> resolveTopicDatasets(Map<String, String> config) {
        List<ConnectorLineage.Dataset> datasets = topicDatasets(config);
        if (datasets.isEmpty()) {
            datasets = topicRegexDatasets(config);
        }
        return datasets;
    }
}
