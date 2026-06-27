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
import org.apache.kafka.connect.openlineage.util.StorageDatasetUtils;

import java.util.List;
import java.util.Map;

/**
 * Extracts lineage for Confluent HDFS 2 and HDFS 3 Sink Connectors.
 *
 * <p>Inputs are Kafka topics; outputs are HDFS paths
 * ({@code hdfs://{namenode}:{port}} / {@code {topics.dir}/{topic}}).
 */
public final class HdfsSinkVisitor implements ConnectorVisitor {

    private static final String HDFS2_CONNECTOR_CLASS =
        "io.confluent.connect.hdfs.HdfsSinkConnector";
    private static final String HDFS3_CONNECTOR_CLASS =
        "io.confluent.connect.hdfs3.Hdfs3SinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        String cls = config.get("connector.class");
        return HDFS2_CONNECTOR_CLASS.equals(cls) || HDFS3_CONNECTOR_CLASS.equals(cls);
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String hdfsUrl = config.getOrDefault("hdfs.url", "hdfs://unknown:8020");
        String topicsDir = config.getOrDefault("topics.dir", "topics");

        // Normalize: strip trailing slash
        if (hdfsUrl.endsWith("/")) {
            hdfsUrl = hdfsUrl.substring(0, hdfsUrl.length() - 1);
        }

        List<ConnectorLineage.Dataset> outputs =
            StorageDatasetUtils.pathDatasets(hdfsUrl, topicsDir, config);

        return new ConnectorLineage(inputs, outputs, "HDFS_SINK");
    }
}
