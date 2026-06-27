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
 * Extracts lineage for the Confluent S3 Sink Connector.
 *
 * <p>Inputs are Kafka topics; outputs are S3 object paths
 * ({@code s3://{bucket}} / {@code {topics.dir}/{topic}}).
 */
public final class S3SinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "io.confluent.connect.s3.S3SinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String bucket = config.getOrDefault("s3.bucket.name", "unknown-bucket");
        String topicsDir = config.getOrDefault("topics.dir", "topics");
        String namespace = "s3://" + bucket;

        List<ConnectorLineage.Dataset> outputs =
            StorageDatasetUtils.pathDatasets(namespace, topicsDir, config);

        return new ConnectorLineage(inputs, outputs, "S3_SINK");
    }
}
