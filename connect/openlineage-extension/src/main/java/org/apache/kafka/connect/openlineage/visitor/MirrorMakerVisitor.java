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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts lineage for the Kafka MirrorSourceConnector (MirrorMaker 2).
 *
 * <p>Inputs are topics on the source cluster; outputs are the corresponding
 * prefixed topics on the target cluster.
 */
public final class MirrorMakerVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "org.apache.kafka.connect.mirror.MirrorSourceConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        String sourceClusterAlias = config.getOrDefault("source.cluster.alias", "source");

        String sourceBootstrap = config.getOrDefault(
            "source.cluster.bootstrap.servers",
            config.getOrDefault("source->target.bootstrap.servers", "localhost:9092"));
        String targetBootstrap = config.getOrDefault(
            "target.cluster.bootstrap.servers",
            config.getOrDefault("bootstrap.servers", "localhost:9092"));

        String sourceNamespace = "kafka://" + sourceBootstrap.split(",")[0].trim();
        String targetNamespace = "kafka://" + targetBootstrap.split(",")[0].trim();

        // Topics to replicate
        String topicsStr = config.get("topics");
        List<String> topics;
        if (topicsStr != null && !topicsStr.isEmpty()) {
            topics = Arrays.stream(topicsStr.split(","))
                .map(String::trim)
                .filter(t -> !t.isEmpty())
                .collect(Collectors.toList());
        } else {
            topics = Collections.singletonList("*");
        }

        List<ConnectorLineage.Dataset> inputs = new ArrayList<>();
        List<ConnectorLineage.Dataset> outputs = new ArrayList<>();
        for (String topic : topics) {
            inputs.add(new ConnectorLineage.Dataset(sourceNamespace, topic));
            // MirrorMaker prefixes the source alias to the topic name
            outputs.add(new ConnectorLineage.Dataset(targetNamespace,
                sourceClusterAlias + "." + topic));
        }

        return new ConnectorLineage(inputs, outputs, "MIRROR_SOURCE");
    }
}
