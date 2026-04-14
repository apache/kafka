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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Fallback visitor that matches any connector and extracts only the Kafka
 * topic datasets.  This is always registered last in the
 * {@link org.apache.kafka.connect.openlineage.VisitorRegistry}.
 */
public final class GenericVisitor implements ConnectorVisitor {

    @Override
    public boolean matches(Map<String, String> config) {
        // Matches everything as a catch-all
        return true;
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> topicDatasets =
            KafkaDatasetUtils.resolveTopicDatasets(config);

        String connectorClass = config.getOrDefault("connector.class", "UNKNOWN");
        String shortName = connectorClass;
        int lastDot = connectorClass.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < connectorClass.length() - 1) {
            shortName = connectorClass.substring(lastDot + 1);
        }

        // For source connectors, topics are outputs; for sink connectors,
        // topics are inputs.  We try to guess from the class name.
        boolean isSink = shortName.toLowerCase(java.util.Locale.ROOT).contains("sink");
        if (isSink) {
            return new ConnectorLineage(topicDatasets, Collections.emptyList(), shortName);
        } else {
            return new ConnectorLineage(Collections.emptyList(), topicDatasets, shortName);
        }
    }
}
