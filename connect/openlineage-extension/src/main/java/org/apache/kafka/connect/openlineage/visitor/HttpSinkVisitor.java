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

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Extracts lineage for the Confluent HTTP Sink Connector.
 *
 * <p>Inputs are Kafka topics; the output is the HTTP endpoint.
 */
public final class HttpSinkVisitor implements ConnectorVisitor {

    private static final String CONNECTOR_CLASS =
        "io.confluent.connect.http.HttpSinkConnector";

    @Override
    public boolean matches(Map<String, String> config) {
        return CONNECTOR_CLASS.equals(config.get("connector.class"));
    }

    @Override
    public ConnectorLineage visit(Map<String, String> config) {
        List<ConnectorLineage.Dataset> inputs = KafkaDatasetUtils.resolveTopicDatasets(config);

        String httpApiUrl = config.getOrDefault("http.api.url", "http://unknown");
        // Use the host as the namespace and the path as the name
        String namespace;
        String name;
        try {
            URI uri = URI.create(httpApiUrl);
            int port = uri.getPort();
            String portStr = port > 0 ? ":" + port : "";
            namespace = uri.getScheme() + "://" + uri.getHost() + portStr;
            name = uri.getPath() != null && !uri.getPath().isEmpty()
                ? uri.getPath()
                : "/";
        } catch (Exception e) {
            namespace = httpApiUrl;
            name = "/";
        }

        List<ConnectorLineage.Dataset> outputs = Collections.singletonList(
            new ConnectorLineage.Dataset(namespace, name)
        );

        return new ConnectorLineage(inputs, outputs, "HTTP_SINK");
    }
}
