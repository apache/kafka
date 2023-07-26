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

package org.apache.kafka.metadata.broker;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.metadata.broker.RawMetaProperties.BROKER_ID_KEY;
import static org.apache.kafka.metadata.broker.RawMetaProperties.CLUSTER_ID_KEY;
import static org.apache.kafka.metadata.broker.RawMetaProperties.NODE_ID_KEY;

public class MetaProperties {
    private String clusterId;
    private int nodeId;

    public MetaProperties(String clusterId, int nodeId) {
        this.clusterId = clusterId;
        this.nodeId = nodeId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public Properties toProperties() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(1);
        properties.setClusterId(clusterId);
        properties.setNodeId(nodeId);
        return properties.getProps();
    }

    public static MetaProperties parse(RawMetaProperties properties) {
        String clusterId = require(CLUSTER_ID_KEY, properties.getClusterId());

        if (properties.getVersion() == 1) {
            Integer nodeId = require(NODE_ID_KEY, properties.getNodeId());
            return new MetaProperties(clusterId, nodeId);
        } else if (properties.getVersion() == 0) {
            Integer brokerId = require(BROKER_ID_KEY, properties.getBrokerId());
            return new MetaProperties(clusterId, brokerId);
        } else {
            throw new RuntimeException("Expected version 0 or 1, but got version " + properties.getVersion());
        }
    }

    private static <T> T require(String key, Optional<T> value) {
        return value.orElseThrow(() -> new RuntimeException("Failed to find required property " + key + "."));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaProperties that = (MetaProperties) o;
        return nodeId == that.nodeId && Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, nodeId);
    }
}
