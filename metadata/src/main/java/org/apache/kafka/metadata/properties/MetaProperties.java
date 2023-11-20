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

package org.apache.kafka.metadata.properties;

import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

/**
 * An immutable class which contains the per-log-directory information stored in an individual
 * meta.properties file.
 */
public final class MetaProperties {
    /**
     * The property that sets the version number.
     */
    static final String VERSION_PROP = "version";

    /**
     * The property that specifies the cluster id.
     */
    static final String CLUSTER_ID_PROP = "cluster.id";

    /**
     * The property that specifies the broker id. Only in V0.
     */
    static final String BROKER_ID_PROP = "broker.id";

    /**
     * The property that specifies the node id. Replaces broker.id in V1.
     */
    static final String NODE_ID_PROP = "node.id";

    /**
     * The property that specifies the directory id.
     */
    static final String DIRECTORY_ID_PROP = "directory.id";

    /**
     * The version of the MetaProperties file.
     */
    private final MetaPropertiesVersion version;

    /**
     * The cluster ID, which may be Optional.empty in V0.
     */
    private final Optional<String> clusterId;

    /**
     * The node ID, which may be OptionalInt.empty in V0.
     */
    private final OptionalInt nodeId;

    /**
     * The directory ID, or Optional.empty if none is specified.
     */
    private final Optional<Uuid> directoryId;

    /**
     * Creates a new MetaProperties object.
     */
    public static class Builder {
        private MetaPropertiesVersion version = MetaPropertiesVersion.V0;
        private Optional<String> clusterId = Optional.empty();
        private OptionalInt nodeId = OptionalInt.empty();
        private Optional<Uuid> directoryId = Optional.empty();

        public Builder() {
        }

        public Builder(Optional<MetaProperties> metaProps) {
            if (metaProps.isPresent()) {
                this.version = metaProps.get().version();
                this.clusterId = metaProps.get().clusterId();
                this.nodeId = metaProps.get().nodeId();
                this.directoryId = metaProps.get().directoryId();
            }
        }

        public Builder(MetaProperties metaProps) {
            this(Optional.of(metaProps));
        }

        public Builder(Properties props) {
            this.version = MetaPropertiesVersion.fromNumberString(
                props.getProperty(VERSION_PROP,
                    MetaPropertiesVersion.V0.numberString()));
            if (version.hasBrokerId()) {
                if (props.containsKey(BROKER_ID_PROP)) {
                    this.nodeId = OptionalInt.of(PropertiesUtils.loadRequiredIntProp(props, BROKER_ID_PROP));
                }
            } else {
                this.nodeId = OptionalInt.of(PropertiesUtils.loadRequiredIntProp(props, NODE_ID_PROP));
            }
            this.clusterId = Optional.ofNullable(props.getProperty(CLUSTER_ID_PROP));
            if (props.containsKey(DIRECTORY_ID_PROP)) {
                try {
                    this.directoryId = Optional.of(Uuid.fromString(props.getProperty(DIRECTORY_ID_PROP)));
                } catch (Exception e) {
                    throw new RuntimeException("Unable to read " + DIRECTORY_ID_PROP + " as a Uuid: " +
                            e.getMessage(), e);
                }
            } else {
                this.directoryId = Optional.empty();
            }
        }

        public MetaPropertiesVersion version() {
            return version;
        }

        public Builder setVersion(MetaPropertiesVersion version) {
            this.version = version;
            return this;
        }

        public Optional<String> clusterId() {
            return clusterId;
        }

        public Builder setClusterId(String clusterId) {
            return setClusterId(Optional.of(clusterId));
        }

        public Builder setClusterId(Optional<String> clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public OptionalInt nodeId() {
            return nodeId;
        }

        public Builder setNodeId(OptionalInt nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setNodeId(int nodeId) {
            return setNodeId(OptionalInt.of(nodeId));
        }

        public Optional<Uuid> directoryId() {
            return directoryId;
        }

        public Builder setDirectoryId(Optional<Uuid> directoryId) {
            this.directoryId = directoryId;
            return this;
        }

        public Builder setDirectoryId(Uuid directoryId) {
            return setDirectoryId(Optional.of(directoryId));
        }

        public MetaProperties build() {
            if (!version.equals(MetaPropertiesVersion.V0)) {
                if (!clusterId.isPresent()) {
                    throw new RuntimeException("cluster.id was not found.");
                }
                if (!nodeId.isPresent()) {
                    throw new RuntimeException("node.id was not found.");
                }
            }
            return new MetaProperties(version,
                clusterId,
                nodeId,
                directoryId);
        }
    }

    private MetaProperties(
        MetaPropertiesVersion version,
        Optional<String> clusterId,
        OptionalInt nodeId,
        Optional<Uuid> directoryId
    ) {
        this.version = version;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.directoryId = directoryId;
    }

    public MetaPropertiesVersion version() {
        return version;
    }

    public Optional<String> clusterId() {
        return clusterId;
    }

    public OptionalInt nodeId() {
        return nodeId;
    }

    public Optional<Uuid> directoryId() {
        return directoryId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version,
            clusterId,
            nodeId,
            directoryId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(MetaProperties.class))) return false;
        MetaProperties other = (MetaProperties) o;
        return version.equals(other.version) &&
            clusterId.equals(other.clusterId) &&
            nodeId.equals(other.nodeId) &&
            directoryId.equals(other.directoryId);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("MetaProperties");
        bld.append("(version=").append(version.number());
        if (clusterId.isPresent()) {
            bld.append(", clusterId=").append(clusterId.get());
        }
        if (nodeId.isPresent()) {
            bld.append(", nodeId=").append(nodeId.getAsInt());
        }
        if (directoryId.isPresent()) {
            bld.append(", directoryId=").append(directoryId.get());
        }
        bld.append(")");
        return bld.toString();
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.setProperty(VERSION_PROP, version.numberString());
        if (clusterId.isPresent()) {
            props.setProperty(CLUSTER_ID_PROP, clusterId.get());
        }
        if (version.hasBrokerId()) {
            if (nodeId.isPresent()) {
                props.setProperty(BROKER_ID_PROP, "" + nodeId.getAsInt());
            }
        } else {
            props.setProperty(NODE_ID_PROP, "" + nodeId.getAsInt());
        }
        if (directoryId.isPresent()) {
            props.setProperty(DIRECTORY_ID_PROP, directoryId.get().toString());
        }
        return props;
    }
}
