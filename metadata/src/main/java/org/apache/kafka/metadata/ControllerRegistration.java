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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerEndpoint;
import org.apache.kafka.common.metadata.RegisterControllerRecord.ControllerFeature;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An immutable class which represents controller registrations.
 */
public class ControllerRegistration {
    public static class Builder {
        private int id;
        private Uuid incarnationId;
        private boolean zkMigrationReady;
        private Map<String, Endpoint> listeners;
        private Map<String, VersionRange> supportedFeatures;

        public Builder() {
            this.id = 0;
            this.incarnationId = null;
            this.zkMigrationReady = false;
            this.listeners = null;
            this.supportedFeatures = null;
        }

        public Builder(RegisterControllerRecord record) {
            this.id = record.controllerId();
            this.incarnationId = record.incarnationId();
            this.zkMigrationReady = record.zkMigrationReady();
            Map<String, Endpoint> newListeners = new HashMap<>();
            record.endPoints().forEach(endPoint -> {
                SecurityProtocol protocol = SecurityProtocol.forId(endPoint.securityProtocol());
                if (protocol == null) {
                    throw new RuntimeException("Unknown security protocol " +
                            (int) endPoint.securityProtocol());
                }
                newListeners.put(endPoint.name(), new Endpoint(endPoint.name(),
                    protocol,
                    endPoint.host(),
                    endPoint.port()));
            });
            this.listeners = Collections.unmodifiableMap(newListeners);
            Map<String, VersionRange> newSupportedFeatures = new HashMap<>();
            record.features().forEach(feature -> {
                newSupportedFeatures.put(feature.name(), VersionRange.of(
                        feature.minSupportedVersion(), feature.maxSupportedVersion()));
            });
            this.supportedFeatures = Collections.unmodifiableMap(newSupportedFeatures);
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setIncarnationId(Uuid incarnationId) {
            this.incarnationId = incarnationId;
            return this;
        }

        public Builder setZkMigrationReady(boolean zkMigrationReady) {
            this.zkMigrationReady = zkMigrationReady;
            return this;
        }

        public Builder setListeners(Map<String, Endpoint> listeners) {
            this.listeners = listeners;
            return this;
        }

        public Builder setSupportedFeatures(Map<String, VersionRange> supportedFeatures) {
            this.supportedFeatures = supportedFeatures;
            return this;
        }

        public ControllerRegistration build() {
            if (incarnationId == null) throw new RuntimeException("You must set incarnationId.");
            if (listeners == null) throw new RuntimeException("You must set listeners.");
            if (supportedFeatures == null) {
                supportedFeatures = new HashMap<>();
                supportedFeatures.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
                        MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
                        MetadataVersion.latestProduction().featureLevel()));
            }
            return new ControllerRegistration(id,
                incarnationId,
                zkMigrationReady,
                listeners,
                supportedFeatures);
        }
    }

    private final int id;
    private final Uuid incarnationId;
    private final boolean zkMigrationReady;
    private final Map<String, Endpoint> listeners;
    private final Map<String, VersionRange> supportedFeatures;

    private ControllerRegistration(int id,
        Uuid incarnationId,
        boolean zkMigrationReady,
        Map<String, Endpoint> listeners,
        Map<String, VersionRange> supportedFeatures
    ) {
        this.id = id;
        this.incarnationId = incarnationId;
        this.zkMigrationReady = zkMigrationReady;
        this.listeners = listeners;
        this.supportedFeatures = supportedFeatures;
    }

    public int id() {
        return id;
    }

    public Uuid incarnationId() {
        return incarnationId;
    }

    public boolean zkMigrationReady() {
        return zkMigrationReady;
    }

    public Map<String, Endpoint> listeners() {
        return listeners;
    }

    public Optional<Node> node(String listenerName) {
        Endpoint endpoint = listeners().get(listenerName);
        if (endpoint == null) {
            return Optional.empty();
        }
        return Optional.of(new Node(id, endpoint.host(), endpoint.port(), null));
    }

    public Map<String, VersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    public ApiMessageAndVersion toRecord(ImageWriterOptions options) {
        RegisterControllerRecord registrationRecord = new RegisterControllerRecord().
            setControllerId(id).
            setIncarnationId(incarnationId).
            setZkMigrationReady(zkMigrationReady);
        for (Entry<String, Endpoint> entry : listeners.entrySet()) {
            Endpoint endpoint = entry.getValue();
            registrationRecord.endPoints().add(new ControllerEndpoint().
                setName(entry.getKey()).
                setHost(endpoint.host()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        }
        for (Entry<String, VersionRange> entry : supportedFeatures.entrySet()) {
            registrationRecord.features().add(new ControllerFeature().
                setName(entry.getKey()).
                setMinSupportedVersion(entry.getValue().min()).
                setMaxSupportedVersion(entry.getValue().max()));
        }
        return new ApiMessageAndVersion(registrationRecord,
            options.metadataVersion().registerControllerRecordVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id,
            incarnationId,
            zkMigrationReady,
            listeners,
            supportedFeatures);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ControllerRegistration)) return false;
        ControllerRegistration other = (ControllerRegistration) o;
        return other.id == id &&
            other.incarnationId.equals(incarnationId) &&
            other.zkMigrationReady == zkMigrationReady &&
            other.listeners.equals(listeners) &&
            other.supportedFeatures.equals(supportedFeatures);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("ControllerRegistration(id=").append(id);
        bld.append(", incarnationId=").append(incarnationId);
        bld.append(", zkMigrationReady=").append(zkMigrationReady);
        bld.append(", listeners=[").append(
            listeners.keySet().stream().sorted().
                map(n -> listeners.get(n).toString()).
                collect(Collectors.joining(", ")));
        bld.append("], supportedFeatures={").append(
            supportedFeatures.keySet().stream().sorted().
                map(k -> k + ": " + supportedFeatures.get(k)).
                collect(Collectors.joining(", ")));
        bld.append("}");
        bld.append(")");
        return bld.toString();
    }
}
