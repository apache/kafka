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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerFeature;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An immutable class which represents broker registrations.
 */
public class BrokerRegistration {
    public static class Builder {
        private int id = 0;
        private long epoch = -1;
        private Uuid incarnationId = null;
        private Map<String, Endpoint> listeners;
        private Map<String, VersionRange> supportedFeatures;
        private Optional<String> rack = Optional.empty();
        private boolean fenced = false;
        private boolean inControlledShutdown = false;
        private boolean isMigratingZkBroker = false;
        private List<Uuid> directories;

        public Builder() {
            this.id = 0;
            this.epoch = -1;
            this.incarnationId = null;
            this.listeners = new HashMap<>();
            this.supportedFeatures = new HashMap<>();
            this.rack = Optional.empty();
            this.fenced = false;
            this.inControlledShutdown = false;
            this.isMigratingZkBroker = false;
            this.directories = Collections.emptyList();
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setEpoch(long epoch) {
            this.epoch = epoch;
            return this;
        }

        public Builder setIncarnationId(Uuid incarnationId) {
            this.incarnationId = incarnationId;
            return this;
        }

        public Builder setListeners(List<Endpoint> listeners) {
            Map<String, Endpoint> listenersMap = new HashMap<>();
            for (Endpoint endpoint : listeners) {
                listenersMap.put(endpoint.listenerName().get(), endpoint);
            }
            this.listeners = listenersMap;
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

        public Builder setRack(Optional<String> rack) {
            Objects.requireNonNull(rack);
            this.rack = rack;
            return this;
        }

        public Builder setFenced(boolean fenced) {
            this.fenced = fenced;
            return this;
        }

        public Builder setInControlledShutdown(boolean inControlledShutdown) {
            this.inControlledShutdown = inControlledShutdown;
            return this;
        }

        public Builder setIsMigratingZkBroker(boolean isMigratingZkBroker) {
            this.isMigratingZkBroker = isMigratingZkBroker;
            return this;
        }

        public Builder setDirectories(List<Uuid> directories) {
            this.directories = directories;
            return this;
        }

        public BrokerRegistration build() {
            return new BrokerRegistration(
                id,
                epoch,
                incarnationId,
                listeners,
                supportedFeatures,
                rack,
                fenced,
                inControlledShutdown,
                isMigratingZkBroker,
                directories);
        }
    }

    public static Optional<Long> zkBrokerEpoch(long value) {
        if (value == -1) {
            return Optional.empty();
        } else {
            return Optional.of(value);
        }
    }

    private final int id;
    private final long epoch;
    private final Uuid incarnationId;
    private final Map<String, Endpoint> listeners;
    private final Map<String, VersionRange> supportedFeatures;
    private final Optional<String> rack;
    private final boolean fenced;
    private final boolean inControlledShutdown;
    private final boolean isMigratingZkBroker;
    private final List<Uuid> directories;

    private BrokerRegistration(
        int id,
        long epoch,
        Uuid incarnationId,
        Map<String, Endpoint> listeners,
        Map<String, VersionRange> supportedFeatures,
        Optional<String> rack,
        boolean fenced,
        boolean inControlledShutdown,
        boolean isMigratingZkBroker,
        List<Uuid> directories
    ) {
        this.id = id;
        this.epoch = epoch;
        this.incarnationId = incarnationId;
        Map<String, Endpoint> newListeners = new HashMap<>(listeners.size());
        for (Entry<String, Endpoint> entry : listeners.entrySet()) {
            if (!entry.getValue().listenerName().isPresent()) {
                throw new IllegalArgumentException("Broker listeners must be named.");
            }
            newListeners.put(entry.getKey(), entry.getValue());
        }
        this.listeners = Collections.unmodifiableMap(newListeners);
        Objects.requireNonNull(supportedFeatures);
        this.supportedFeatures = new HashMap<>(supportedFeatures);
        this.rack = rack;
        this.fenced = fenced;
        this.inControlledShutdown = inControlledShutdown;
        this.isMigratingZkBroker = isMigratingZkBroker;
        directories = new ArrayList<>(directories);
        directories.sort(Uuid::compareTo);
        this.directories = Collections.unmodifiableList(directories);
    }

    public static BrokerRegistration fromRecord(RegisterBrokerRecord record) {
        Map<String, Endpoint> listeners = new HashMap<>();
        for (BrokerEndpoint endpoint : record.endPoints()) {
            listeners.put(endpoint.name(), new Endpoint(endpoint.name(),
                SecurityProtocol.forId(endpoint.securityProtocol()),
                endpoint.host(),
                endpoint.port()));
        }
        Map<String, VersionRange> supportedFeatures = new HashMap<>();
        for (BrokerFeature feature : record.features()) {
            supportedFeatures.put(feature.name(), VersionRange.of(
                feature.minSupportedVersion(), feature.maxSupportedVersion()));
        }
        return new BrokerRegistration(record.brokerId(),
            record.brokerEpoch(),
            record.incarnationId(),
            listeners,
            supportedFeatures,
            Optional.ofNullable(record.rack()),
            record.fenced(),
            record.inControlledShutdown(),
            record.isMigratingZkBroker(),
            record.logDirs());
    }

    public int id() {
        return id;
    }

    public long epoch() {
        return epoch;
    }

    public Uuid incarnationId() {
        return incarnationId;
    }

    public Map<String, Endpoint> listeners() {
        return listeners;
    }

    public Optional<Node> node(String listenerName) {
        Endpoint endpoint = listeners().get(listenerName);
        if (endpoint == null) {
            return Optional.empty();
        }
        return Optional.of(new Node(id, endpoint.host(), endpoint.port(), rack.orElse(null)));
    }

    public Map<String, VersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    public Optional<String> rack() {
        return rack;
    }

    public boolean fenced() {
        return fenced;
    }

    public boolean inControlledShutdown() {
        return inControlledShutdown;
    }

    public boolean isMigratingZkBroker() {
        return isMigratingZkBroker;
    }

    public List<Uuid> directories() {
        return directories;
    }

    public boolean hasOnlineDir(Uuid dir) {
        return DirectoryId.isOnline(dir, directories);
    }

    public List<Uuid> directoryIntersection(List<Uuid> otherDirectories) {
        List<Uuid> results = new ArrayList<>();
        for (Uuid directory : directories) {
            if (otherDirectories.contains(directory)) {
                results.add(directory);
            }
        }
        return results;
    }

    public List<Uuid> directoryDifference(List<Uuid> otherDirectories) {
        List<Uuid> results = new ArrayList<>();
        for (Uuid directory : directories) {
            if (!otherDirectories.contains(directory)) {
                results.add(directory);
            }
        }
        return results;
    }

    public ApiMessageAndVersion toRecord(ImageWriterOptions options) {
        RegisterBrokerRecord registrationRecord = new RegisterBrokerRecord().
            setBrokerId(id).
            setRack(rack.orElse(null)).
            setBrokerEpoch(epoch).
            setIncarnationId(incarnationId).
            setFenced(fenced);

        if (inControlledShutdown) {
            if (options.metadataVersion().isInControlledShutdownStateSupported()) {
                registrationRecord.setInControlledShutdown(true);
            } else {
                options.handleLoss("the inControlledShutdown state of one or more brokers");
            }
        }

        if (isMigratingZkBroker) {
            if (options.metadataVersion().isMigrationSupported()) {
                registrationRecord.setIsMigratingZkBroker(isMigratingZkBroker);
            } else {
                options.handleLoss("the isMigratingZkBroker state of one or more brokers");
            }
        }

        if (directories.isEmpty() || options.metadataVersion().isDirectoryAssignmentSupported()) {
            registrationRecord.setLogDirs(directories);
        } else {
            options.handleLoss("the online log directories of one or more brokers");
        }

        for (Entry<String, Endpoint> entry : listeners.entrySet()) {
            Endpoint endpoint = entry.getValue();
            registrationRecord.endPoints().add(new BrokerEndpoint().
                setName(entry.getKey()).
                setHost(endpoint.host()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        }

        for (Entry<String, VersionRange> entry : supportedFeatures.entrySet()) {
            registrationRecord.features().add(new BrokerFeature().
                setName(entry.getKey()).
                setMinSupportedVersion(entry.getValue().min()).
                setMaxSupportedVersion(entry.getValue().max()));
        }

        return new ApiMessageAndVersion(registrationRecord,
            options.metadataVersion().registerBrokerRecordVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, epoch, incarnationId, listeners, supportedFeatures,
            rack, fenced, inControlledShutdown, isMigratingZkBroker, directories);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BrokerRegistration)) return false;
        BrokerRegistration other = (BrokerRegistration) o;
        return other.id == id &&
            other.epoch == epoch &&
            other.incarnationId.equals(incarnationId) &&
            other.listeners.equals(listeners) &&
            other.supportedFeatures.equals(supportedFeatures) &&
            other.rack.equals(rack) &&
            other.fenced == fenced &&
            other.inControlledShutdown == inControlledShutdown &&
            other.isMigratingZkBroker == isMigratingZkBroker &&
            other.directories.equals(directories);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("BrokerRegistration(id=").append(id);
        bld.append(", epoch=").append(epoch);
        bld.append(", incarnationId=").append(incarnationId);
        bld.append(", listeners=[").append(
            listeners.keySet().stream().sorted().
                map(n -> listeners.get(n).toString()).
                collect(Collectors.joining(", ")));
        bld.append("], supportedFeatures={").append(
            supportedFeatures.keySet().stream().sorted().
                map(k -> k + ": " + supportedFeatures.get(k)).
                collect(Collectors.joining(", ")));
        bld.append("}");
        bld.append(", rack=").append(rack);
        bld.append(", fenced=").append(fenced);
        bld.append(", inControlledShutdown=").append(inControlledShutdown);
        bld.append(", isMigratingZkBroker=").append(isMigratingZkBroker);
        bld.append(", directories=").append(directories);
        bld.append(")");
        return bld.toString();
    }

    public BrokerRegistration cloneWith(
        Optional<Boolean> fencingChange,
        Optional<Boolean> inControlledShutdownChange,
        Optional<List<Uuid>> directoriesChange
    ) {
        boolean newFenced = fencingChange.orElse(fenced);
        boolean newInControlledShutdownChange = inControlledShutdownChange.orElse(inControlledShutdown);
        List<Uuid> newDirectories = directoriesChange.orElse(directories);

        if (newFenced == fenced && newInControlledShutdownChange == inControlledShutdown && newDirectories.equals(directories))
            return this;

        return new BrokerRegistration(
            id,
            epoch,
            incarnationId,
            listeners,
            supportedFeatures,
            rack,
            newFenced,
            newInControlledShutdownChange,
            isMigratingZkBroker,
            newDirectories
        );
    }
}
