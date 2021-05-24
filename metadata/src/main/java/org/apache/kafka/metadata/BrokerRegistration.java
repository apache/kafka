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
import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An immutable class which represents broker registrations.
 */
public class BrokerRegistration {
    private final int id;
    private final long epoch;
    private final Uuid incarnationId;
    private final Map<String, Endpoint> listeners;
    private final Map<String, VersionRange> supportedFeatures;
    private final Optional<String> rack;
    private final boolean fenced;

    public BrokerRegistration(int id,
                              long epoch,
                              Uuid incarnationId,
                              List<Endpoint> listeners,
                              Map<String, VersionRange> supportedFeatures,
                              Optional<String> rack,
                              boolean fenced) {
        this.id = id;
        this.epoch = epoch;
        this.incarnationId = incarnationId;
        Map<String, Endpoint> listenersMap = new HashMap<>();
        for (Endpoint endpoint : listeners) {
            listenersMap.put(endpoint.listenerName().get(), endpoint);
        }
        this.listeners = Collections.unmodifiableMap(listenersMap);
        Objects.requireNonNull(supportedFeatures);
        this.supportedFeatures = new HashMap<>(supportedFeatures);
        Objects.requireNonNull(rack);
        this.rack = rack;
        this.fenced = fenced;
    }

    public BrokerRegistration(int id,
                              long epoch,
                              Uuid incarnationId,
                              Map<String, Endpoint> listeners,
                              Map<String, VersionRange> supportedFeatures,
                              Optional<String> rack,
                              boolean fenced) {
        this.id = id;
        this.epoch = epoch;
        this.incarnationId = incarnationId;
        this.listeners = new HashMap<>(listeners);
        this.supportedFeatures = new HashMap<>(supportedFeatures);
        this.rack = rack;
        this.fenced = fenced;
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

    public Map<String, VersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    public Optional<String> rack() {
        return rack;
    }

    public boolean fenced() {
        return fenced;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, epoch, incarnationId, listeners, supportedFeatures,
            rack, fenced);
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
            other.fenced == fenced;
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
            supportedFeatures.entrySet().stream().sorted().
                map(e -> e.getKey() + ": " + e.getValue()).
                collect(Collectors.joining(", ")));
        bld.append("}");
        bld.append(", rack=").append(rack);
        bld.append(", fenced=").append(fenced);
        bld.append(")");
        return bld.toString();
    }

    public BrokerRegistration cloneWithFencing(boolean fencing) {
        return new BrokerRegistration(id, epoch, incarnationId, listeners,
            supportedFeatures, rack, fencing);
    }
}
