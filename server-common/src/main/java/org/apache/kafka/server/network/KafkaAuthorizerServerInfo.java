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

package org.apache.kafka.server.network;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.util.DeferredValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;


/**
 * Runtime broker configuration metadata provided to authorizers during start up.
 */
public final class KafkaAuthorizerServerInfo implements AuthorizerServerInfo {
    private final DeferredValue<String> clusterId;
    private final int brokerId;
    private final Collection<Endpoint> endpoints;
    private final Endpoint interbrokerEndpoint;
    private final Collection<String> earlyStartListeners;

    public KafkaAuthorizerServerInfo(
        DeferredValue<String> clusterId,
        int brokerId,
        Collection<Endpoint> endpoints,
        Endpoint interbrokerEndpoint,
        Collection<String> earlyStartListeners
    ) {
        this.clusterId = clusterId;
        this.brokerId = brokerId;
        this.endpoints = Collections.unmodifiableCollection(new ArrayList<>(endpoints));
        this.interbrokerEndpoint = interbrokerEndpoint;
        this.earlyStartListeners = Collections.unmodifiableCollection(new ArrayList<>(earlyStartListeners));
    }

    @Override
    public ClusterResource clusterResource() {
        return new ClusterResource(clusterId.getNow());
    }

    @Override
    public int brokerId() {
        return brokerId;
    }

    @Override
    public Collection<Endpoint> endpoints() {
        return endpoints;
    }

    @Override
    public Endpoint interBrokerEndpoint() {
        return interbrokerEndpoint;
    }

    @Override
    public Collection<String> earlyStartListeners() {
        return earlyStartListeners;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o.getClass().equals(KafkaAuthorizerServerInfo.class)))) return false;
        KafkaAuthorizerServerInfo other = (KafkaAuthorizerServerInfo) o;
        return clusterId.equals(other.clusterId) &&
                brokerId == other.brokerId &&
                endpoints.equals(other.endpoints) &&
                interbrokerEndpoint.equals(other.interbrokerEndpoint) &&
                earlyStartListeners.equals(other.earlyStartListeners);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId,
                brokerId,
                endpoints,
                interbrokerEndpoint,
                earlyStartListeners);
    }

    @Override
    public String toString() {
        return "KafkaAuthorizerServerInfo(" +
                "clusterResource=" + clusterId +
                ", brokerId=" + brokerId +
                ", endpoints=" + endpoints +
                ", earlyStartListeners=" + earlyStartListeners +
                ")";
    }
}
