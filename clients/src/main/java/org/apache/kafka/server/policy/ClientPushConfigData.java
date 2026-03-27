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
package org.apache.kafka.server.policy;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable class containing the PushConfig API data.
 * <p>
 * The client profile and configuration values come directly from the RPC.
 * The broker supplies its current timestamp (UTC) for when the request was received.
 */
public final class ClientPushConfigData {
    private final ClientProfile clientProfile;
    private final Map<String, String> configs;
    private final long timestamp;

    /**
     * Creates a new ClientPushConfigData.
     *
     * @param clientProfile The client profile information from the request context
     * @param configs       The configuration key-value pairs pushed by the client
     * @param timestamp     UTC timestamp (milliseconds) when the broker received the request
     */
    public ClientPushConfigData(
        ClientProfile clientProfile,
        Map<String, String> configs,
        long timestamp
    ) {
        this.clientProfile = clientProfile;
        this.configs = configs;
        this.timestamp = timestamp;
    }

    public ClientProfile clientProfile() {
        return clientProfile;
    }

    public Map<String, String> configs() {
        return configs;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientPushConfigData that = (ClientPushConfigData) o;
        return timestamp == that.timestamp &&
               Objects.equals(clientProfile, that.clientProfile) &&
               Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientProfile, configs, timestamp);
    }

    @Override
    public String toString() {
        return "ClientPushConfigData{" +
               "clientProfile=" + clientProfile +
               ", configs=" + configs +
               ", timestamp=" + timestamp +
               '}';
    }
}
