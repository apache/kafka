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

import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.SortedMap;

/**
 * Immutable class containing the client profile from the RequestContext.
 * <p>
 * A client profile is made up of the tuple of ClientSoftwareName, ClientSoftwareVersion,
 * ClientInstanceId, and ClientMetadata. The client profile provides the ClientConfigPolicy
 * implementation with a detailed view of the client in use, allowing it to distinguish between
 * different client types (e.g., librdkafka 2.12.0 vs Apache Kafka Java 4.4.0 Producer).
 */
public final class ClientProfile {
    private final Uuid clientInstanceId;
    private final String clientSoftwareName;
    private final String clientSoftwareVersion;
    private final SortedMap<String, String> clientMetadata;

    /**
     * Creates a new ClientProfile.
     *
     * @param clientInstanceId   Unique identifier for this client instance (UUID v4)
     * @param clientSoftwareName The name of the client software (e.g., "apache-kafka-java")
     * @param clientSoftwareVersion The version of the client software (e.g., "3.8.0")
     * @param clientMetadata     Optional metadata as key-value pairs for additional client context
     */
    public ClientProfile(
        Uuid clientInstanceId,
        String clientSoftwareName,
        String clientSoftwareVersion,
        SortedMap<String, String> clientMetadata
    ) {
        this.clientInstanceId = clientInstanceId;
        this.clientSoftwareName = clientSoftwareName;
        this.clientSoftwareVersion = clientSoftwareVersion;
        this.clientMetadata = clientMetadata;
    }

    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    public String clientSoftwareName() {
        return clientSoftwareName;
    }

    public String clientSoftwareVersion() {
        return clientSoftwareVersion;
    }

    public SortedMap<String, String> clientMetadata() {
        return clientMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientProfile that = (ClientProfile) o;
        return Objects.equals(clientInstanceId, that.clientInstanceId) &&
               Objects.equals(clientSoftwareName, that.clientSoftwareName) &&
               Objects.equals(clientSoftwareVersion, that.clientSoftwareVersion) &&
               Objects.equals(clientMetadata, that.clientMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientInstanceId, clientSoftwareName, clientSoftwareVersion, clientMetadata);
    }

    @Override
    public String toString() {
        return "ClientProfile{" +
               "clientInstanceId=" + clientInstanceId +
               ", clientSoftwareName='" + clientSoftwareName + '\'' +
               ", clientSoftwareVersion='" + clientSoftwareVersion + '\'' +
               ", clientMetadata=" + clientMetadata +
               '}';
    }
}
