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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.utils.FetchRequestUtils;

import java.util.Objects;
import java.util.Optional;

public class FetchParams {
    private final short requestVersion;
    private final int replicaId;
    private final long maxWaitMs;
    private final int minBytes;
    private final int maxBytes;
    private final FetchIsolation isolation;
    private Optional<ClientMetadata> clientMetadata;

    public FetchParams(short requestVersion,
                       int replicaId,
                       long maxWaitMs,
                       int minBytes,
                       int maxBytes,
                       FetchIsolation isolation,
                       Optional<ClientMetadata> clientMetadata) {
        this.requestVersion = requestVersion;
        this.replicaId = replicaId;
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.isolation = isolation;
        this.clientMetadata = clientMetadata;
    }

    public boolean isFromFollower() {
        return FetchRequestUtils.isValidBrokerId(replicaId);
    }

    public boolean isFromConsumer() {
        return FetchRequestUtils.isConsumer(replicaId);
    }

    public boolean fetchOnlyLeader() {
        return isFromFollower() || (isFromConsumer() && !clientMetadata.isPresent());
    }

    public boolean hardMaxBytesLimit() {
        return requestVersion <= 2;
    }

    public short requestVersion() {
        return requestVersion;
    }

    public int replicaId() {
        return replicaId;
    }

    public long maxWaitMs() {
        return maxWaitMs;
    }

    public int minBytes() {
        return minBytes;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public FetchIsolation isolation() {
        return isolation;
    }

    public Optional<ClientMetadata> clientMetadata() {
        return clientMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchParams that = (FetchParams) o;
        return requestVersion == that.requestVersion
                && replicaId == that.replicaId
                && maxWaitMs == that.maxWaitMs
                && minBytes == that.minBytes
                && maxBytes == that.maxBytes
                && Objects.equals(isolation.getClass(), that.isolation.getClass())
                && Objects.equals(clientMetadata, that.clientMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestVersion, replicaId, maxWaitMs, minBytes, maxBytes, isolation, clientMetadata);
    }

    @Override
    public String toString() {
        return "FetchParams{" +
                "requestVersion=" + requestVersion +
                ", replicaId=" + replicaId +
                ", maxWaitMs=" + maxWaitMs +
                ", minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ", isolation=" + isolation +
                ", clientMetadata=" + clientMetadata +
                '}';
    }
}
