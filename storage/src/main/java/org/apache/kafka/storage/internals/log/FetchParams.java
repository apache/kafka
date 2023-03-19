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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.replica.ClientMetadata;
import org.apache.kafka.common.requests.FetchRequest;

import java.util.Objects;
import java.util.Optional;

public class FetchParams {
    public final short requestVersion;
    public final int replicaId;
    public final long replicaEpoch;
    public final long maxWaitMs;
    public final int minBytes;
    public final int maxBytes;
    public final FetchIsolation isolation;
    public final Optional<ClientMetadata> clientMetadata;

    public FetchParams(short requestVersion,
                       int replicaId,
                       long replicaEpoch,
                       long maxWaitMs,
                       int minBytes,
                       int maxBytes,
                       FetchIsolation isolation,
                       Optional<ClientMetadata> clientMetadata) {
        Objects.requireNonNull(isolation);
        Objects.requireNonNull(clientMetadata);
        this.requestVersion = requestVersion;
        this.replicaId = replicaId;
        this.replicaEpoch = replicaEpoch;
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.isolation = isolation;
        this.clientMetadata = clientMetadata;
    }

    public boolean isFromFollower() {
        return FetchRequest.isValidBrokerId(replicaId);
    }

    public boolean isFromConsumer() {
        return FetchRequest.isConsumer(replicaId);
    }

    public boolean fetchOnlyLeader() {
        return isFromFollower() || (isFromConsumer() && !clientMetadata.isPresent());
    }

    public boolean hardMaxBytesLimit() {
        return requestVersion <= 2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchParams that = (FetchParams) o;
        return requestVersion == that.requestVersion
                && replicaId == that.replicaId
                && replicaEpoch == that.replicaEpoch
                && maxWaitMs == that.maxWaitMs
                && minBytes == that.minBytes
                && maxBytes == that.maxBytes
                && isolation.equals(that.isolation)
                && clientMetadata.equals(that.clientMetadata);
    }

    @Override
    public int hashCode() {
        int result = requestVersion;
        result = 31 * result + replicaId;
        result = 31 * result + (int) replicaEpoch;
        result = 31 * result + Long.hashCode(32);
        result = 31 * result + minBytes;
        result = 31 * result + maxBytes;
        result = 31 * result + isolation.hashCode();
        result = 31 * result + clientMetadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FetchParams(" +
                "requestVersion=" + requestVersion +
                ", replicaId=" + replicaId +
                ", replicaEpoch=" + replicaEpoch +
                ", maxWaitMs=" + maxWaitMs +
                ", minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ", isolation=" + isolation +
                ", clientMetadata=" + clientMetadata +
                ')';
    }
}
