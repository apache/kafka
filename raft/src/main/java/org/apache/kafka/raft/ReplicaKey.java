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
package org.apache.kafka.raft;

import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.Optional;

public final class ReplicaKey implements Comparable<ReplicaKey> {
    public static final Uuid NO_DIRECTORY_ID = Uuid.ZERO_UUID;

    private final int id;
    private final Optional<Uuid> directoryId;

    private ReplicaKey(int id, Optional<Uuid> directoryId) {
        this.id = id;
        this.directoryId = directoryId;
    }

    public int id() {
        return id;
    }

    public Optional<Uuid> directoryId() {
        return directoryId;
    }

    @Override
    public int compareTo(ReplicaKey that) {
        int idComparison = Integer.compare(this.id, that.id);
        if (idComparison == 0) {
            return directoryId
                .orElse(NO_DIRECTORY_ID)
                .compareTo(that.directoryId.orElse(NO_DIRECTORY_ID));
        } else {
            return idComparison;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicaKey that = (ReplicaKey) o;

        if (id != that.id) return false;
        return Objects.equals(directoryId, that.directoryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, directoryId);
    }

    @Override
    public String toString() {
        return String.format("ReplicaKey(id=%d, directoryId=%s)", id, directoryId.map(Uuid::toString).orElse("<undefined>"));
    }

    public static ReplicaKey of(int id, Uuid directoryId) {
        return new ReplicaKey(
            id,
            directoryId.equals(NO_DIRECTORY_ID) ? Optional.empty() : Optional.of(directoryId)
        );
    }
}
