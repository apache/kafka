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
package org.apache.kafka.raft.internals;

import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.Uuid;

public final class ReplicaKey {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicaKey that = (ReplicaKey) o;

        if (id != that.id) return false;
        if (!Objects.equals(directoryId, that.directoryId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, directoryId);
    }

    @Override
    public String toString() {
        return String.format("ReplicaKey(id=%d, directoryId=%s)", id, directoryId);
    }

    public static ReplicaKey of(int id, Optional<Uuid> directoryId) {
        return new ReplicaKey(id, directoryId);
    }
}
