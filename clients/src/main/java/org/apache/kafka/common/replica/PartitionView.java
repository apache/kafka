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
package org.apache.kafka.common.replica;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * View of a partition used by {@link ReplicaSelector} to determine a preferred replica.
 */
public interface PartitionView {
    Set<ReplicaView> replicas();

    ReplicaView leader();

    class DefaultPartitionView implements PartitionView {
        private final Set<ReplicaView> replicas;
        private final ReplicaView leader;

        public DefaultPartitionView(Set<ReplicaView> replicas, ReplicaView leader) {
            this.replicas = Collections.unmodifiableSet(replicas);
            this.leader = leader;
        }

        @Override
        public Set<ReplicaView> replicas() {
            return replicas;
        }

        @Override
        public ReplicaView leader() {
            return leader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DefaultPartitionView that = (DefaultPartitionView) o;
            return Objects.equals(replicas, that.replicas) &&
                    Objects.equals(leader, that.leader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, leader);
        }

        @Override
        public String toString() {
            return "DefaultPartitionView{" +
                    "replicas=" + replicas +
                    ", leader=" + leader +
                    '}';
        }
    }
}
