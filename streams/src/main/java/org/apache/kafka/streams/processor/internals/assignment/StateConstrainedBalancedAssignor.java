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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

public interface StateConstrainedBalancedAssignor<ID extends Comparable<? super ID>> {

    class ClientIdAndRank<ID extends Comparable<? super ID>> implements Comparable<ClientIdAndRank<ID>> {
        private final ID clientId;
        private final long rank;

        public ClientIdAndRank(final ID clientId, final long rank) {
            this.clientId = clientId;
            this.rank = rank;
        }

        public static <ID extends Comparable<? super ID>> ClientIdAndRank<ID> make(final ID clientId, final long rank) {
            return new ClientIdAndRank<>(clientId, rank);
        }

        public ID clientId() {
            return clientId;
        }

        public long rank() {
            return rank;
        }

        @Override
        public int compareTo(final ClientIdAndRank<ID> clientIdAndRank) {
            if (rank < clientIdAndRank.rank) {
                return -1;
            } else if (rank > clientIdAndRank.rank) {
                return 1;
            } else {
                return clientId.compareTo(clientIdAndRank.clientId);
            }
        }
    }

    Map<ID, List<TaskId>> assign(final SortedMap<TaskId, SortedSet<ClientIdAndRank<ID>>> statefulTasksToRankedClients,
                                 final int balanceFactor,
                                 final Set<ID> clients,
                                 final Map<ID, Integer> clientsToNumberOfStreamThreads);
}
