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
import java.util.SortedMap;
import java.util.SortedSet;

public interface StateConstrainedBalancedAssignor<ID extends Comparable<? super ID>> {

    class ClientIdAndLag<ID extends Comparable<? super ID>> implements Comparable<ClientIdAndLag<ID>> {
        private final ID clientId;
        private final long lag;

        public ClientIdAndLag(final ID clientId, final long lag) {
            this.clientId = clientId;
            this.lag = lag;
        }

        public static <ID extends Comparable<? super ID>> ClientIdAndLag<ID> make(final ID clientId, final long lag) {
            return new ClientIdAndLag<>(clientId, lag);
        }

        public ID clientId() {
            return clientId;
        }

        public long lag() {
            return lag;
        }

        @Override
        public int compareTo(final ClientIdAndLag<ID> clientIdAndLag) {
            if (lag < clientIdAndLag.lag) {
                return -1;
            } else if (lag > clientIdAndLag.lag) {
                return 1;
            } else {
                return clientId.compareTo(clientIdAndLag.clientId);
            }
        }
    }

    Map<ID, List<TaskId>> assign(final SortedMap<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients,
                                 final int balanceFactor);
}
