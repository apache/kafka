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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable collection of connectors and tasks. Can represent the complete set of connectors and tasks
 * assigned to a worker, the set of to-be-revoked connectors and tasks for a worker, the total set of connectors
 * and tasks configured to run on the cluster, etc.
 */
public class ConnectorsAndTasks {

    public static final ConnectorsAndTasks EMPTY =
            new ConnectorsAndTasks(Collections.emptySet(), Collections.emptySet());

    private final Set<String> connectors;
    private final Set<ConnectorTaskId> tasks;

    private ConnectorsAndTasks(Set<String> connectors, Set<ConnectorTaskId> tasks) {
        Objects.requireNonNull(connectors);
        Objects.requireNonNull(tasks);
        this.connectors = Collections.unmodifiableSet(connectors);
        this.tasks = Collections.unmodifiableSet(tasks);
    }

    public static class Builder {
        private final Set<String> connectors;
        private final Set<ConnectorTaskId> tasks;

        private Builder() {
            this.connectors = new LinkedHashSet<>();
            this.tasks = new LinkedHashSet<>();
        }

        public Builder addConnectors(Collection<String> connectors) {
            this.connectors.addAll(connectors);
            return this;
        }

        public Builder addTasks(Collection<ConnectorTaskId> tasks) {
            this.tasks.addAll(tasks);
            return this;
        }

        public Builder addAll(ConnectorsAndTasks connectorsAndTasks) {
            addConnectors(connectorsAndTasks.connectors());
            addTasks(connectorsAndTasks.tasks());
            return this;
        }

        public Builder removeConnectors(Collection<String> connectors) {
            this.connectors.removeAll(connectors);
            return this;
        }

        public Builder removeTasks(Collection<ConnectorTaskId> tasks) {
            this.tasks.removeAll(tasks);
            return this;
        }

        public Builder removeAll(ConnectorsAndTasks connectorsAndTasks) {
            removeConnectors(connectorsAndTasks.connectors());
            removeTasks(connectorsAndTasks.tasks());
            return this;
        }

        public ConnectorsAndTasks build() {
            return new ConnectorsAndTasks(connectors, tasks);
        }

    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
        return builder()
                .addConnectors(connectors)
                .addTasks(tasks);
    }

    public static ConnectorsAndTasks of(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
        return builder(connectors, tasks).build();
    }

    public static ConnectorsAndTasks of(ConnectProtocol.Assignment assignment) {
        return of(assignment.connectors(), assignment.tasks());
    }

    public static ConnectorsAndTasks of(WorkerCoordinator.WorkerLoad workerLoad) {
        return of(workerLoad.connectors(), workerLoad.tasks());
    }

    public static ConnectorsAndTasks copy(ConnectorsAndTasks connectorsAndTasks) {
        return of(connectorsAndTasks.connectors(), connectorsAndTasks.tasks());
    }

    public static ConnectorsAndTasks combine(ConnectorsAndTasks... connectorsAndTasksList) {
        return combine(Arrays.asList(connectorsAndTasksList));
    }

    public static ConnectorsAndTasks combine(Collection<ConnectorsAndTasks> connectorsAndTasksCollection) {
        Builder result = builder();
        for (ConnectorsAndTasks connectorsAndTasks : connectorsAndTasksCollection) {
            result.addAll(connectorsAndTasks);
        }
        return result.build();
    }

    public static ConnectorsAndTasks intersection(ConnectorsAndTasks first, ConnectorsAndTasks second) {
        Set<String> connectors = Utils.intersection(LinkedHashSet::new, first.connectors(), second.connectors());
        Set<ConnectorTaskId> tasks = Utils.intersection(LinkedHashSet::new, first.tasks(), second.tasks());
        return of(connectors, tasks);
    }

    public static ConnectorsAndTasks diff(ConnectorsAndTasks base, ConnectorsAndTasks... toSubtract) {
        Builder result = base.toBuilder();
        for (ConnectorsAndTasks subtracted : toSubtract) {
            result.removeAll(subtracted);
        }
        return result.build();
    }

    public Builder toBuilder() {
        return builder(connectors, tasks);
    }

    public Set<String> connectors() {
        return connectors;
    }

    public Set<ConnectorTaskId> tasks() {
        return tasks;
    }

    public boolean isEmpty() {
        return connectors.isEmpty() && tasks.isEmpty();
    }

    @Override
    public String toString() {
        return "{ connectorIds=" + connectors + ", taskIds=" + tasks + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ConnectorsAndTasks))
            return false;
        ConnectorsAndTasks that = (ConnectorsAndTasks) o;
        return connectors.equals(that.connectors) && tasks.equals(that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectors, tasks);
    }
}
