package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public interface StateConstrainedBalancedAssignor<ID> {

    class ClientIdAndLag<ID> {
        private final ID clientId;
        private final long lag;

        public ClientIdAndLag(final ID clientId, final long lag) {
            this.clientId = clientId;
            this.lag = lag;
        }

        public ID clientId() {
            return clientId;
        }

        public long lag() {
            return lag;
        }
    }

    Map<ID, List<TaskId>> assign(final Map<TaskId, SortedSet<ClientIdAndLag<ID>>> statefulTasksToRankedClients,
                                 final int balanceFactor);
}
