package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.List;
import java.util.Map;

public interface StateConstrainedBalancedAssignor {

    class ClientIdAndLag {
        private final String clientId;
        private final int lag;

        public ClientIdAndLag(final String clientId, final int lag) {
            this.clientId = clientId;
            this.lag = lag;
        }

        public String clientId() {
            return clientId;
        }

        public int lag() {
            return lag;
        }
    }

    Map<String, List<TaskId>> assign(final Map<TaskId, List<ClientIdAndLag>> statefulTasksToRankedClients,
                                     final int balanceFactor);
}
