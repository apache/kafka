package org.apache.kafka.clients.consumer.internals;

import java.util.Optional;
import java.util.Set;

public interface StreamsGroupRebalanceCallbacks {

    Optional<Exception> onTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks);

    Optional<Exception> onTasksAssigned(final StreamsRebalanceData.Assignment assignment);

    Optional<Exception> onAllTasksLost();
}
