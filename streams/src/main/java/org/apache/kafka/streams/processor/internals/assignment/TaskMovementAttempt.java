package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

class TaskMovementAttempt {
    private final TaskId taskId;
    private final ClientState sourceClient;
    private final ClientState destinationClient;

    TaskMovementAttempt(final TaskId taskId,
                        final ClientState sourceClient,
                        final ClientState destinationClient) {
        this.taskId = taskId;
        this.sourceClient = sourceClient;
        this.destinationClient = destinationClient;
    }

    public TaskId taskId() {
        return taskId;
    }

    public ClientState sourceClient() {
        return sourceClient;
    }

    public ClientState destinationClient() {
        return destinationClient;
    }
}
