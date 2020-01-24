package org.apache.kafka.streams.processor.internals;

public abstract class AbstractTask implements Task {
    private Task.State state = Task.State.CREATED;

    @Override
    public final Task.State state() {
        return state;
    }

    final void transitionTo(final Task.State newState) {
        final State oldState = state();
        if (oldState == State.CREATED && (newState == State.RESTORING || newState == State.CLOSED)) {
        } else if (oldState == State.RESTORING && (newState == State.RUNNING || newState == State.SUSPENDED || newState == State.CLOSED)) {
        } else if (oldState == State.RUNNING && (newState == State.SUSPENDED || newState == State.CLOSED)) {
        } else if (oldState == State.SUSPENDED && (newState == State.RUNNING || newState == State.CLOSED)) {
        } else {
            throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
        }
        state = newState;
    }
}
