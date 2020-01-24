package org.apache.kafka.streams.processor.internals;

public abstract class AbstractTask implements Task {
    private Task.State state = Task.State.CREATED;

    @Override
    public final Task.State state() {
        return state;
    }

    final void transitionTo(final Task.State newState) {
        Task.State.validateTransition(state(), newState);
        state = newState;
    }
}
