package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;

public class ProtocolRequestManager implements RequestManager {
    GroupState groupState;
    HeartbeatRequestManager heartbeatRequestManager;
    RebalanceProtocol protocol;
    BlockingQueue<ApplicationEvent> eventQueue;
    Optional<Long> callbackInvokedMs;

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        switch (groupState.state) {
            case UNJOINED:
                return null;
            case PREPARE:
                return protocol.onPrepare(currentTimeMs);
            case ASSIGNING:
                invokeRebalance(currentTimeMs);
                return protocol.onAssign(currentTimeMs);
            case COMPLETE:
                return protocol.onComplete(currentTimeMs);
            case STABLE:
                return protocol.onStable(currentTimeMs);
        }
        return null;
    }

    public void ackCallbackInvocation(long currentTimeMs) {
        callbackInvokedMs = Optional.empty();
        groupState.state = GroupState.State.COMPLETE;
    }

    private void invokeRebalance(long currentTimeMs) {
        if (callbackInvokedMs.isPresent()) {
            // do nothing as we've invoked callback
            return;
        }
        callbackInvokedMs = Optional.of(currentTimeMs);
        eventQueue.add(new RebalanceCallbackEvent());
    }

    class RebalanceCallbackEvent extends ApplicationEvent {
        protected RebalanceCallbackEvent() {
            super(null); // it should actually accept a type.REBALANCE_CALLBACK_TRIGGER but i'm too lazy to create one
        }
    }

    class HeartbeatRequestManager implements RequestManager {
        RequestState heartbeatRequestState;

        @Override
        public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
            // just a demo, don't do anything
            return null;
        }

        public void sendHeartbeat(long currentTimeMs) {
            heartbeatRequestState.canSendRequest(currentTimeMs);
        }
    }

    class ServierSideProtocol implements RebalanceProtocol {

        @Override
        public NetworkClientDelegate.PollResult onPrepare(long currentTimeMs) {
            return null;
        }

        @Override
        public NetworkClientDelegate.PollResult onAssign(long currentTimeMs) {
            return null;
        }

        @Override
        public NetworkClientDelegate.PollResult onComplete(long currentTimeMs) {
            return null;
        }

        @Override
        public NetworkClientDelegate.PollResult onStable(long currentTimeMs) {
            return null;
        }
    }
    interface RebalanceProtocol {
        NetworkClientDelegate.PollResult onPrepare(long currentTimeMs);
        NetworkClientDelegate.PollResult onAssign(long currentTimeMs);
        NetworkClientDelegate.PollResult onComplete(long currentTimeMs);
        NetworkClientDelegate.PollResult onStable(long currentTimeMs);

    }
}
