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
        HeartbeatRequestState heartbeatRequestState;

        @Override
        public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
            NetworkClientDelegate.PollResult req = trySendHeartbeat(currentTimeMs);
        }

        private NetworkClientDelegate.PollResult trySendHeartbeat(long currentTimeMs) {
            if (groupState.state != GroupState.State.UNJOINED && heartbeatRequestState.canSendHeartbeat(currentTimeMs)) {
                return sendHeartbeat(currentTimeMs);
            }
        }

        private NetworkClientDelegate.PollResult sendHeartbeat(long currentTimeMs) {
            // construct a heartbeat
            return null;
        }

        class HeartbeatRequestState extends RequestState  {

           public HeartbeatRequestState(long retryBackoffMs) {
               super(retryBackoffMs);
           }

           public boolean canSendHeartbeat(long currentTimeMs) {
               // of course, this is demo
               return true;
           }
       }
    }

    static class NeoConsumerProtocol implements RebalanceProtocol {
        private Mode mode; // get it from the config
        enum Mode {
            CLIENT_SIDE, SERVER_SIDE
        }

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
