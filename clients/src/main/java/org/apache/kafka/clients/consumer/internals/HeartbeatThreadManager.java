package org.apache.kafka.clients.consumer.internals;

// TODO document methods
public interface HeartbeatThreadManager {

    void pollHeartbeat(long now);

    void startHeartbeatThreadIfNeeded();

    void disableHeartbeatThread();

    void enableHeartbeatThread();

    void closeHeartbeatThread();

    RequestFuture<Void> sendHeartbeatRequest();
}
