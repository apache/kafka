package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the co-ordinator
 */
public final class Heartbeat {
    
    /* The number of heartbeats to attempt to complete per session timeout interval.
     * so, e.g., with a session timeout of 3 seconds we would attempt a heartbeat
     * once per second.
     */
    private final static int HEARTBEATS_PER_SESSION_INTERVAL = 3;

    private final long timeout;
    private long lastHeartbeatSend;
    private long lastHeartbeatResponse;

    public Heartbeat(long timeout, long now) {
        this.timeout = timeout;
        this.lastHeartbeatSend = now;
        this.lastHeartbeatResponse = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
    }

    public void receivedResponse(long now) {
        this.lastHeartbeatResponse = now;
    }

    public void markDead() {
        this.lastHeartbeatResponse = -1;
    }

    public boolean isAlive(long now) {
        return now - lastHeartbeatResponse <= timeout;
    }

    public boolean shouldHeartbeat(long now) {
        return now - lastHeartbeatSend > (1.0 / HEARTBEATS_PER_SESSION_INTERVAL) * this.timeout;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }
}