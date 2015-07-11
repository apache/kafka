/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the co-ordinator
 */
public final class Heartbeat {
    
    /* The number of heartbeats to attempt to complete per session timeout interval.
     * so, e.g., with a session timeout of 3 seconds we would attempt a heartbeat
     * once per second.
     */
    public final static int HEARTBEATS_PER_SESSION_INTERVAL = 3;

    private final long timeout;
    private long lastHeartbeatSend;

    public Heartbeat(long timeout, long now) {
        this.timeout = timeout;
        this.lastHeartbeatSend = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
    }

    public boolean shouldHeartbeat(long now) {
        return now - lastHeartbeatSend > (1.0 / HEARTBEATS_PER_SESSION_INTERVAL) * this.timeout;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    public long timeToNextHeartbeat(long now) {
        long timeSinceLastHeartbeat = now - lastHeartbeatSend;

        long hbInterval = timeout / HEARTBEATS_PER_SESSION_INTERVAL;
        if (timeSinceLastHeartbeat > hbInterval)
            return 0;
        else
            return hbInterval - timeSinceLastHeartbeat;
    }
}