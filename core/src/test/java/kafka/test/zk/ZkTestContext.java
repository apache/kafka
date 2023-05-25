/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.test.zk;

import java.util.Iterator;

/**
 * Represents the expected timeline of receive-request, connections and session expiration events
 * in a test with simulated failures.
 */
public class ZkTestContext {
    public final Iterator<ReceiptEvent> requestTimelime;
    public final Iterator<ConnectionEvent> connectionTimeline;
    public final Iterator<ExpirationEvent> sessionExpirationTimeline;
    private boolean terminated;

    public ZkTestContext(
        Iterator<ReceiptEvent> requestTimelime,
        Iterator<ConnectionEvent> connectionTimeline,
        Iterator<ExpirationEvent> sessionExpirationTimeline
    ) {
        this.requestTimelime = requestTimelime;
        this.connectionTimeline = connectionTimeline;
        this.sessionExpirationTimeline = sessionExpirationTimeline;
    }

    public synchronized boolean isTerminated() {
        return terminated;
    }

    public synchronized void terminate() {
        terminated = true;
    }
}
