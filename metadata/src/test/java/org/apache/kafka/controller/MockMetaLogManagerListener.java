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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.ApiMessage;

import java.util.ArrayList;
import java.util.List;

public class MockMetaLogManagerListener implements MetaLogManager.Listener {
    public static final String COMMIT = "COMMIT";
    public static final String LAST_COMMITTED_OFFSET = "LAST_COMMITTED_OFFSET";
    public static final String CLAIM = "CLAIM";
    public static final String RENOUNCE = "RENOUNCE";
    public static final String SHUTDOWN = "SHUTDOWN";

    private final List<String> serializedEvents = new ArrayList<>();
    private long currentClaimEpoch = -1;

    @Override
    public synchronized void handleCommits(long lastCommittedOffset, List<ApiMessage> messages) {
        for (ApiMessage message : messages) {
            StringBuilder bld = new StringBuilder();
            bld.append(COMMIT).append(" ").append(message.toString());
            serializedEvents.add(bld.toString());
        }
        StringBuilder bld = new StringBuilder();
        bld.append(LAST_COMMITTED_OFFSET).append(" ").append(lastCommittedOffset);
        serializedEvents.add(bld.toString());
    }

    @Override
    public void handleClaim(long epoch) {
        StringBuilder bld = new StringBuilder();
        bld.append(CLAIM).append(" ").append(epoch);
        synchronized (this) {
            serializedEvents.add(bld.toString());
            currentClaimEpoch = epoch;
        }
    }

    @Override
    public void handleRenounce(long epoch) {
        StringBuilder bld = new StringBuilder();
        bld.append(RENOUNCE).append(" ").append(epoch);
        synchronized (this) {
            serializedEvents.add(bld.toString());
            currentClaimEpoch = -1;
        }
    }

    @Override
    public void beginShutdown() {
        StringBuilder bld = new StringBuilder();
        bld.append(SHUTDOWN);
        synchronized (this) {
            serializedEvents.add(bld.toString());
        }
    }

    @Override
    public synchronized long currentClaimEpoch() {
        return currentClaimEpoch;
    }

    public synchronized List<String> serializedEvents() {
        return new ArrayList<>(serializedEvents);
    }
}
