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

import java.util.concurrent.atomic.AtomicReference;

public class MockMetaLogManagerListener implements MetaLogManager.Listener {
    private final AtomicReference<String> firstError;
    private final CommitHandler commitHandler;
    private final int nodeId;
    private boolean shutdown = false;
    private long curEpoch = -1;

    interface CommitHandler {
        void handle(int nodeId, long epoch, long index, ApiMessage message) throws Exception;
    }

    MockMetaLogManagerListener(AtomicReference<String> firstError,
                               CommitHandler commitHandler,
                               int nodeId) {
        this.firstError = firstError;
        this.commitHandler = commitHandler;
        this.nodeId = nodeId;
    }

    @Override
    public void handleCommit(long epoch, long index, ApiMessage message) {
        try {
            commitHandler.handle(nodeId, epoch, index, message);
        } catch (Exception e) {
            firstError.compareAndSet(null, "error handling commit(epoch=" + epoch +
                ", index=" + index + ", message=" + message + "): " +
                e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @Override
    public synchronized void handleClaim(long epoch) {
        if (epoch < 0) {
            firstError.compareAndSet(null, nodeId + " invalid negative epoch in claim(" +
                epoch + ")");
            return;
        }
        if (shutdown) {
            firstError.compareAndSet(null, nodeId + " invoked claim(" + epoch + ") after " +
                "shutdown.");
            return;
        }
        if (curEpoch != -1L) {
            firstError.compareAndSet(null, nodeId + " invoked claim(" + epoch + ") before " +
                "renouncing epoch " + curEpoch);
            return;
        }
        curEpoch = epoch;
    }

    @Override
    public synchronized void handleRenounce(long epoch) {
        if (curEpoch != epoch) {
            firstError.compareAndSet(null, nodeId + " invoked renounce(" + epoch + ") " +
                "but the current epoch is " + curEpoch);
            return;
        }
        curEpoch = -1;
    }

    @Override
    public synchronized void beginShutdown() {
        this.shutdown = true;
    }

    public int nodeId() {
        return nodeId;
    }

    public synchronized boolean isShutdown() {
        return shutdown;
    }

    public synchronized long curEpoch() {
        return curEpoch;
    }
}
