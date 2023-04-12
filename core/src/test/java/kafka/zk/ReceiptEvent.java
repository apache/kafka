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
package kafka.zk;

import org.apache.zookeeper.server.MutedServerCxn;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Event corresponding to the invocation of the Zookeeper request processor for a request.
 */
public class ReceiptEvent {
    private final int opCode;
    private final Lock lock = new ReentrantLock();
    private final Condition received = lock.newCondition();
    private final Condition processed = lock.newCondition();
    private State state = State.notReceived;
    private boolean sendResponse;

    public ReceiptEvent(int opCode, boolean sendResponse) {
        this.opCode = opCode;
        this.sendResponse = sendResponse;
    }

    public boolean matches(Request request) {
        return request.type == opCode;
    }

    public void serverReceived() {
        lock.lock();
        try {
            state = Collections.max(Arrays.asList(state, State.received));
            received.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public void serverProcessed() {
        lock.lock();
        try {
            state = Collections.max(Arrays.asList(state, State.processed));
            processed.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public Request maybeDecorate(Request request) {
        if (sendResponse) {
            return request;
        }
        ServerCnxn unresponsiveCxn = new MutedServerCxn(request.cnxn);
        return new DelegatingRequest(unresponsiveCxn, request);
    }

    public void awaitProcessed() throws InterruptedException {
        lock.lock();
        try {
            while (state.compareTo(State.processed) < 0) {
                processed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "Received: " + Request.op2String(opCode);
    }

    enum State {
        notReceived,
        received,
        processed;
    }
}
