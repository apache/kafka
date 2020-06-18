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
package org.apache.kafka.clients.consumer.internals;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ibm.disni.verbs.IbvMr;

import org.apache.kafka.clients.RdmaClient;
import org.apache.kafka.clients.RDMARequestCompletionHandler;
import org.apache.kafka.clients.RDMAWrBuilder;
import org.apache.kafka.clients.ClientRDMARequest;
import org.apache.kafka.clients.ClientRDMAResponse;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

public class ConsumerRDMAClient   {
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    private final RdmaClient rdmaClient;
    private final String clientId;
    private final Time time;

    private final int maxPollTimeoutMs;


    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();


    public boolean isConnected(Node node) {
        return rdmaClient.isConnected(node);
    }


    public void tryConnect(int id, String hostname, int port) {
        Node node = new Node(id, hostname, port);
        try {
            rdmaClient.connect(node);
        } catch (Exception e) {
            System.out.println("Uncaught exception during tryConnect");
        }
    }

    public ConsumerRDMAClient(RdmaClient rdmaClient, String clientId, Time time) {
        this.rdmaClient = rdmaClient;
        this.clientId = clientId;
        this.time = time;
        this.maxPollTimeoutMs = MAX_POLL_TIMEOUT_MS;
    }

    public ConsumerRDMAClient(RdmaClient rdmaClient, String clientId, Time time, int maxPollTimeoutMs) {
        this.rdmaClient = rdmaClient;
        this.clientId = clientId;
        this.time = time;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
    }

    public IbvMr MemReg(ByteBuffer buf) {
        IbvMr mr = null;
        try {
            mr = rdmaClient.MemReg(buf);
        } catch (Exception e) {
            System.out.println("Uncaught exception");
        }
        return mr;
    }



    public RequestFuture<ClientRDMAResponse> send(String nodeid, RDMAWrBuilder requestBuilder) {
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        long now = time.nanoseconds();
        ClientRDMARequest clientRequest = rdmaClient.newClientRdmaRequest(nodeid, requestBuilder, now, 10000, true, false, completionHandler);
        rdmaClient.send(clientRequest, time.milliseconds());

        return completionHandler.future;
    }


    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }


    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    public void poll(Timer timer) {
        poll(timer, null);
    }

    public void poll(Timer timer, ConsumerNetworkClient.PollCondition pollCondition) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests();

        if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
            // if there are no requests in flight, do not block longer than the retry backoff
            long pollTimeout = Math.min(timer.remainingMs(), maxPollTimeoutMs);
          //  if (rdmaClient.inFlightRequestCount() == 0)
         //       pollTimeout = Math.min(pollTimeout, retryBackoffMs);
            rdmaClient.poll(pollTimeout, time.nanoseconds());
        } else {
            rdmaClient.poll(0, time.nanoseconds());
        }

        timer.update();

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests();
    }

    private void firePendingCompletedRequests() {
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;
            completionHandler.fireCompletion();
        }
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null);
    }

    public boolean hasPendingRequests() {
        return rdmaClient.hasInFlightRequests();
    }


    private class RequestFutureCompletionHandler implements RDMARequestCompletionHandler  {
        private final RequestFuture<ClientRDMAResponse> future;
        private ClientRDMAResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        public void onComplete(ClientRDMAResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

}
