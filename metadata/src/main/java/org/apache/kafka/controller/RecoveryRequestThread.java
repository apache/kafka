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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

class RecoveryRequestThread extends InterBrokerSendThread implements RecoveryFetcher.Sender {
    static class RecoveryCompletionHandler implements RequestCompletionHandler {
        final RecoveryFetcher.Receiver receiver;
        final RecoveryFetcher.Request previous;

        RecoveryCompletionHandler(RecoveryFetcher.Receiver receiver, RecoveryFetcher.Request previous) {
            this.receiver = receiver;
            this.previous = previous;
        }

        @Override
        public void onComplete(ClientResponse response) {
            receiver.receive(RecoveryFetcher.Result.fromResponse(response, previous));
        }
    }

    static class RequestAndReceiver {
        final RecoveryFetcher.Receiver receiver;
        final RecoveryFetcher.Request request;

        RequestAndReceiver(RecoveryFetcher.Receiver receiver, RecoveryFetcher.Request request) {
            this.request = request;
            this.receiver = receiver;
        }
    }

    private static final int REQUEST_TIMEOUT_MS = 3000;
    private static final int QUEUE_CAPACITY = 1000;
    private final ArrayBlockingQueue<RequestAndReceiver> queue;
    private final Time time;

    RecoveryRequestThread(String name,
                          KafkaClient networkClient,
                          Time time) {
        super(name, networkClient, REQUEST_TIMEOUT_MS, time);
        this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.time = time;
    }

    public void enqueueRequest(RecoveryFetcher.Receiver receiver, RecoveryFetcher.Request request) {
        queue.add(new RequestAndReceiver(receiver, request));
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        ArrayList<RequestAndCompletionHandler> requests = new ArrayList<>(this.queue.size());
        Iterator<RequestAndReceiver> iterator = this.queue.iterator();
        while (iterator.hasNext()) {
            RequestAndReceiver r = iterator.next();
            RequestCompletionHandler completionHandler = new RecoveryCompletionHandler(r.receiver, r.request);
            requests.add(new RequestAndCompletionHandler(time.milliseconds(), r.request.node, r.request.builder, completionHandler));
            iterator.remove();
        }
        return requests;
    }
}
