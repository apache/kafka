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

package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.List;

/**
 * Provides additional utilities for {@link NetworkClient} (e.g. to implement blocking behaviour).
 */
public final class NetworkClientUtils {

    private NetworkClientUtils() {}

    /**
     * Checks whether the node is currently connected, first calling `client.poll` to ensure that any pending
     * disconnects have been processed.
     *
     * This method can be used to check the status of a connection prior to calling the blocking version to be able
     * to tell whether the latter completed a new connection.
     */
    public static boolean isReady(KafkaClient client, Node node, long currentTime) {
        client.poll(0, currentTime);
        return client.isReady(node, currentTime);
    }

    /**
     * Invokes `client.poll` to discard pending disconnects, followed by `client.ready` and 0 or more `client.poll`
     * invocations until the connection to `node` is ready, the timeoutMs expires or the connection fails.
     *
     * It returns `true` if the call completes normally or `false` if the timeoutMs expires. If the connection fails,
     * an `IOException` is thrown instead. Note that if the `NetworkClient` has been configured with a positive
     * connection timeoutMs, it is possible for this method to raise an `IOException` for a previous connection which
     * has recently disconnected. If authentication to the node fails, an `AuthenticationException` is thrown.
     *
     * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
     * care.
     */
    public static boolean awaitReady(KafkaClient client, Node node, Time time, long timeoutMs) throws IOException {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("Timeout needs to be greater than 0");
        }
        long startTime = time.milliseconds();

        if (isReady(client, node, startTime) ||  client.ready(node, startTime))
            return true;

        long attemptStartTime = time.milliseconds();
        while (!client.isReady(node, attemptStartTime) && attemptStartTime - startTime < timeoutMs) {
            if (client.connectionFailed(node)) {
                throw new IOException("Connection to " + node + " failed.");
            }
            long pollTimeout = timeoutMs - (attemptStartTime - startTime); // initialize in this order to avoid overflow

            // If the network client is waiting to send data for some reason (eg. throttling or retry backoff),
            // polling longer than that is potentially dangerous as the producer will not attempt to send
            // any pending requests.
            long waitingTime = client.pollDelayMs(node, startTime);
            if (waitingTime > 0 && pollTimeout > waitingTime) {
                // Block only until the next-scheduled time that it's okay to send data to the producer,
                // wake up, and try again. This is the way.
                pollTimeout = waitingTime;
            }

            client.poll(pollTimeout, attemptStartTime);
            if (client.authenticationException(node) != null)
                throw client.authenticationException(node);
            attemptStartTime = time.milliseconds();
        }
        return client.isReady(node, attemptStartTime);
    }

    /**
     * Invokes `client.send` followed by 1 or more `client.poll` invocations until a response is received or a
     * disconnection happens (which can happen for a number of reasons including a request timeout).
     *
     * In case of a disconnection, an `IOException` is thrown.
     * If shutdown is initiated on the client during this method, an IOException is thrown.
     *
     * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
     * care.
     */
    public static ClientResponse sendAndReceive(KafkaClient client, ClientRequest request, Time time) throws IOException {
        try {
            client.send(request, time.milliseconds());
            while (client.active()) {
                List<ClientResponse> responses = client.poll(Long.MAX_VALUE, time.milliseconds());
                for (ClientResponse response : responses) {
                    if (response.requestHeader().correlationId() == request.correlationId()) {
                        if (response.wasDisconnected()) {
                            throw new IOException("Connection to " + response.destination() + " was disconnected before the response was read");
                        }
                        if (response.versionMismatch() != null) {
                            throw response.versionMismatch();
                        }
                        return response;
                    }
                }
            }
            throw new IOException("Client was shutdown before response was read");
        } catch (DisconnectException e) {
            if (client.active())
                throw e;
            else
                throw new IOException("Client was shutdown before response was read");

        }
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     */
    public static boolean isUnavailable(KafkaClient client, Node node, Time time) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    public static void maybeThrowAuthFailure(KafkaClient client, Node node) {
        AuthenticationException exception = client.authenticationException(node);
        if (exception != null)
            throw exception;
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the
     * failed status of a socket.
     */
    public static void tryConnect(KafkaClient client, Node node, Time time) {
        client.ready(node, time.milliseconds());
    }
}
