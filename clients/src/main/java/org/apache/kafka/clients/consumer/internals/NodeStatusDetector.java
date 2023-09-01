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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.Time;

/**
 * Use {@code NodeStatusDetector} to determine the status of a given broker {@link Node}. It's also
 * possible to check for previous authentication errors if the node isn't available.
 *
 * @see ConsumerNetworkClient
 * @see NetworkClientDelegate
 */
public interface NodeStatusDetector {

    /**
     * Check if the node is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     *
     * @param node {@link Node} to check for availability
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    boolean isUnavailable(Node node);

    /**
     * Checks for an authentication error on a given node and throws the exception if it exists.
     *
     * @param node {@link Node} to check for a previous {@link AuthenticationException}; if found it is thrown
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    void maybeThrowAuthFailure(Node node);

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting
     * the failed status of a socket.
     *
     * @param node The node to connect to
     */
    void tryConnect(Node node);
}