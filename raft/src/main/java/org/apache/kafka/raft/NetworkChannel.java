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
package org.apache.kafka.raft;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * A simple network interface with few assumptions. We do not assume ordering
 * of requests or even that every request will receive a response.
 */
public interface NetworkChannel extends Closeable {

    /**
     * Generate a new and unique correlationId for a new request to be sent.
     */
    int newCorrelationId();

    /**
     * Send an outbound message. This could be either an outbound request
     * (i.e. an instance of {@link org.apache.kafka.raft.RaftRequest.Outbound})
     * or a response to a request that was received through {@link #receive(long)}
     * (i.e. an instance of {@link org.apache.kafka.raft.RaftResponse.Outbound}).
     */
    void send(RaftMessage message);

    /**
     * Receive inbound messages. These could contain either inbound requests
     * (i.e. instances of {@link org.apache.kafka.raft.RaftRequest.Inbound})
     * or responses to outbound requests sent through {@link #send(RaftMessage)}
     * (i.e. instances of {@link org.apache.kafka.raft.RaftResponse.Inbound}).
     */
    List<RaftMessage> receive(long timeoutMs);

    /**
     * Wakeup the channel if it is blocking in {@link #receive(long)}. This will cause
     * the call to immediately return with whatever messages are available.
     */
    void wakeup();

    /**
     * Update connection information for the given id.
     */
    void updateEndpoint(int id, InetSocketAddress address);

    default void close() {}

}
