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

/**
 * A simple network interface with few assumptions. We do not assume ordering
 * of requests or even that every outbound request will receive a response.
 */
public interface NetworkChannel extends AutoCloseable {

    /**
     * Generate a new and unique correlationId for a new request to be sent.
     */
    int newCorrelationId();

    /**
     * Send an outbound request message.
     *
     * @param request outbound request to send
     */
    void send(RaftRequest.Outbound request);

    /**
     * Update connection information for the given id.
     */
    void updateEndpoint(int id, RaftConfig.InetAddressSpec address);

    default void close() throws InterruptedException {}

}
