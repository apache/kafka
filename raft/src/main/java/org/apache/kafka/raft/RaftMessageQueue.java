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
 * This class is used to serialize inbound requests or responses to outbound requests.
 * It basically just allows us to wrap a blocking queue so that we can have a mocked
 * implementation which does not depend on system time.
 *
 * See {@link org.apache.kafka.raft.internals.BlockingMessageQueue}.
 */
public interface RaftMessageQueue {

    /**
     * Block for the arrival of a new message.
     *
     * @param timeoutMs timeout in milliseconds to wait for a new event
     * @return the event or null if either the timeout was reached or there was
     *     a call to {@link #wakeup()} before any events became available
     */
    RaftMessage poll(long timeoutMs);

    /**
     * Add a new message to the queue.
     *
     * @param message the message to deliver
     * @throws IllegalStateException if the queue cannot accept the message
     */
    void add(RaftMessage message);

    /**
     * Check whether there are pending messages awaiting delivery.
     *
     * @return if there are no pending messages to deliver
     */
    boolean isEmpty();

    /**
     * Wakeup the thread blocking in {@link #poll(long)}. This will cause
     * {@link #poll(long)} to return null if no messages are available.
     */
    void wakeup();

}
