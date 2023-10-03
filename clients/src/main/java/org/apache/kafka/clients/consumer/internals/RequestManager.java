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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * {@code PollResult} consist of {@code UnsentRequest} if there are requests to send; otherwise, return the time till
 * the next poll event.
 */
public interface RequestManager {

    /**
     * During normal operation of the {@link Consumer}, a request manager may need to send out network requests.
     * Implementations can return {@link PollResult their need for network I/O} by returning the requests here.
     * Because the {@code poll} method is called within the single-threaded context of the consumer's main network
     * I/O thread, there should be no need for synchronization protection within itself or other state.
     *
     * <p/>
     *
     * <em>Note</em>: no network I/O occurs in this method. The method itself should not block on I/O or for any
     * other reason. This method is called from by the consumer's main network I/O thread. So quick execution of
     * this method in <em>all</em> request managers is critical to ensure that we can heartbeat in a timely fashion.
     *
     * @param currentTimeMs The current system time at which the method was called; useful for determining if
     *                      time-sensitive operations should be performed
     */
    PollResult poll(long currentTimeMs);

    /**
     * On shutdown of the {@link Consumer}, a request manager may need to send out network requests. Implementations
     * can signal that by returning the {@link PollResult close} requests here. Unlike {@link #poll(long)}, the
     * {@code pollOnClose} method is called from the <em>application thread</em>. Therefore, protection should be made
     * when interacting with other any state that could be affected by other threads.
     *
     * <p/>
     *
     * <em>Note</em>: no network I/O occurs in this method. The method itself should not block on I/O or for any
     * other reason. This method is called (indirectly) by the {@link Consumer#close() consumer's close method}.
     * So quick execution of this method in <em>all</em> request managers is critical to ensure that we can
     * complete as many of the shutdown tasks as possible given the user-provided timeout.
     */
    default PollResult pollOnClose() {
        return EMPTY;
    }
}
