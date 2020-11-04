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
import org.apache.kafka.common.protocol.ApiMessageAndVersion;

import java.util.List;

/**
 * The MetaLogManager handles storing metadata and electing leaders.
 */
public interface MetaLogManager extends AutoCloseable {
    /**
     * Listeners receive notifications from the MetaLogManager.
     */
    interface Listener {
        /**
         * Called when the MetaLogManager commits some messages.
         *
         * @param lastOffset    The last offset found in all the given messages.
         * @param messages      The messages.
         */
        void handleCommits(long lastOffset, List<ApiMessage> messages);

        /**
         * Called when the MetaLogManager has claimed the leadership.
         *
         * @param epoch         The controller epoch that is starting.
         */
        default void handleClaim(long epoch) {}

        /**
         * Called when the MetaLogManager has renounced the leadership.
         *
         * @param epoch         The controller epoch that has ended.
         */
        default void handleRenounce(long epoch) {}

        /**
         * Called when the MetaLogManager has finished shutting down, and wants to tell its
         * listener that it is safe to shut down as well.
         */
        default void beginShutdown() {}

        /**
         * If this listener is currently active, return the controller epoch it is active
         * for.  Otherwise, return -1.
         */
        default long currentClaimEpoch() {
            return -1L;
        }
    }

    /**
     * Register the listener, and start this meta log manager.
     * The manager must be ready to accept incoming calls after this function returns.
     * It is an error to initialize a MetaLogManager more than once.
     *
     * @param listener      The listener to register.
     */
    void initialize(Listener listener);

    /**
     * Schedule a write to the log.
     *
     * The write will be scheduled to happen at some time in the future.  There is no
     * error return or exception thrown if the write fails.  Instead, the listener may
     * regard the write as successful if and only if the MetaLogManager reaches the given
     * index before renouncing its leadership.  The listener should determine this by
     * monitoring the committed indexes.
     *
     * @param epoch         The controller epoch.
     * @param batch         The batch of messages to write.
     *
     * @return              The index of the message.
     */
    long scheduleWrite(long epoch, List<ApiMessageAndVersion> batch);

    /**
     * Renounce the leadership.
     *
     * @param epoch         The epoch.  If this does not match the current epoch, this
     *                      call will be ignored.
     */
    void renounce(long epoch);

    /**
     * Begin shutting down, but don't block.  You must still call close to clean up all
     * resources.
     */
    void beginShutdown();

    /**
     * Returns the current active node, or -1 if there is none.  The active node may
     * change immediately after this function is called, of course.
     */
    int activeNode();

    /**
     * Blocks until we have shut down and freed all resources.  It is not necessary to
     * call beginShutdown before calling this function.
     */
    void close() throws InterruptedException;
}
