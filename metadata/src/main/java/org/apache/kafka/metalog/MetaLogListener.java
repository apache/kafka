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

package org.apache.kafka.metalog;

import org.apache.kafka.common.protocol.ApiMessage;

import java.util.List;

/**
 * Listeners receive notifications from the MetaLogManager.
 */
public interface MetaLogListener {
    /**
     * Called when the MetaLogManager commits some messages.
     *
     * @param lastOffset    The last offset found in all the given messages.
     * @param messages      The messages.
     */
    void handleCommits(long lastOffset, List<ApiMessage> messages);

    /**
     * Called when a new leader is elected.
     *
     * @param leader        The new leader id and epoch.
     */
    default void handleNewLeader(MetaLogLeader leader) {}

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
}
