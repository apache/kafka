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

import org.apache.kafka.common.protocol.ApiMessageAndVersion;

import java.util.List;

/**
 * The MetaLogManager handles storing metadata and electing leaders.
 */
public interface MetaLogManager {

    /**
     * Start this meta log manager.
     * The manager must be ready to accept incoming calls after this function returns.
     * It is an error to initialize a MetaLogManager more than once.
     */
    void initialize() throws Exception;

    /**
     * Register the listener.  The manager must be initialized already.
     * The listener must be ready to accept incoming calls immediately.
     *
     * @param listener      The listener to register.
     */
    void register(MetaLogListener listener) throws Exception;

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
     * Returns the current leader.  The active node may change immediately after this
     * function is called, of course.
     */
    MetaLogLeader leader();

    /**
     * Returns the node id.
     */
    int nodeId();

}
