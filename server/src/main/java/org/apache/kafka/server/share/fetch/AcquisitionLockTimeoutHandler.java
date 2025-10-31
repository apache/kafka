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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.server.util.timer.TimerTask;

/**
 * AcquisitionLockTimeoutHandler is an interface that defines a handler for acquisition lock timeouts.
 * It is used to handle cases where the acquisition lock for a share partition times out.
 */
public interface AcquisitionLockTimeoutHandler {

    /**
     * Handles the acquisition lock timeout for a share partition.
     *
     * @param memberId the id of the member that requested the lock
     * @param firstOffset the first offset
     * @param lastOffset the last offset
     */
    void handle(String memberId, long firstOffset, long lastOffset, TimerTask timerTask);

}
