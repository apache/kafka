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
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;


/**
 * The LogReplayTracker manages state associated with replaying the metadata log, such as whether
 * we have seen any records. It is accessed solely from the quorum controller thread.
 */
public class LogReplayTracker {
    public static class Builder {
        private LogContext logContext = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public LogReplayTracker build() {
            if (logContext == null) logContext = new LogContext();
            return new LogReplayTracker(logContext);
        }
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * True if we haven't replayed any records yet.
     */
    private boolean empty;

    private LogReplayTracker(
        LogContext logContext
    ) {
        this.log = logContext.logger(LogReplayTracker.class);
        resetToEmpty();
    }

    void resetToEmpty() {
        this.empty = true;
    }

    boolean empty() {
        return empty;
    }

    void replay(ApiMessage message) {
        empty = false;
    }
}
