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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.KafkaException;

/**
 * Representing certain types of fatal exceptions thrown from
 * {@link org.apache.kafka.raft} module. The Raft client will catch
 * this exception and wrap it with a controlled shutdown to make the state transition smooth.
 * The general handling logic is like:
 *
 * 1.
 *
 *
 */
public class LogWriteFailureException extends KafkaException {

    private final boolean shutdownNeeded;

    public LogWriteFailureException(String message, boolean shutdownNeeded) {
        super(message);
        this.shutdownNeeded = shutdownNeeded;
    }

    public LogWriteFailureException(String message, Throwable cause, boolean shutdownNeeded) {
        super(message, cause);
        this.shutdownNeeded = shutdownNeeded;
    }

    public boolean shutdownNeeded() {
        return shutdownNeeded;
    }
}
