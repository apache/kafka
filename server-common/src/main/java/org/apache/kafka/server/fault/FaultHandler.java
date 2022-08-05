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

package org.apache.kafka.server.fault;

import org.slf4j.Logger;


/**
 * Handle a server fault.
 */
public interface FaultHandler {
    /**
     * Handle a fault.
     *
     * @param failureMessage        The failure message to log.
     */
    default void handleFault(String failureMessage) {
        handleFault(failureMessage, null);
    }

    /**
     * Handle a fault.
     *
     * @param failureMessage        The failure message to log.
     * @param cause                 The exception that caused the problem, or null.
     */
    void handleFault(String failureMessage, Throwable cause);

    /**
     * Log a failure message about a fault.
     *
     * @param log               The log4j logger.
     * @param failureMessage    The failure message.
     * @param cause             The exception which caused the failure, or null.
     */
    static void logFailureMessage(Logger log, String failureMessage, Throwable cause) {
        if (cause == null) {
            log.error("Encountered fatal fault: {}", failureMessage);
        } else {
            log.error("Encountered fatal fault: {}", failureMessage, cause);
        }
    }
}
