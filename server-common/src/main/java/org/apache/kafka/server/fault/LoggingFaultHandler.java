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
import org.slf4j.LoggerFactory;


/**
 * A fault handler which logs an error message and executes a runnable.
 */
public class LoggingFaultHandler implements FaultHandler {
    private static final Logger log = LoggerFactory.getLogger(LoggingFaultHandler.class);
    private final String type;
    private final Runnable action;

    public LoggingFaultHandler(
        String type,
        Runnable action
    ) {
        this.type = type;
        this.action = action;
    }

    @Override
    public RuntimeException handleFault(String failureMessage, Throwable cause) {
        if (cause == null) {
            log.error("Encountered {} fault: {}", type, failureMessage);
        } else {
            log.error("Encountered {} fault: {}", type, failureMessage, cause);
        }
        try {
            action.run();
        } catch (Throwable e) {
            log.error("Failed to run LoggingFaultHandler action.", e);
        }
        return new FaultHandlerException(failureMessage, cause);
    }
}
