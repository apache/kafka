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

import org.apache.kafka.common.utils.Exit;


/**
 * This is a fault handler which terminates the JVM process.
 */
final public class ProcessTerminatingFaultHandler implements FaultHandler {
    private static final Logger log = LoggerFactory.getLogger(ProcessTerminatingFaultHandler.class);

    private final Runnable action;
    private final boolean shouldHalt;

    private ProcessTerminatingFaultHandler(boolean shouldHalt, Runnable action) {
        this.action = action;
        this.shouldHalt = shouldHalt;
    }

    @Override
    public RuntimeException handleFault(String failureMessage, Throwable cause) {
        if (cause == null) {
            log.error("Encountered fatal fault: {}", failureMessage);
        } else {
            log.error("Encountered fatal fault: {}", failureMessage, cause);
        }

        try {
            action.run();
        } catch (Throwable e) {
            log.error("Failed to run terminating action.", e);
        }

        int statusCode = 1;
        if (shouldHalt) {
            Exit.halt(statusCode);
        } else {
            Exit.exit(statusCode);
        }

        return null;
    }

    /**
     * Same as {@code exitingWithAction} with a no-op action.
     */
    public static FaultHandler exiting() {
        return exitingWithAction(() -> { });
    }

    /**
     * Handle faults by running an action and calling {@code Exit.exit}.
     *
     * The default implementation of {@code Exit.exit} calls {@code Runtime.exit} which
     * waits on all of the shutdown hooks executing.
     */
    public static FaultHandler exitingWithAction(Runnable action) {
        return new ProcessTerminatingFaultHandler(false, action);
    }

    /**
     * Same as {@code haltingWithAction} with a no-op action.
     */
    public static FaultHandler halting() {
        return haltingWithAction(() -> { });
    }

    /**
     * Handle faults by running an action and calling {@code Exit.halt}.
     *
     * The default implementation of {@code Exit.halt} calls {@code Runtime.halt} which
     * forcibly terminates the JVM.
     */
    public static FaultHandler haltingWithAction(Runnable action) {
        return new ProcessTerminatingFaultHandler(true, action);
    }
}
