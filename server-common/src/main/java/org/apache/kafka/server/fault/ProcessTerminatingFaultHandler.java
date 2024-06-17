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

import java.util.Objects;
import org.apache.kafka.common.utils.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a fault handler which terminates the JVM process.
 */
final public class ProcessTerminatingFaultHandler implements FaultHandler {
    private static final Logger log = LoggerFactory.getLogger(ProcessTerminatingFaultHandler.class);

    private final Runnable action;
    private final boolean shouldHalt;

    private ProcessTerminatingFaultHandler(boolean shouldHalt, Runnable action) {
        this.shouldHalt = shouldHalt;
        this.action = action;
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

    public static final class Builder {
        private boolean shouldHalt = true;
        private Runnable action = () -> { };

        /**
         * Set if halt or exit should be used.
         * <br>
         * When {@code value} is {@code false} {@code Exit.exit} is called, otherwise {@code Exit.halt} is
         * called. The default value is {@code true}.
         * <br>
         * The default implementation of {@code Exit.exit} calls {@code Runtime.exit} which
         * blocks on all of the shutdown hooks executing.
         * <br>
         * The default implementation of {@code Exit.halt} calls {@code Runtime.halt} which
         * forcibly terminates the JVM.
         */
        public Builder setShouldHalt(boolean value) {
            shouldHalt = value;
            return this;
        }

        /**
         * Set the {@code Runnable} to run when handling a fault.
         */
        public Builder setAction(Runnable action) {
            this.action = Objects.requireNonNull(action);
            return this;
        }

        public ProcessTerminatingFaultHandler build() {
            return new ProcessTerminatingFaultHandler(shouldHalt, action);
        }
    }
}
