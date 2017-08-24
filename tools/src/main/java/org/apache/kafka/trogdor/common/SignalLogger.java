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

package org.apache.kafka.trogdor.common;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Log a message when we have received an operating system signal.
 */
public class SignalLogger {
    private static AtomicBoolean registered = new AtomicBoolean(false);

    private static final Logger log = LoggerFactory.getLogger(SignalLogger.class);

    private static class SignalLoggerHandler implements SignalHandler {
        final private SignalHandler nextHandler;

        SignalLoggerHandler(String name) {
            this.nextHandler = Signal.handle(new Signal(name), this);
        }

        @Override
        public void handle(Signal signal) {
            log.error("RECEIVED SIGNAL " + signal.getNumber() + ": SIG" + signal.getName());
            nextHandler.handle(signal);
        }
    }

    public static void register() {
        if (!registered.compareAndSet(false, true)) {
            return;
        }
        StringBuilder bld = new StringBuilder();
        bld.append("registered UNIX signal handlers for [");
        final String[] signalNames = {"TERM", "HUP", "INT"};
        String separator = "";
        for (String name : signalNames) {
            try {
                new SignalLoggerHandler(name);
                bld.append(separator);
                bld.append(name);
                separator = ", ";
            } catch (Exception e) {
                log.warn("Failed to install signal logger for SIG{}", name, e);
            }
        }
        bld.append("]");
        log.info(bld.toString());
    }
}
