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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingSignalHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingSignalHandler.class);

    /**
     * Register signal handler to log termination due to SIGTERM, SIGHUP and SIGINT (control-c) if not on Windows.
     */
    public static void maybeRegister() {
        if (!OperatingSystem.IS_WINDOWS) {
            final Map<String, SignalHandler> jvmSignalHandlers = new ConcurrentHashMap<>();
            register("TERM", jvmSignalHandlers);
            register("INT", jvmSignalHandlers);
            register("HUP", jvmSignalHandlers);
        }
    }

    private static void register(String signalName, final Map<String, SignalHandler> jvmSignalHandlers) {
        SignalHandler oldHandler = Signal.handle(new Signal(signalName), new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                log.info("Terminating process due to signal {}", signal);
                SignalHandler oldHandler = jvmSignalHandlers.get(signal.getName());
                if (oldHandler != null)
                    oldHandler.handle(signal);
            }
        });
        jvmSignalHandlers.put(signalName, oldHandler);
    }
}
