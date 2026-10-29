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
package org.apache.kafka.server.logger;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoggingControllerTest {

    @Test
    public void testLoggerLevelIsResolved() {
        LoggingController controller = new LoggingController();
        String loggerName = "org.apache.kafka";
        String childLoggerName = "org.apache.kafka.server.logger.LoggingControllerTest";
        String previousLevel = controller.getLogLevel(loggerName);
        try {
            controller.setLogLevel(loggerName, "TRACE");
            // Do some logging so that the Logger is created within the hierarchy
            // (until loggers are used only loggers in the config file exist).
            LoggerFactory.getLogger(childLoggerName).trace("test");
            assertEquals("TRACE", controller.getLogLevel(loggerName));
            assertEquals("TRACE", controller.getLogLevel(childLoggerName));
            assertTrue(controller.getLoggers().contains(loggerName + "=TRACE"));
            assertTrue(controller.getLoggers().contains(childLoggerName + "=TRACE"));
        } finally {
            controller.setLogLevel(loggerName, previousLevel);
        }
    }
}
