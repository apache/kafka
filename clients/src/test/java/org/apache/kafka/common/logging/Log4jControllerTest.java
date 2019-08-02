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
package org.apache.kafka.common.logging;

import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Hashtable;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Log4jControllerTest {

    @Test
    public void testGetLoggers() {
        Hashtable<String, Logger> currentLoggersMock = new Hashtable<>();
        Logger log1 = new Logger("log1"){};
        log1.setLevel(Level.INFO);
        Logger log2 = new Logger("log2"){};
        log2.setLevel(Level.WARN);
        currentLoggersMock.put("log2", log2);
        currentLoggersMock.put("log1", log1);
        Log4jController controller = mock(Log4jController.class);
        when(controller.getLoggers()).thenCallRealMethod();
        when(controller.getCurrentLevel(any())).thenCallRealMethod();
        when(controller.currentLoggers()).thenReturn(currentLoggersMock.elements());
        when(controller.rootLogLevel()).thenReturn(Level.ERROR);

        List<String> loggers = controller.getLoggers();

        assertEquals("Expecting 3 loggers", 3, loggers.size());
        assertEquals("Root loggers should be set at error level", "root=ERROR", loggers.get(0));
        assertEquals("log1 should be ordered before log2", "log1=INFO", loggers.get(1));
        assertEquals("log2 should be set at warn", "log2=WARN", loggers.get(2));
    }

    @Test
    public void testGetLogLevel() {
        Log4jController controller = mock(Log4jController.class);
        when(controller.getLogLevel(any())).thenCallRealMethod();
        Logger h = new Logger("hello"){};
        when(controller.loggerByName("hello")).thenReturn(h);
        when(controller.getCurrentLevel(any())).thenReturn(Level.WARN);

        assertEquals("logger should be at WARN level", "WARN", controller.getLogLevel("hello"));
    }

    @Test
    public void testSetLogLevel() {
        Log4jController controller = mock(Log4jController.class);
        Logger log = new Logger("hello"){};
        log.setLevel(Level.INFO);
        when(controller.loggerByName("hello")).thenReturn(log);
        when(controller.setLogLevel(any(), any())).thenCallRealMethod();
        when(controller.getLogLevel(any())).thenCallRealMethod();
        assertEquals("original level is INFO", log.getLevel(), Level.INFO);

        controller.setLogLevel("hello", "warn");

        assertEquals("new level should be WARN", Level.WARN, log.getLevel());
    }

    @Test
    public void testEffectiveLogLevel() {
        Log4jController controller = mock(Log4jController.class);
        Logger root = new Logger("some"){};
        Hierarchy hierarchy = new Hierarchy(root);
        Logger log = hierarchy.getLogger("some.name.hello", name -> new Logger(name){});
        root.setLevel(Level.WARN);
        when(controller.loggerByName("some.name.hello")).thenReturn(log);
        when(controller.loggerByName("some")).thenReturn(root);
        when(controller.getLogLevel(any())).thenCallRealMethod();
        when(controller.getCurrentLevel(any())).thenCallRealMethod();
        assertEquals("should be warn", "WARN", controller.getLogLevel("some.name.hello"));
    }
}
