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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class LoggersTest {

    private static final long INITIAL_TIME = 1696951712135L;
    private Time time;

    @Before
    public void setup() {
        time = new MockTime(0, INITIAL_TIME, 0);
    }

    @Test
    public void testGetLoggersIgnoresNullLevels() {
        Logger root = logger("root");

        Logger a = logger("a");
        a.setLevel(null);
        Logger b = logger("b");
        b.setLevel(Level.INFO);

        Loggers loggers = new TestLoggers(root, a, b);

        Map<String, LoggerLevel> expectedLevels = Collections.singletonMap(
                "b",
                new LoggerLevel(Level.INFO.toString(), null)
        );
        Map<String, LoggerLevel> actualLevels = loggers.allLevels();
        assertEquals(expectedLevels, actualLevels);
    }

    @Test
    public void testGetLoggerFallsBackToEffectiveLogLevel() {
        Logger root = logger("root");
        root.setLevel(Level.ERROR);

        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);

        Loggers loggers = new TestLoggers(root, a, b);

        LoggerLevel expectedLevel = new LoggerLevel(Level.ERROR.toString(), null);
        LoggerLevel actualLevel = loggers.level("a");
        assertEquals(expectedLevel, actualLevel);
    }

    @Test
    public void testGetUnknownLogger() {
        Logger root = logger("root");
        root.setLevel(Level.ERROR);

        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);

        Loggers loggers = new TestLoggers(root, a, b);

        LoggerLevel level = loggers.level("c");
        assertNull(level);
    }

    @Test
    public void testSetLevel() {
        Logger root = logger("root");
        root.setLevel(Level.ERROR);

        Logger x = logger("a.b.c.p.X");
        Logger y = logger("a.b.c.p.Y");
        Logger z = logger("a.b.c.p.Z");
        Logger w = logger("a.b.c.s.W");
        x.setLevel(Level.INFO);
        y.setLevel(Level.INFO);
        z.setLevel(Level.INFO);
        w.setLevel(Level.INFO);

        // We don't explicitly register a logger for a.b.c.p, so it won't appear in the list of current loggers;
        // one should be created by the Loggers instance when we set the level
        TestLoggers loggers = new TestLoggers(root, x, y, z, w);

        List<String> modified = loggers.setLevel("a.b.c.p", Level.DEBUG);
        assertEquals(Arrays.asList("a.b.c.p", "a.b.c.p.X", "a.b.c.p.Y", "a.b.c.p.Z"), modified);
        assertEquals(Level.DEBUG.toString(), loggers.level("a.b.c.p").level());
        assertEquals(Level.DEBUG, x.getLevel());
        assertEquals(Level.DEBUG, y.getLevel());
        assertEquals(Level.DEBUG, z.getLevel());

        LoggerLevel expectedLevel = new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME);
        LoggerLevel actualLevel = loggers.level("a.b.c.p");
        assertEquals(expectedLevel, actualLevel);

        // Sleep a little and adjust the level of a leaf logger
        time.sleep(10);
        loggers.setLevel("a.b.c.p.X", Level.ERROR);
        expectedLevel = new LoggerLevel(Level.ERROR.toString(), INITIAL_TIME + 10);
        actualLevel = loggers.level("a.b.c.p.X");
        assertEquals(expectedLevel, actualLevel);

        // Make sure that the direct parent logger and a sibling logger remain unaffected
        expectedLevel = new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME);
        actualLevel = loggers.level("a.b.c.p");
        assertEquals(expectedLevel, actualLevel);

        expectedLevel = new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME);
        actualLevel = loggers.level("a.b.c.p.Y");
        assertEquals(expectedLevel, actualLevel);

        // Set the same level again, and verify that the last modified time hasn't been altered
        time.sleep(10);
        loggers.setLevel("a.b.c.p.X", Level.ERROR);
        expectedLevel = new LoggerLevel(Level.ERROR.toString(), INITIAL_TIME + 10);
        actualLevel = loggers.level("a.b.c.p.X");
        assertEquals(expectedLevel, actualLevel);
    }

    @Test
    public void testSetRootLevel() {
        Logger root = logger("root");
        root.setLevel(Level.ERROR);

        Logger p = logger("a.b.c.p");
        Logger x = logger("a.b.c.p.X");
        Logger y = logger("a.b.c.p.Y");
        Logger z = logger("a.b.c.p.Z");
        Logger w = logger("a.b.c.s.W");
        x.setLevel(Level.INFO);
        y.setLevel(Level.INFO);
        z.setLevel(Level.INFO);
        w.setLevel(Level.INFO);

        Loggers loggers = new TestLoggers(root, x, y, z, w);

        List<String> modified = loggers.setLevel("root", Level.DEBUG);
        assertEquals(Arrays.asList("a.b.c.p.X", "a.b.c.p.Y", "a.b.c.p.Z", "a.b.c.s.W", "root"), modified);

        assertNull(p.getLevel());

        assertEquals(root.getLevel(), Level.DEBUG);

        assertEquals(w.getLevel(), Level.DEBUG);
        assertEquals(x.getLevel(), Level.DEBUG);
        assertEquals(y.getLevel(), Level.DEBUG);
        assertEquals(z.getLevel(), Level.DEBUG);

        Map<String, LoggerLevel> expectedLevels = new HashMap<>();
        expectedLevels.put("root", new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME));
        expectedLevels.put("a.b.c.p.X", new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME));
        expectedLevels.put("a.b.c.p.Y", new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME));
        expectedLevels.put("a.b.c.p.Z", new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME));
        expectedLevels.put("a.b.c.s.W", new LoggerLevel(Level.DEBUG.toString(), INITIAL_TIME));

        Map<String, LoggerLevel> actualLevels = loggers.allLevels();
        assertEquals(expectedLevels, actualLevels);
    }

    @Test
    public void testSetLevelNullArguments() {
        Logger root = logger("root");
        Loggers loggers = new TestLoggers(root);
        assertThrows(NullPointerException.class, () -> loggers.setLevel(null, Level.INFO));
        assertThrows(NullPointerException.class, () -> loggers.setLevel("root", null));
    }

    private class TestLoggers extends Loggers {

        private final Logger rootLogger;
        private final Map<String, Logger> currentLoggers;

        public TestLoggers(Logger rootLogger, Logger... knownLoggers) {
            super(time);
            this.rootLogger = rootLogger;
            this.currentLoggers = new HashMap<>(Stream.of(knownLoggers)
                    .collect(Collectors.toMap(
                            Logger::getName,
                            Function.identity()
                    )));
        }

        @Override
        Logger lookupLogger(String logger) {
            return currentLoggers.computeIfAbsent(logger, l -> new Logger(logger) { });
        }

        @Override
        Enumeration<Logger> currentLoggers() {
            return new Vector<>(currentLoggers.values()).elements();
        }

        @Override
        Logger rootLogger() {
            return rootLogger;
        }
    }

    private Logger logger(String name) {
        return new Logger(name) { };
    }

}
