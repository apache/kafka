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

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.WARN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoggersTest {
    private static final long INITIAL_TIME = 1696951712135L;
    private Loggers.Log4jLoggers loggers;
    private Time time;

    @BeforeEach
    public void setup() {
        time = new MockTime(0, INITIAL_TIME, 0);
        loggers = (Loggers.Log4jLoggers) Loggers.newInstance(time);
    }

    @Test
    public void testLevelWithNullLoggerName() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> loggers.level(null));
        assertEquals("Logger may not be null", exception.getMessage());
    }

    @Test
    public void testLevelWithValidRootLoggerNames() {
        assertEquals(
            loggers.level(""),
            loggers.level("root"),
            "Root logger level should be the same whether accessed via empty string or 'root' name"
        );
    }

    @Test
    public void testLevelWithExistLoggerName() {
        loggers.setLevel("foo", DEBUG.name());
        assertEquals(new LoggerLevel(DEBUG.name(), INITIAL_TIME),
            loggers.level("foo")
        );
    }

    @Test
    public void testLevelWithNonExistLoggerName() {
        assertNull(loggers.level("another-dummy"), "Unconfigured logger should return null");
    }

    @Test
    public void testLevelWithNewlyCreatedLogger() {
        loggers.setLevel("dummy", ERROR.name());
        assertEquals(
            new LoggerLevel(ERROR.name(), time.milliseconds()),
            loggers.level("dummy"),
            "Newly created logger should have the level we just set"
        );
    }

    @Test
    public void testAllLevelsAfterCreatingNewLogger() {
        loggers.setLevel("foo", WARN.name());
        loggers.setLevel("bar", ERROR.name());
        Map<String, LoggerLevel> loggerToLevel = loggers.allLevels();
        Map<String, LoggerLevel> expectedLevels = Map.of(
            "foo", new LoggerLevel(WARN.name(), INITIAL_TIME),
            "bar", new LoggerLevel(ERROR.name(), INITIAL_TIME)
        );

        assertTrue(loggerToLevel.entrySet().containsAll(expectedLevels.entrySet()));
    }

    @Test
    public void testSetLevelWithNullNameSpaceOrNullLevel() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> loggers.setLevel(null, null));
        assertEquals("Logging namespace may not be null", exception.getMessage());

        exception = assertThrows(NullPointerException.class, () -> loggers.setLevel("dummy", null));
        assertEquals("Level may not be null", exception.getMessage());
    }

    @Test
    public void testSetLevelWithValidRootLoggerNames() {
        loggers.setLevel("", ERROR.name());
        List<String> setLevelResultWithRoot = loggers.setLevel("root", ERROR.name());
        assertTrue(setLevelResultWithRoot.isEmpty(),
            "Setting level with empty string ('') and 'root' should affect the same set of loggers - " +
            "when setting the same level twice, second call should return empty list indicating no loggers were affected");
    }

    @Test
    public void testSetLevel() {
        loggers.setLevel("a.b.c", DEBUG.name());
        loggers.setLevel("a.b", ERROR.name());
        loggers.setLevel("a", WARN.name());
        Map<String, LoggerLevel> expected = Map.of(
            "a", new LoggerLevel(WARN.name(), INITIAL_TIME),
            "a.b", new LoggerLevel(WARN.name(), INITIAL_TIME),
            "a.b.c", new LoggerLevel(WARN.name(), INITIAL_TIME)
        );
        assertTrue(loggers.allLevels().entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void testLookupLoggerAfterCreatingNewLogger() {
        loggers.setLevel("dummy", INFO.name());
        Logger logger = loggers.lookupLogger("dummy");
        assertNotNull(logger);
        assertEquals(INFO, logger.getLevel());
    }

    @Test
    public void testSetLevelWithSameLevel() {
        String loggerName = "dummy";
        loggers.setLevel(loggerName, DEBUG.name());
        time.sleep(100);
        loggers.setLevel(loggerName, DEBUG.name());
        assertEquals(
            new LoggerLevel(DEBUG.name(), INITIAL_TIME),
            loggers.allLevels().get(loggerName),
            "Setting same log level should not update the lastModified timestamp"
        );
    }

    @Test
    public void testSetLevelWithDifferentLevels() {
        String loggerName = "dummy";
        loggers.setLevel(loggerName, DEBUG.name());
        time.sleep(100);
        loggers.setLevel(loggerName, WARN.name());
        assertEquals(
            new LoggerLevel(WARN.name(), INITIAL_TIME + 100),
            loggers.allLevels().get(loggerName),
            "Setting different log level should update the lastModified timestamp"
        );
    }

    @Test
    public void testLookupLoggerWithValidRootLoggerNames() {
        assertEquals(
            loggers.lookupLogger("root"),
            loggers.lookupLogger(""),
            "Both 'root' and empty string should retrieve the root logger"
        );

        assertEquals(
            loggers.lookupLogger(""),
            loggers.rootLogger(),
            "Empty string lookup should match direct root logger access"
        );
    }
}
