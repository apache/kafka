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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class LoggingResourceTest {

    @Test
    public void getLoggersIgnoresOFFLevelsTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.OFF);
        final Logger a = loggerContext.getLogger("a");
        a.setLevel(null);
        final Logger b = loggerContext.getLogger("b");
        b.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.listLoggers()).thenCallRealMethod();
        Map<String, Map<String, String>> loggers = (Map<String, Map<String, String>>) loggingResource.listLoggers().getEntity();
        assertEquals(1, loggers.size());
        assertEquals("INFO", loggers.get("b").get("level"));

        // restore
        loggerContext.removeObject("b");
        loggerContext.removeObject("a");
        loggerContext.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void getLoggerFallsbackToEffectiveLogLevelTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger a = loggerContext.getLogger("a");
        a.setLevel(null);
        final Logger b = loggerContext.getLogger("b");
        b.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.getLogger(any())).thenCallRealMethod();
        Map<String, String> level = (Map<String, String>) loggingResource.getLogger("a").getEntity();
        assertEquals(1, level.size());
        assertEquals("ERROR", level.get("level"));

        // restore
        loggerContext.removeObject("b");
        loggerContext.removeObject("a");
        loggerContext.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void getUnknownLoggerTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger a = loggerContext.getLogger("a");
        a.setLevel(null);
        final Logger b = loggerContext.getLogger("b");
        b.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.getLogger(any())).thenCallRealMethod();
        assertThrows(NotFoundException.class, () -> loggingResource.getLogger("c"));
    }

    @Test
    public void setLevelTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger p = loggerContext.getLogger("a.b.c.p");
        p.setLevel(Level.INFO);
        final Logger x = loggerContext.getLogger("a.b.c.p.X");
        x.setLevel(Level.INFO);
        final Logger y = loggerContext.getLogger("a.b.c.p.Y");
        y.setLevel(Level.INFO);
        final Logger z = loggerContext.getLogger("a.b.c.p.Z");
        z.setLevel(Level.INFO);
        final Logger w = loggerContext.getLogger("a.b.c.s.W");
        w.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(x, y, z, w));
        when(loggingResource.lookupLogger("a.b.c.p")).thenReturn(p);
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        List<String> modified = (List<String>) loggingResource.setLevel("a.b.c.p", Collections.singletonMap("level", "DEBUG")).getEntity();
        assertEquals(4, modified.size());
        assertEquals(Arrays.asList("a.b.c.p", "a.b.c.p.X", "a.b.c.p.Y", "a.b.c.p.Z"), modified);
        assertEquals(p.getLevel(), Level.DEBUG);
        assertEquals(x.getLevel(), Level.DEBUG);
        assertEquals(y.getLevel(), Level.DEBUG);
        assertEquals(z.getLevel(), Level.DEBUG);

        // restore
        loggerContext.removeObject("a.b.c.s.W");
        loggerContext.removeObject("a.b.c.p.Z");
        loggerContext.removeObject("a.b.c.p.Y");
        loggerContext.removeObject("a.b.c.p.X");
        loggerContext.removeObject("a.b.c.p");
        loggerContext.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void setRootLevelTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger p = loggerContext.getLogger("a.b.c.p");
        p.setLevel(Level.INFO);
        final Logger x = loggerContext.getLogger("a.b.c.p.X");
        x.setLevel(Level.INFO);
        final Logger y = loggerContext.getLogger("a.b.c.p.Y");
        y.setLevel(Level.INFO);
        final Logger z = loggerContext.getLogger("a.b.c.p.Z");
        z.setLevel(Level.INFO);
        final Logger w = loggerContext.getLogger("a.b.c.s.W");
        w.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(x, y, z, w));
        when(loggingResource.lookupLogger("a.b.c.p")).thenReturn(p);
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        List<String> modified = (List<String>) loggingResource.setLevel("root", Collections.singletonMap("level", "DEBUG")).getEntity();
        assertEquals(5, modified.size());
        assertEquals(Arrays.asList("a.b.c.p.X", "a.b.c.p.Y", "a.b.c.p.Z", "a.b.c.s.W", "root"), modified);
        assertEquals(root.getLevel(), Level.DEBUG);
        assertEquals(p.getLevel(), Level.INFO);
        assertEquals(w.getLevel(), Level.DEBUG);
        assertEquals(x.getLevel(), Level.DEBUG);
        assertEquals(y.getLevel(), Level.DEBUG);
        assertEquals(z.getLevel(), Level.DEBUG);

        // restore
        loggerContext.removeObject("a.b.c.s.W");
        loggerContext.removeObject("a.b.c.p.Z");
        loggerContext.removeObject("a.b.c.p.Y");
        loggerContext.removeObject("a.b.c.p.X");
        loggerContext.removeObject("a.b.c.p");
        loggerContext.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void setLevelWithEmptyArgTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger a = loggerContext.getLogger("a");
        a.setLevel(null);
        final Logger b = loggerContext.getLogger("b");
        b.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        assertThrows(BadRequestException.class, () -> loggingResource.setLevel("@root", Collections.emptyMap()));
    }

    @Test
    public void setLevelWithInvalidArgTest() {
        // setup
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Logger root = loggerContext.getRootLogger();
        root.setLevel(Level.ERROR);
        final Logger a = loggerContext.getLogger("a");
        a.setLevel(null);
        final Logger b = loggerContext.getLogger("b");
        b.setLevel(Level.INFO);

        LoggingResource loggingResource = mock(LoggingResource.class);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        assertThrows(NotFoundException.class, () -> loggingResource.setLevel("@root", Collections.singletonMap("level", "HIGH")));
    }

    private List<Logger> loggers(Logger... loggers) {
        return Arrays.asList(loggers);
    }

}
