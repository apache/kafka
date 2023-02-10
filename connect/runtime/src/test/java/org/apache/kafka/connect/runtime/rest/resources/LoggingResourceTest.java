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
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class LoggingResourceTest {

    @Test
    public void getLoggersIgnoresNullLevelsTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        Logger a = new Logger("a") {
        };
        a.setLevel(null);
        Logger b = new Logger("b") {
        };
        b.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.listLoggers()).thenCallRealMethod();
        Map<String, Map<String, String>> loggers = (Map<String, Map<String, String>>) loggingResource.listLoggers().getEntity();
        assertEquals(1, loggers.size());
        assertEquals("INFO", loggers.get("b").get("level"));
    }

    @Test
    public void getLoggerFallsbackToEffectiveLogLevelTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.getLogger(any())).thenCallRealMethod();
        Map<String, String> level = (Map<String, String>) loggingResource.getLogger("a").getEntity();
        assertEquals(1, level.size());
        assertEquals("ERROR", level.get("level"));
    }

    @Test
    public void getUnknownLoggerTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.getLogger(any())).thenCallRealMethod();
        assertThrows(NotFoundException.class, () -> loggingResource.getLogger("c"));
    }

    @Test
    public void setLevelTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger p = hierarchy.getLogger("a.b.c.p");
        Logger x = hierarchy.getLogger("a.b.c.p.X");
        Logger y = hierarchy.getLogger("a.b.c.p.Y");
        Logger z = hierarchy.getLogger("a.b.c.p.Z");
        Logger w = hierarchy.getLogger("a.b.c.s.W");
        x.setLevel(Level.INFO);
        y.setLevel(Level.INFO);
        z.setLevel(Level.INFO);
        w.setLevel(Level.INFO);
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
    }

    @Test
    public void setRootLevelTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger p = hierarchy.getLogger("a.b.c.p");
        Logger x = hierarchy.getLogger("a.b.c.p.X");
        Logger y = hierarchy.getLogger("a.b.c.p.Y");
        Logger z = hierarchy.getLogger("a.b.c.p.Z");
        Logger w = hierarchy.getLogger("a.b.c.s.W");
        x.setLevel(Level.INFO);
        y.setLevel(Level.INFO);
        z.setLevel(Level.INFO);
        w.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(x, y, z, w));
        when(loggingResource.lookupLogger("a.b.c.p")).thenReturn(p);
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        List<String> modified = (List<String>) loggingResource.setLevel("root", Collections.singletonMap("level", "DEBUG")).getEntity();
        assertEquals(5, modified.size());
        assertEquals(Arrays.asList("a.b.c.p.X", "a.b.c.p.Y", "a.b.c.p.Z", "a.b.c.s.W", "root"), modified);
        assertNull(p.getLevel());
        assertEquals(root.getLevel(), Level.DEBUG);
        assertEquals(w.getLevel(), Level.DEBUG);
        assertEquals(x.getLevel(), Level.DEBUG);
        assertEquals(y.getLevel(), Level.DEBUG);
        assertEquals(z.getLevel(), Level.DEBUG);
    }

    @Test
    public void setLevelWithEmptyArgTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        assertThrows(BadRequestException.class, () -> loggingResource.setLevel("@root", Collections.emptyMap()));
    }

    @Test
    public void setLevelWithInvalidArgTest() {
        LoggingResource loggingResource = mock(LoggingResource.class);
        Logger root = new Logger("root") {
        };
        root.setLevel(Level.ERROR);
        Hierarchy hierarchy = new Hierarchy(root);
        Logger a = hierarchy.getLogger("a");
        a.setLevel(null);
        Logger b = hierarchy.getLogger("b");
        b.setLevel(Level.INFO);
        when(loggingResource.currentLoggers()).thenReturn(loggers(a, b));
        when(loggingResource.rootLogger()).thenReturn(root);
        when(loggingResource.setLevel(any(), any())).thenCallRealMethod();
        assertThrows(NotFoundException.class, () -> loggingResource.setLevel("@root", Collections.singletonMap("level", "HIGH")));
    }

    private Enumeration<Logger> loggers(Logger... loggers) {
        return new Vector<>(Arrays.asList(loggers)).elements();
    }

}
