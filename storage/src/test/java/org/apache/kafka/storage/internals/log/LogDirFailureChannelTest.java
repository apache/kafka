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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogDirFailureChannelTest {

    /**
     * Only the failure that takes a log dir offline is logged and enqueued: the failures that follow it in the same log
     * dir neither flood the log nor enqueue the log dir again, which would overflow the queue sized for one entry per
     * log dir.
     */
    @Test
    public void testOnlyTheFailureThatTakesLogDirOfflineIsLoggedAndEnqueued() throws InterruptedException {
        LogDirFailureChannel channel = new LogDirFailureChannel(2);
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(LogDirFailureChannel.class)) {
            appender.setClassLogger(LogDirFailureChannel.class, Level.ERROR);

            channel.maybeAddOfflineLogDir("/data1", "first", new IOException("Input/output error"));
            channel.maybeAddOfflineLogDir("/data1", "second", new IOException("Input/output error"));
            channel.maybeAddOfflineLogDir("/data2", "other", new IOException("Input/output error"));
            // The queue is full: a later failure of an offline log dir must not try to add to it
            channel.maybeAddOfflineLogDir("/data1", "third", new IOException("Input/output error"));

            List<LogCaptureAppender.Event> events = appender.getEvents();
            assertEquals(List.of("first", "other"), events.stream().map(LogCaptureAppender.Event::getMessage).toList());
            events.forEach(event -> {
                assertEquals("ERROR", event.getLevel());
                assertEquals(Optional.of(IOException.class.getName()), event.getThrowableClassName());
            });
        }
        assertTrue(channel.hasOfflineLogDir("/data1"));
        assertTrue(channel.hasOfflineLogDir("/data2"));
        assertEquals("/data1", channel.takeNextOfflineLogDir());
        assertEquals("/data2", channel.takeNextOfflineLogDir());
    }
}
