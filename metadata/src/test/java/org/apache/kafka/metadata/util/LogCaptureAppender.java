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

package org.apache.kafka.metadata.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.LinkedList;
import java.util.List;

class LogCaptureAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new LinkedList<>();

    @Override
    protected void append(final LoggingEvent event) {
        synchronized (events) {
            events.add(event);
        }
    }

    public List<String> getMessages() {
        final LinkedList<String> result = new LinkedList<>();
        synchronized (events) {
            for (final LoggingEvent event : events) {
                result.add(event.getRenderedMessage());
            }
        }
        return result;
    }

    @Override
    public void close() {
        synchronized (events) {
            events.clear();
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}

