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

package org.apache.kafka.deferred;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * The queue which holds deferred events that have been started, but not yet completed.
 * We wait for the high watermark of the log to advance before completing them.
 */
public class DeferredEventQueue {
    private final Logger log;

    /**
     * A map from log offsets to events.  Each event will be completed once the log
     * advances past its offset.
     */
    private final TreeMap<Long, List<DeferredEvent>> pending = new TreeMap<>();

    public DeferredEventQueue(LogContext logContext) {
        this.log = logContext.logger(DeferredEventQueue.class);
    }

    /**
     * Complete some purgatory entries.
     *
     * @param offset        The offset which the high water mark has advanced to.
     */
    public void completeUpTo(long offset) {
        Iterator<Entry<Long, List<DeferredEvent>>> iter = pending.entrySet().iterator();
        int numCompleted = 0;
        while (iter.hasNext()) {
            Entry<Long, List<DeferredEvent>> entry = iter.next();
            if (entry.getKey() > offset) {
                break;
            }
            for (DeferredEvent event : entry.getValue()) {
                log.debug("completeUpTo({}): successfully completing {}", offset, event);
                event.complete(null);
                numCompleted++;
            }
            iter.remove();
        }
        if (log.isTraceEnabled()) {
            log.trace("completeUpTo({}): successfully completed {} deferred entries",
                    offset, numCompleted);
        }
    }

    /**
     * Fail all deferred events with the provided exception.
     *
     * @param exception     The exception to fail the entries with.
     */
    public void failAll(Exception exception) {
        Iterator<Entry<Long, List<DeferredEvent>>> iter = pending.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, List<DeferredEvent>> entry = iter.next();
            for (DeferredEvent event : entry.getValue()) {
                log.info("failAll({}): failing {}.", exception.getClass().getSimpleName(), event);
                event.complete(exception);
            }
            iter.remove();
        }
    }

    /**
     * Add a new deferred event to be completed by the provided offset.
     *
     * @param offset        The offset to add the new event at.
     * @param event         The new event.
     */
    public void add(long offset, DeferredEvent event) {
        if (!pending.isEmpty()) {
            long lastKey = pending.lastKey();
            if (offset < lastKey) {
                throw new IllegalArgumentException("There is already a deferred event with " +
                    "offset " + lastKey + ". We should not add one with an offset of " +
                    offset + " which is lower than that.");
            }
        }
        List<DeferredEvent> events = pending.get(offset);
        if (events == null) {
            events = new ArrayList<>();
            pending.put(offset, events);
        }
        events.add(event);
        if (log.isTraceEnabled()) {
            log.trace("Adding deferred event {} at offset {}", event, offset);
        }
    }

    /**
     * Get the offset of the highest pending event, or empty if there are no pending
     * events.
     */
    public OptionalLong highestPendingOffset() {
        if (pending.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(pending.lastKey());
        }
    }
}
