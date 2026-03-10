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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.deferred.DeferredEvent;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of {@link DeferredEvent}. When completed, completes all the events in the collection
 * and logs any exceptions thrown.
 */
public class DeferredEventCollection implements DeferredEvent {
    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The list of events.
     */
    private final List<DeferredEvent> events = new ArrayList<>();

    public DeferredEventCollection(Logger log) {
        this.log = log;
    }

    @Override
    public void complete(Throwable t) {
        for (DeferredEvent event : events) {
            try {
                event.complete(t);
            } catch (Throwable e) {
                log.error("Completion of event {} failed due to {}.", event, e.getMessage(), e);
            }
        }
    }

    public boolean add(DeferredEvent event) {
        return events.add(event);
    }

    public int size() {
        return events.size();
    }

    @Override
    public String toString() {
        return "DeferredEventCollection(events=" + events + ")";
    }

    public static DeferredEventCollection of(Logger log, DeferredEvent... deferredEvents) {
        DeferredEventCollection collection = new DeferredEventCollection(log);
        for (DeferredEvent deferredEvent : deferredEvents) {
            collection.add(deferredEvent);
        }
        return collection;
    }
}
