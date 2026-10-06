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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

/**
 * The processes of a group, grouped by their values for the keys of {@code rack.aware.assignment.tags}, which the
 * rack-aware picks cannot tell apart. The least-loaded lookups assume that loads only grow and room only shrinks.
 *
 * @param <P> The assignor's process type.
 */
final class IdenticalTagGroups<P> {

    private final Collection<Group<P>> groups;
    private final Map<P, Group<P>> groupByProcess;

    /**
     * Groups order their processes by load, then in the order of {@code processes}, so that a pick breaks load ties
     * as a scan over all processes would.
     *
     * @param tagKeys    The keys of {@code rack.aware.assignment.tags}.
     * @param processes  All processes of the group.
     * @param clientTags The client tags of a process.
     * @param load       The load of a process.
     * @param hasRoom    Whether a process can take another task.
     */
    IdenticalTagGroups(
        final List<String> tagKeys,
        final Collection<P> processes,
        final Function<P, Map<String, String>> clientTags,
        final ToDoubleFunction<P> load,
        final Predicate<P> hasRoom
    ) {
        final Map<List<String>, Group<P>> groupsByTagValues = new LinkedHashMap<>();
        groupByProcess = Utils.newHashMap(processes.size());
        int order = 0;
        for (final P process : processes) {
            final Map<String, String> tags = clientTags.apply(process);
            final List<String> tagValues = new ArrayList<>(tagKeys.size());
            for (final String tagKey : tagKeys) {
                tagValues.add(tags.get(tagKey));
            }
            final Group<P> group = groupsByTagValues.computeIfAbsent(tagValues, values -> new Group<>(tags, load, hasRoom));
            group.processesByLoad.add(new QueuedProcess<>(process, order++, load.applyAsDouble(process)));
            groupByProcess.put(process, group);
        }
        groups = groupsByTagValues.values();
    }

    /** The groups, in the order of their first process. */
    Collection<Group<P>> groups() {
        return groups;
    }

    Group<P> groupOf(final P process) {
        return groupByProcess.get(process);
    }

    /** The least-loaded process with room of the candidate groups, which all have one. */
    static <P> P leastLoaded(final Collection<Group<P>> candidates) {
        QueuedProcess<P> leastLoaded = null;
        for (final Group<P> candidate : candidates) {
            final QueuedProcess<P> head = candidate.leastLoadedWithRoom();
            if (leastLoaded == null || QueuedProcess.ORDER.compare(head, leastLoaded) < 0) {
                leastLoaded = head;
            }
        }
        return leastLoaded.process;
    }

    /** Processes with the same values for the keys of {@code rack.aware.assignment.tags}. */
    static final class Group<P> {
        private final Map<String, String> clientTags;
        private final ToDoubleFunction<P> processLoad;
        private final Predicate<P> processHasRoom;
        // The processes of the group that may still have room, by the load each was queued with. Placing a task
        // leaves the queue alone: a process whose load has grown since is queued again once it reaches the head.
        private final PriorityQueue<QueuedProcess<P>> processesByLoad = new PriorityQueue<>(QueuedProcess.ORDER);

        private Group(final Map<String, String> clientTags, final ToDoubleFunction<P> processLoad, final Predicate<P> processHasRoom) {
            this.clientTags = clientTags;
            this.processLoad = processLoad;
            this.processHasRoom = processHasRoom;
        }

        Map<String, String> clientTags() {
            return clientTags;
        }

        boolean hasRoom() {
            return leastLoadedWithRoom() != null;
        }

        /**
         * The least-loaded process of the group with room, or null when none has room. Loads only grow, so the head is
         * the least loaded once the load it was queued with is current. One without room is dropped for good, since
         * room only shrinks.
         */
        private QueuedProcess<P> leastLoadedWithRoom() {
            while (!processesByLoad.isEmpty()) {
                final QueuedProcess<P> head = processesByLoad.peek();
                final double currentLoad = processLoad.applyAsDouble(head.process);
                if (head.load != currentLoad) {
                    processesByLoad.poll();
                    head.load = currentLoad;
                    processesByLoad.add(head);
                } else if (!processHasRoom.test(head.process)) {
                    processesByLoad.poll();
                } else {
                    return head;
                }
            }
            return null;
        }
    }

    /** A process in the queue of its group, with its position in the processes and the load it was queued with. */
    private static final class QueuedProcess<P> {
        private static final Comparator<QueuedProcess<?>> ORDER = (process1, process2) -> {
            final int byLoad = Double.compare(process1.load, process2.load);
            return byLoad != 0 ? byLoad : Integer.compare(process1.order, process2.order);
        };

        private final P process;
        private final int order;
        private double load;

        private QueuedProcess(final P process, final int order, final double load) {
            this.process = process;
            this.order = order;
            this.load = load;
        }
    }
}
