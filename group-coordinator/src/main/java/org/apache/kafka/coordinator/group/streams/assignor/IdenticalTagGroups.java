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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

/**
 * Splits the processes of a streams group into tag groups by their values for {@code rack.aware.assignment.tags}.
 * The least-loaded lookups assume that loads only grow and room only shrinks.
 *
 * @param <P> The assignor's process type.
 */
final class IdenticalTagGroups<P> {

    private final Collection<TagGroup<P>> tagGroups;
    private final Map<P, TagGroup<P>> tagGroupByProcess;

    /**
     * @param tagKeys    The keys of {@code rack.aware.assignment.tags}.
     * @param processes  All processes of the streams group; an earlier one wins a load tie.
     * @param clientTags The client tags of a process.
     * @param load       The load of a process.
     * @param hasRoom    Whether a process can take another task; {@code process -> true} without a limit.
     */
    IdenticalTagGroups(
        final List<String> tagKeys,
        final Collection<P> processes,
        final Function<P, Map<String, String>> clientTags,
        final ToDoubleFunction<P> load,
        final Predicate<P> hasRoom
    ) {
        final Map<List<String>, TagGroup<P>> tagGroupsByTagValues = new HashMap<>();
        tagGroupByProcess = Utils.newHashMap(processes.size());
        int processIndex = 0;
        for (final P process : processes) {
            final Map<String, String> tags = clientTags.apply(process);
            final List<String> tagValues = new ArrayList<>(tagKeys.size());
            for (final String tagKey : tagKeys) {
                tagValues.add(tags.get(tagKey));
            }
            final TagGroup<P> tagGroup =
                tagGroupsByTagValues.computeIfAbsent(tagValues, values -> new TagGroup<>(tags, load, hasRoom));
            tagGroup.processesByLoad.add(new QueuedProcess<>(process, processIndex++, load.applyAsDouble(process)));
            tagGroupByProcess.put(process, tagGroup);
        }
        tagGroups = tagGroupsByTagValues.values();
    }

    Collection<TagGroup<P>> tagGroups() {
        return tagGroups;
    }

    /** The tag group of a process, valid even when loads drop. */
    TagGroup<P> tagGroupOf(final P process) {
        return tagGroupByProcess.get(process);
    }

    /** The least-loaded process with room of the candidate tag groups, which all have one. */
    static <P> P leastLoaded(final Collection<TagGroup<P>> candidates) {
        QueuedProcess<P> leastLoaded = null;
        for (final TagGroup<P> candidate : candidates) {
            final QueuedProcess<P> head = candidate.leastLoadedWithRoom();
            if (leastLoaded == null || QueuedProcess.ORDER.compare(head, leastLoaded) < 0) {
                leastLoaded = head;
            }
        }
        return leastLoaded.process;
    }

    /** Processes with the same values for {@code rack.aware.assignment.tags}. */
    static final class TagGroup<P> {
        private final Map<String, String> clientTags;
        private final ToDoubleFunction<P> processLoad;
        private final Predicate<P> processHasRoom;
        // The processes that may still have room, by the load each was queued with.
        private final PriorityQueue<QueuedProcess<P>> processesByLoad = new PriorityQueue<>(QueuedProcess.ORDER);

        private TagGroup(final Map<String, String> clientTags, final ToDoubleFunction<P> processLoad, final Predicate<P> processHasRoom) {
            this.clientTags = clientTags;
            this.processLoad = processLoad;
            this.processHasRoom = processHasRoom;
        }

        /** The client tags of the first process: only the values for {@code rack.aware.assignment.tags} are shared by the tag group. */
        Map<String, String> clientTags() {
            return clientTags;
        }

        boolean hasRoom() {
            return leastLoadedWithRoom() != null;
        }

        /** The least-loaded process with room, or null if none. A stale head is queued again, one without room dropped. */
        QueuedProcess<P> leastLoadedWithRoom() {
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

    /** A process with its index in {@code processes} and the load it was queued with. */
    static final class QueuedProcess<P> {
        static final Comparator<QueuedProcess<?>> ORDER = (process1, process2) -> {
            final int byLoad = Double.compare(process1.load, process2.load);
            return byLoad != 0 ? byLoad : Integer.compare(process1.processIndex, process2.processIndex);
        };

        final P process;
        final int processIndex;
        double load;

        private QueuedProcess(final P process, final int processIndex, final double load) {
            this.process = process;
            this.processIndex = processIndex;
            this.load = load;
        }
    }
}
