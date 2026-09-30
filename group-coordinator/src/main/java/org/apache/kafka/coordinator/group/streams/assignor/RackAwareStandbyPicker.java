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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Picks the processes for the standbys of one task over the keys of {@code rack.aware.assignment.tags}, whose list
 * order is the priority. A standby goes to an eligible process whose value for the highest-priority key is not yet
 * carried by a holder of the task; among those, the one with the lexicographically largest diversity vector over the
 * lower-priority keys wins (one bit per key, set where its value is new to the task), then the assignor's tie-break
 * decides. When no eligible process has a new value for that key, the key is given up and the search repeats with the
 * next key in priority order. Once every key is given up no process can make the task more diverse, so the assignor's
 * tag-blind pass places the remaining standbys.
 * <p>
 * For each task: {@link #startTask()}, {@link #markUsed(Object)} for the active owner, then per standby
 * {@link #pickCandidates(Predicate)} followed by the assignor's own choice, and {@link #markUsed(Object)} for the
 * winner.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    private record TaggedProcess<P>(P process, Map<String, String> clientTags) {
    }

    private final List<String> tagKeys;
    private final Function<P, Map<String, String>> clientTags;
    private final List<TaggedProcess<P>> allProcesses;

    // State of the task being placed, reset by startTask.
    private final List<Set<String>> usedTagValues;       // per key, the values already carried by a holder of the task
    private int priorityIndex;                           // position in tagKeys of the key the filter enforces
    private List<TaggedProcess<P>> candidates;           // the pool the next pick filters

    /**
     * @param tagKeys    The keys of {@code rack.aware.assignment.tags}, highest priority first.
     * @param processes  All processes of the group.
     * @param clientTags The client tags of a process.
     */
    RackAwareStandbyPicker(
        final List<String> tagKeys,
        final Collection<P> processes,
        final Function<P, Map<String, String>> clientTags
    ) {
        this.tagKeys = tagKeys;
        this.clientTags = clientTags;
        allProcesses = new ArrayList<>(processes.size());
        for (final P process : processes) {
            allProcesses.add(new TaggedProcess<>(process, clientTags.apply(process)));
        }
        usedTagValues = new ArrayList<>(tagKeys.size());
        for (int i = 0; i < tagKeys.size(); i++) {
            usedTagValues.add(new HashSet<>());
        }
        candidates = allProcesses;
    }

    void startTask() {
        for (final Set<String> values : usedTagValues) {
            values.clear();
        }
        priorityIndex = 0;
        candidates = allProcesses;
    }

    /** Records the tag values of a holder of the task, so that no later standby lands on them while a new value exists. */
    void markUsed(final P holder) {
        final Map<String, String> holderTags = clientTags.apply(holder);
        for (int i = 0; i < tagKeys.size(); i++) {
            final String value = holderTags.get(tagKeys.get(i));
            if (value != null) {
                usedTagValues.get(i).add(value);
            }
        }
    }

    /**
     * Returns the processes that make the task most diverse for the next standby, in group order, or an empty set
     * once no process can make it more diverse. The assignor chooses among them with its own rules and then calls
     * {@link #markUsed(Object)} for the one it took.
     *
     * @param eligible Whether a process may take the standby: not a holder of the task, plus the assignor's own rules.
     */
    Set<P> pickCandidates(final Predicate<P> eligible) {
        while (priorityIndex < tagKeys.size()) {
            final String priorityKey = tagKeys.get(priorityIndex);
            final Set<String> usedPriorityValues = usedTagValues.get(priorityIndex);

            final List<TaggedProcess<P>> survivors = new ArrayList<>();
            for (final TaggedProcess<P> candidate : candidates) {
                final String value = candidate.clientTags.get(priorityKey);
                if (value != null && !usedPriorityValues.contains(value) && eligible.test(candidate.process)) {
                    survivors.add(candidate);
                }
            }

            if (survivors.isEmpty()) {
                // Give up the key: it can no longer be diversified, so the next key becomes the priority.
                priorityIndex++;
                candidates = allProcesses;
                continue;
            }

            // A process dropped by the filter stays out while the key is the priority: usedTagValues only grows and
            // a holder stays a holder, so the survivors are the pool for the next standby. The one taken now drops
            // out through the eligibility test once it holds the task.
            candidates = survivors;
            return mostDiverse(survivors);
        }
        return Set.of();
    }

    private Set<P> mostDiverse(final List<TaggedProcess<P>> survivors) {
        final Set<P> best = new LinkedHashSet<>();
        TaggedProcess<P> reference = survivors.get(0);
        best.add(reference.process);
        for (int i = 1; i < survivors.size(); i++) {
            final TaggedProcess<P> candidate = survivors.get(i);
            final int comparison = compareDiversity(reference, candidate);
            if (comparison > 0) {
                best.clear();
                reference = candidate;
                best.add(candidate.process);
            } else if (comparison == 0) {
                best.add(candidate.process);
            }
        }
        return best;
    }

    /**
     * Compares the diversity vectors of two survivors, one bit per key in priority order: 1 where the process carries
     * a value for the key that no holder carries yet. Both share the bits up to the priority key, so the more diverse
     * process orders first.
     */
    private int compareDiversity(final TaggedProcess<P> process1, final TaggedProcess<P> process2) {
        for (int i = priorityIndex + 1; i < tagKeys.size(); i++) {
            final int comparison = Boolean.compare(hasUnusedValue(process2, i), hasUnusedValue(process1, i));
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean hasUnusedValue(final TaggedProcess<P> process, final int keyIndex) {
        final String value = process.clientTags.get(tagKeys.get(keyIndex));
        return value != null && !usedTagValues.get(keyIndex).contains(value);
    }
}
