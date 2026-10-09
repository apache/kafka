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
package org.apache.kafka.jmh.assignor;

import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.MemberTaskOffsets;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.assignor.TaskId;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


public class StreamsAssignorBenchmarkUtils {

    /**
     * The end offset every reported changelog is at. Far enough from zero that a lagging offset stays positive for
     * any realistic acceptable recovery lag.
     */
    private static final long CHANGELOG_END_OFFSET = 1L << 40;

    /**
     * What the members report about the state they restore, which is what the refiner's decisions hinge on. A
     * member reports offsets for its standby and warm-up tasks, and for an active task only while it is still
     * restoring it.
     */
    public enum LagPicture {
        /** Every standby and warm-up task is within the acceptable recovery lag. */
        ALL_CAUGHT_UP,
        /** Every standby and warm-up task has restored part of its state, but lags beyond the acceptable recovery lag. */
        ALL_LAGGING,
        /** No member reports any offsets, as right after a coordinator failover. */
        NOT_REPORTED,
        /** Standby and warm-up tasks take turns being caught up, lagging, and not reported. */
        MIXED,
        /**
         * Like ALL_LAGGING, and every member that a task migrates to is also restoring one of its own active tasks,
         * which stalls every warm-up task on that member. Only a member that keeps a stateful active task of its target
         * can report one as restoring, so where each member has a single active task, as with 1000 members and 10
         * subtopologies of 100 partitions, this is the same as ALL_LAGGING.
         */
        RESTORING_DESTINATIONS
    }

    /**
     * The turns the copies of a MIXED lag picture take, in order.
     */
    private static final LagPicture[] MIXED_TURNS = {
        LagPicture.ALL_CAUGHT_UP, LagPicture.ALL_LAGGING, LagPicture.NOT_REPORTED
    };

    /**
     * Creates a GroupSpec from the given StreamsGroupMembers.
     *
     * @param members               The StreamsGroupMembers.
     * @param assignmentConfigs     The assignment configs.
     * @param taskOffsets           The reported offset sums per member, empty for members reporting none.
     *
     * @return The new GroupSpec.
     */
    public static GroupSpec createGroupSpec(
        Map<String, StreamsGroupMember> members,
        AssignmentConfigs assignmentConfigs,
        Map<String, Map<String, Map<Integer, Long>>> taskOffsets
    ) {
        Map<String, MemberMetadataAndStateImpl> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        for (Map.Entry<String, StreamsGroupMember> memberEntry : members.entrySet()) {
            String memberId = memberEntry.getKey();
            StreamsGroupMember member = memberEntry.getValue();

            memberSpecs.put(memberId, new MemberMetadataAndStateImpl(
                member.instanceId(),
                member.rackId(),
                member.processId(),
                member.clientTags(),
                Map.of(),
                Map.of(),
                Map.of(),
                taskOffsets.getOrDefault(memberId, Map.of()),
                Map.of()
            ));
        }

        return new GroupSpecImpl(
            memberSpecs,
            assignmentConfigs
        );
    }

    /**
     * Creates the offset sums the members report for the state they hold on local disk.
     *
     * Tasks of stateful subtopologies are spread round-robin over the members, so every member reports the tasks
     * it would have owned in an earlier generation. Each dormant replica makes a following member report the same
     * task with a lower offset sum, so several members compete as candidates for it and the candidate ranking
     * actually has something to sort.
     *
     * @param memberIds         The members to spread the tasks over, in a stable order.
     * @param subtopologyMap    The subtopologies; only the stateful ones get offsets.
     * @param dormantReplicas   The number of members reporting a task on top of the one owning it.
     *
     * @return The reported offset sums, per member.
     */
    public static Map<String, Map<String, Map<Integer, Long>>> createTaskOffsets(
        List<String> memberIds,
        SortedMap<String, ConfiguredSubtopology> subtopologyMap,
        int dormantReplicas
    ) {
        Map<String, Map<String, Map<Integer, Long>>> taskOffsets = new HashMap<>();

        int taskIndex = 0;
        for (Map.Entry<String, ConfiguredSubtopology> subtopologyEntry : subtopologyMap.entrySet()) {
            ConfiguredSubtopology subtopology = subtopologyEntry.getValue();
            if (subtopology.stateChangelogTopics().isEmpty()) {
                continue;
            }

            for (int partition = 0; partition < subtopology.numberOfTasks(); partition++) {
                for (int replica = 0; replica <= dormantReplicas; replica++) {
                    String memberId = memberIds.get((taskIndex + replica) % memberIds.size());
                    // The owner holds the most caught-up state, every dormant copy a little less.
                    long offsetSum = (long) (dormantReplicas + 1 - replica) * 1_000_000L + taskIndex;

                    taskOffsets
                        .computeIfAbsent(memberId, id -> new HashMap<>())
                        .computeIfAbsent(subtopologyEntry.getKey(), id -> new HashMap<>())
                        .put(partition, offsetSum);
                }
                taskIndex++;
            }
        }

        return taskOffsets;
    }

    /**
     * Creates a StreamsGroupMembers map where all members have the same topic subscriptions.
     *
     * @param memberCount           The number of members in the group.
     * @param membersPerProcess     The number of members per process.
     *
     * @return The new StreamsGroupMembers map.
     */
    public static Map<String, StreamsGroupMember> createStreamsMembers(
        int memberCount,
        int membersPerProcess
    ) {
        Map<String, StreamsGroupMember> members = new HashMap<>();

        for (int i = 0; i < memberCount; i++) {
            String memberId = "member-" + i;
            String processId = "process-" + i / membersPerProcess;

            members.put(memberId, StreamsGroupMember.Builder.withDefaults(memberId)
                    .setProcessId(processId)
                    .build());
        }

        return members;
    }

    /**
     * Creates a subtopology map with the given number of partitions per topic and a list of topic names.
     * For simplicity, each subtopology is associated with a single topic, and every second subtopology
     * is stateful (i.e., has a changelog topic).
     *
     * The number of topics a subtopology is associated with is irrelevant, and
     * so is the number of changelog topics.
     *
     * @param partitionsPerTopic The number of partitions per topic, implies the number of tasks for the subtopology.
     * @param allTopicNames All topics names.
     * @return A sorted map of subtopology IDs to ConfiguredSubtopology objects.
     */
    public static SortedMap<String, ConfiguredSubtopology> createSubtopologyMap(
        int partitionsPerTopic,
        List<String> allTopicNames
    ) {
        TreeMap<String, ConfiguredSubtopology> subtopologyMap = new TreeMap<>();
        for (int i = 0; i < allTopicNames.size(); i++) {
            String topicName = allTopicNames.get(i);
            if (i % 2 == 0) {
                subtopologyMap.put(topicName + "_subtopology", new ConfiguredSubtopology(partitionsPerTopic, Set.of(topicName), Map.of(), Set.of(), Map.of(
                    topicName + "_changelog", new ConfiguredInternalTopic(
                        topicName + "_changelog",
                        partitionsPerTopic,
                        Optional.empty(),
                        Map.of()
                    )
                )));
            } else {
                subtopologyMap.put(topicName + "_subtopology", new ConfiguredSubtopology(partitionsPerTopic, Set.of(topicName), Map.of(), Set.of(), Map.of()));
            }
        }
        return subtopologyMap;
    }

    /**
     * Creates the members' current assignment as the target assignment with a fraction of the stateful active tasks
     * still running where they were before the target assignment moved them, which is the work the refiner has to
     * do. A group whose current assignment equals its target leaves the refiner nothing to decide.
     * <p>
     * Every chosen task runs on a member of another process than its target owner, preferring one that holds no copy
     * of the task, round-robin over that process's members. When every other process holds a standby of the task,
     * the active and one standby swap places instead, so that the target owner holds the standby. The chosen tasks
     * are spread evenly over the stateful tasks in task order. The target's standby tasks are kept otherwise.
     * <p>
     * A refinement step after the first one finds the warm-up budget spent on migrations already under way, so
     * inFlightWarmups of the migrating tasks whose target owner holds nothing for them get a warm-up task on it,
     * spread evenly over those tasks.
     *
     * @param members               The members, which must span at least two processes if any task migrates.
     * @param targetAssignment      The target assignment, keyed by member ID.
     * @param subtopologyMap        The subtopologies; only the tasks of stateful ones migrate.
     * @param migratingTaskFraction The fraction of the stateful active tasks not on their target owner yet.
     * @param inFlightWarmups       The number of migrating tasks already warming up on their target owner.
     *
     * @return The members, with their current assignment set.
     */
    public static Map<String, StreamsGroupMember> divergeAssignment(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        SortedMap<String, ConfiguredSubtopology> subtopologyMap,
        double migratingTaskFraction,
        int inFlightWarmups
    ) {
        Map<String, Map<String, Map<Integer, Integer>>> activeTasks = new HashMap<>();
        Map<String, Map<String, Set<Integer>>> standbyTasks = new HashMap<>();
        Map<String, Map<String, Set<Integer>>> warmupTasks = new HashMap<>();
        TreeMap<TaskId, String> statefulActiveOwners = new TreeMap<>();
        Map<TaskId, SortedSet<String>> standbyHolders = new HashMap<>();

        for (Map.Entry<String, TasksTuple> entry : targetAssignment.entrySet()) {
            String memberId = entry.getKey();
            int memberEpoch = members.get(memberId).memberEpoch();
            entry.getValue().activeTasks().forEach((subtopologyId, partitions) -> partitions.forEach(partition -> {
                addActive(activeTasks, memberId, new TaskId(subtopologyId, partition), memberEpoch);
                if (isStateful(subtopologyMap, subtopologyId)) {
                    statefulActiveOwners.put(new TaskId(subtopologyId, partition), memberId);
                }
            }));
            entry.getValue().standbyTasks().forEach((subtopologyId, partitions) -> partitions.forEach(partition -> {
                addTask(standbyTasks, memberId, new TaskId(subtopologyId, partition));
                standbyHolders.computeIfAbsent(new TaskId(subtopologyId, partition), id -> new TreeSet<>()).add(memberId);
            }));
        }

        List<String> processIds = new ArrayList<>();
        Map<String, List<String>> membersByProcess = new TreeMap<>();
        members.values().stream().sorted(Comparator.comparing(StreamsGroupMember::memberId)).forEach(member ->
            membersByProcess.computeIfAbsent(member.processId(), id -> new ArrayList<>()).add(member.memberId()));
        processIds.addAll(membersByProcess.keySet());
        if (migratingTaskFraction > 0 && processIds.size() < 2) {
            throw new IllegalArgumentException("A task can only migrate between two processes, but the members run in "
                + processIds.size() + ".");
        }
        Map<String, Integer> nextMemberOfProcess = new HashMap<>();

        List<Map.Entry<TaskId, String>> warmupCandidates = new ArrayList<>();
        int taskIndex = 0;
        for (Map.Entry<TaskId, String> entry : statefulActiveOwners.entrySet()) {
            boolean migrating = Math.floor((taskIndex + 1) * migratingTaskFraction) > Math.floor(taskIndex * migratingTaskFraction);
            taskIndex++;
            if (!migrating) {
                continue;
            }

            TaskId task = entry.getKey();
            String targetOwner = entry.getValue();
            int epoch = activeTasks.get(targetOwner).get(task.subtopologyId()).get(task.partition());
            removeActive(activeTasks, targetOwner, task);

            Optional<String> freeProcess = processWithoutCopy(members, processIds, targetOwner, standbyHolders.get(task));
            if (freeProcess.isPresent()) {
                List<String> processMembers = membersByProcess.get(freeProcess.get());
                int next = nextMemberOfProcess.merge(freeProcess.get(), 1, Integer::sum) - 1;
                addActive(activeTasks, processMembers.get(next % processMembers.size()), task, epoch);
                warmupCandidates.add(entry);
            } else {
                String standbyHolder = standbyHolders.get(task).first();
                removeTask(standbyTasks, standbyHolder, task);
                addActive(activeTasks, standbyHolder, task, epoch);
                addTask(standbyTasks, targetOwner, task);
            }
        }

        int warmups = Math.min(inFlightWarmups, warmupCandidates.size());
        for (int warmup = 0; warmup < warmups; warmup++) {
            Map.Entry<TaskId, String> candidate =
                warmupCandidates.get((int) ((long) warmup * warmupCandidates.size() / warmups));
            addTask(warmupTasks, candidate.getValue(), candidate.getKey());
        }

        Map<String, StreamsGroupMember> divergedMembers = new HashMap<>();
        for (StreamsGroupMember member : members.values()) {
            String memberId = member.memberId();
            divergedMembers.put(memberId, new StreamsGroupMember.Builder(member)
                .setAssignedTasks(new TasksTupleWithEpochs(
                    activeTasks.getOrDefault(memberId, Map.of()),
                    standbyTasks.getOrDefault(memberId, Map.of()),
                    warmupTasks.getOrDefault(memberId, Map.of())
                ))
                .build());
        }
        return divergedMembers;
    }

    /**
     * The first process after the target owner's, in process ID order, that holds no standby of the task.
     */
    private static Optional<String> processWithoutCopy(
        Map<String, StreamsGroupMember> members,
        List<String> processIds,
        String targetOwner,
        Set<String> standbyHolders
    ) {
        Set<String> copyProcesses = new HashSet<>();
        if (standbyHolders != null) {
            standbyHolders.forEach(holder -> copyProcesses.add(members.get(holder).processId()));
        }

        int targetProcessIndex = processIds.indexOf(members.get(targetOwner).processId());
        for (int offset = 1; offset < processIds.size(); offset++) {
            String processId = processIds.get((targetProcessIndex + offset) % processIds.size());
            if (!copyProcesses.contains(processId)) {
                return Optional.of(processId);
            }
        }
        return Optional.empty();
    }

    /**
     * Creates the changelog offsets and end offsets the members report in their heartbeats, from which the refiner
     * tells whether a copy of a task is caught up.
     * <p>
     * Every member reports the stateful standby and warm-up tasks of its current assignment, lagging or caught up as
     * the lag picture says. Under RESTORING_DESTINATIONS, every member that the target assignment moves a stateful
     * active task to also reports its first stateful active task that stays with it, which reads as restoring.
     *
     * @param members               The members, with their current assignment set.
     * @param targetAssignment      The target assignment, keyed by member ID.
     * @param subtopologyMap        The subtopologies; only the tasks of stateful ones are reported.
     * @param lagPicture            What the members report.
     * @param acceptableRecoveryLag The lag at or below which a task counts as caught up.
     *
     * @return The reported offsets, per member.
     */
    public static Map<String, MemberTaskOffsets> createMemberTaskOffsets(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        SortedMap<String, ConfiguredSubtopology> subtopologyMap,
        LagPicture lagPicture,
        long acceptableRecoveryLag
    ) {
        if (acceptableRecoveryLag < 0 || acceptableRecoveryLag > CHANGELOG_END_OFFSET / 4) {
            throw new IllegalArgumentException("Unsupported acceptable recovery lag " + acceptableRecoveryLag + ".");
        }
        long caughtUpOffset = CHANGELOG_END_OFFSET - acceptableRecoveryLag;
        long laggingOffset = CHANGELOG_END_OFFSET - 2 * acceptableRecoveryLag - 1;

        Map<String, MemberTaskOffsets> memberTaskOffsets = new HashMap<>();
        int copyIndex = 0;
        List<String> memberIds = new ArrayList<>(members.keySet());
        memberIds.sort(null);
        for (String memberId : memberIds) {
            if (lagPicture == LagPicture.NOT_REPORTED) {
                continue;
            }

            TasksTupleWithEpochs assignedTasks = members.get(memberId).assignedTasks();
            Map<String, Map<Integer, Long>> taskOffsets = new HashMap<>();
            Map<String, Map<Integer, Long>> taskEndOffsets = new HashMap<>();

            SortedSet<TaskId> copies = statefulTasks(assignedTasks.standbyTasks(), subtopologyMap);
            copies.addAll(statefulTasks(assignedTasks.warmupTasks(), subtopologyMap));
            for (TaskId task : copies) {
                LagPicture reported = lagPicture == LagPicture.MIXED ? MIXED_TURNS[copyIndex++ % MIXED_TURNS.length] : lagPicture;
                if (reported != LagPicture.NOT_REPORTED) {
                    reportOffsets(taskOffsets, taskEndOffsets, task,
                        reported == LagPicture.ALL_CAUGHT_UP ? caughtUpOffset : laggingOffset);
                }
            }

            if (lagPicture == LagPicture.RESTORING_DESTINATIONS) {
                SortedSet<TaskId> currentActives = statefulTasks(assignedTasks.activeTasks(), subtopologyMap);
                SortedSet<TaskId> targetActives = statefulTasks(
                    targetAssignment.getOrDefault(memberId, TasksTuple.EMPTY).activeTasks(), subtopologyMap);
                boolean destination = !currentActives.containsAll(targetActives);
                currentActives.retainAll(targetActives);
                if (destination && !currentActives.isEmpty()) {
                    reportOffsets(taskOffsets, taskEndOffsets, currentActives.first(), laggingOffset);
                }
            }

            memberTaskOffsets.put(memberId, new MemberTaskOffsets(taskOffsets, taskEndOffsets));
        }

        return memberTaskOffsets;
    }

    private static void reportOffsets(
        Map<String, Map<Integer, Long>> taskOffsets,
        Map<String, Map<Integer, Long>> taskEndOffsets,
        TaskId task,
        long offset
    ) {
        taskOffsets.computeIfAbsent(task.subtopologyId(), id -> new HashMap<>()).put(task.partition(), offset);
        taskEndOffsets.computeIfAbsent(task.subtopologyId(), id -> new HashMap<>()).put(task.partition(), CHANGELOG_END_OFFSET);
    }

    private static SortedSet<TaskId> statefulTasks(
        Map<String, Set<Integer>> tasks,
        SortedMap<String, ConfiguredSubtopology> subtopologyMap
    ) {
        SortedSet<TaskId> statefulTasks = new TreeSet<>();
        tasks.forEach((subtopologyId, partitions) -> {
            if (isStateful(subtopologyMap, subtopologyId)) {
                partitions.forEach(partition -> statefulTasks.add(new TaskId(subtopologyId, partition)));
            }
        });
        return statefulTasks;
    }

    private static boolean isStateful(SortedMap<String, ConfiguredSubtopology> subtopologyMap, String subtopologyId) {
        ConfiguredSubtopology subtopology = subtopologyMap.get(subtopologyId);
        return subtopology != null && !subtopology.stateChangelogTopics().isEmpty();
    }

    private static void addActive(
        Map<String, Map<String, Map<Integer, Integer>>> activeTasks,
        String memberId,
        TaskId task,
        int epoch
    ) {
        activeTasks.computeIfAbsent(memberId, id -> new HashMap<>())
            .computeIfAbsent(task.subtopologyId(), id -> new HashMap<>())
            .put(task.partition(), epoch);
    }

    private static void removeActive(
        Map<String, Map<String, Map<Integer, Integer>>> activeTasks,
        String memberId,
        TaskId task
    ) {
        Map<String, Map<Integer, Integer>> memberTasks = activeTasks.get(memberId);
        Map<Integer, Integer> partitions = memberTasks.get(task.subtopologyId());
        partitions.remove(task.partition());
        if (partitions.isEmpty()) {
            memberTasks.remove(task.subtopologyId());
        }
    }

    private static void addTask(Map<String, Map<String, Set<Integer>>> tasks, String memberId, TaskId task) {
        tasks.computeIfAbsent(memberId, id -> new HashMap<>())
            .computeIfAbsent(task.subtopologyId(), id -> new HashSet<>())
            .add(task.partition());
    }

    private static void removeTask(Map<String, Map<String, Set<Integer>>> tasks, String memberId, TaskId task) {
        Map<String, Set<Integer>> memberTasks = tasks.get(memberId);
        Set<Integer> partitions = memberTasks.get(task.subtopologyId());
        partitions.remove(task.partition());
        if (partitions.isEmpty()) {
            memberTasks.remove(task.subtopologyId());
        }
    }
}
