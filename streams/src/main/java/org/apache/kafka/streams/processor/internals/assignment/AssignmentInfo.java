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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.assignment.ConsumerProtocolUtils.readTaskIdFrom;
import static org.apache.kafka.streams.processor.internals.assignment.ConsumerProtocolUtils.writeTaskIdTo;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;

public class AssignmentInfo {
    private static final Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    private final int usedVersion;
    private final int commonlySupportedVersion;
    private List<TaskId> activeTasks;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHost;
    private Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost;
    private int errCode;
    private Long nextRebalanceMs = Long.MAX_VALUE;

    // used for decoding and "future consumer" assignments during version probing
    public AssignmentInfo(final int version,
                          final int commonlySupportedVersion) {
        this(version,
             commonlySupportedVersion,
             Collections.emptyList(),
             Collections.emptyMap(),
             Collections.emptyMap(),
             Collections.emptyMap(),
             0);
    }

    public AssignmentInfo(final int version,
                          final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                          final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                          final int errCode) {
        this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, partitionsByHost, standbyPartitionsByHost, errCode);
    }

    public AssignmentInfo(final int version,
                          final int commonlySupportedVersion,
                          final List<TaskId> activeTasks,
                          final Map<TaskId, Set<TopicPartition>> standbyTasks,
                          final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                          final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                          final int errCode) {
        this.usedVersion = version;
        this.commonlySupportedVersion = commonlySupportedVersion;
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.partitionsByHost = partitionsByHost;
        this.standbyPartitionsByHost = standbyPartitionsByHost;
        this.errCode = errCode;

        if (version < 1 || version > LATEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                + "; was: " + version);
        }
    }

    public void setNextRebalanceTime(final long nextRebalanceTimeMs) {
        this.nextRebalanceMs = nextRebalanceTimeMs;
    }

    public int version() {
        return usedVersion;
    }

    public int errCode() {
        return errCode;
    }

    public int commonlySupportedVersion() {
        return commonlySupportedVersion;
    }

    public List<TaskId> activeTasks() {
        return activeTasks;
    }

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }

    public Map<HostInfo, Set<TopicPartition>> partitionsByHost() {
        return partitionsByHost;
    }

    public Map<HostInfo, Set<TopicPartition>> standbyPartitionByHost() {
        return standbyPartitionsByHost;
    }

    public long nextRebalanceMs() {
        return nextRebalanceMs;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
     * IO exception during encoding
     */
    public ByteBuffer encode() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final DataOutputStream out = new DataOutputStream(baos)) {
            switch (usedVersion) {
                case 1:
                    out.writeInt(usedVersion); // version
                    encodeActiveAndStandbyTaskAssignment(out);
                    break;
                case 2:
                    out.writeInt(usedVersion); // version
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodePartitionsByHost(out);
                    break;
                case 3:
                    out.writeInt(usedVersion);
                    out.writeInt(commonlySupportedVersion);
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodePartitionsByHost(out);
                    break;
                case 4:
                    out.writeInt(usedVersion);
                    out.writeInt(commonlySupportedVersion);
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodePartitionsByHost(out);
                    out.writeInt(errCode);
                    break;
                case 5:
                    out.writeInt(usedVersion);
                    out.writeInt(commonlySupportedVersion);
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodePartitionsByHostAsDictionary(out);
                    out.writeInt(errCode);
                    break;
                case 6:
                    out.writeInt(usedVersion);
                    out.writeInt(commonlySupportedVersion);
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodeActiveAndStandbyHostPartitions(out);
                    out.writeInt(errCode);
                    break;
                case 7:
                case 8:
                case 9:
                case 10:
                case 11:
                    out.writeInt(usedVersion);
                    out.writeInt(commonlySupportedVersion);
                    encodeActiveAndStandbyTaskAssignment(out);
                    encodeActiveAndStandbyHostPartitions(out);
                    out.writeInt(errCode);
                    out.writeLong(nextRebalanceMs);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata version: " + usedVersion
                            + "; latest commonly supported version: " + commonlySupportedVersion);
            }

            out.flush();
            out.close();

            return ByteBuffer.wrap(baos.toByteArray());
        } catch (final IOException ex) {
            throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
        }
    }

    private void encodeActiveAndStandbyTaskAssignment(final DataOutputStream out) throws IOException {
        // encode active tasks
        out.writeInt(activeTasks.size());
        for (final TaskId id : activeTasks) {
            writeTaskIdTo(id, out, usedVersion);
        }

        // encode standby tasks
        out.writeInt(standbyTasks.size());
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
            final TaskId id = entry.getKey();
            writeTaskIdTo(id, out, usedVersion);

            final Set<TopicPartition> partitions = entry.getValue();
            writeTopicPartitions(out, partitions);
        }
    }

    private void encodePartitionsByHost(final DataOutputStream out) throws IOException {
        // encode partitions by host
        out.writeInt(partitionsByHost.size());
        for (final Map.Entry<HostInfo, Set<TopicPartition>> entry : partitionsByHost.entrySet()) {
            writeHostInfo(out, entry.getKey());
            writeTopicPartitions(out, entry.getValue());
        }
    }

    private void encodeHostPartitionMapUsingDictionary(final DataOutputStream out,
                                                       final Map<String, Integer> topicNameDict,
                                                       final Map<HostInfo, Set<TopicPartition>> hostPartitionMap) throws IOException {
        // encode partitions by host
        out.writeInt(hostPartitionMap.size());

        // Write the topic index, partition
        for (final Map.Entry<HostInfo, Set<TopicPartition>> entry : hostPartitionMap.entrySet()) {
            writeHostInfo(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (final TopicPartition partition : entry.getValue()) {
                out.writeInt(topicNameDict.get(partition.topic()));
                out.writeInt(partition.partition());
            }
        }
    }

    private Map<String, Integer> encodeTopicDictionaryAndGet(final DataOutputStream out,
                                                             final Set<TopicPartition> topicPartitions) throws IOException {
        // Build a dictionary to encode topicNames
        int topicIndex = 0;
        final Map<String, Integer> topicNameDict = new HashMap<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            if (!topicNameDict.containsKey(topicPartition.topic())) {
                topicNameDict.put(topicPartition.topic(), topicIndex++);
            }
        }

        // write the topic name dictionary out
        out.writeInt(topicNameDict.size());
        for (final Map.Entry<String, Integer> entry : topicNameDict.entrySet()) {
            out.writeInt(entry.getValue());
            out.writeUTF(entry.getKey());
        }

        return topicNameDict;
    }

    private void encodePartitionsByHostAsDictionary(final DataOutputStream out) throws IOException {
        final Set<TopicPartition> allTopicPartitions = partitionsByHost.values().stream()
            .flatMap(Collection::stream).collect(Collectors.toSet());
        final Map<String, Integer> topicNameDict = encodeTopicDictionaryAndGet(out, allTopicPartitions);
        encodeHostPartitionMapUsingDictionary(out, topicNameDict, partitionsByHost);
    }

    private void encodeActiveAndStandbyHostPartitions(final DataOutputStream out) throws IOException {
        final Set<TopicPartition> allTopicPartitions = Stream
            .concat(partitionsByHost.values().stream(), standbyPartitionsByHost.values().stream())
            .flatMap(Collection::stream).collect(Collectors.toSet());
        final Map<String, Integer> topicNameDict = encodeTopicDictionaryAndGet(out, allTopicPartitions);
        encodeHostPartitionMapUsingDictionary(out, topicNameDict, partitionsByHost);
        encodeHostPartitionMapUsingDictionary(out, topicNameDict, standbyPartitionsByHost);
    }

    private void writeHostInfo(final DataOutputStream out, final HostInfo hostInfo) throws IOException {
        out.writeUTF(hostInfo.host());
        out.writeInt(hostInfo.port());
    }

    private void writeTopicPartitions(final DataOutputStream out,
                                      final Set<TopicPartition> partitions) throws IOException {
        out.writeInt(partitions.size());
        for (final TopicPartition partition : partitions) {
            out.writeUTF(partition.topic());
            out.writeInt(partition.partition());
        }
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
     */
    public static AssignmentInfo decode(final ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        try (final DataInputStream in = new DataInputStream(new ByteBufferInputStream(data))) {
            final AssignmentInfo assignmentInfo;

            final int usedVersion = in.readInt();
            final int commonlySupportedVersion;
            switch (usedVersion) {
                case 1:
                    assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    assignmentInfo.partitionsByHost = new HashMap<>();
                    break;
                case 2:
                    assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodePartitionsByHost(assignmentInfo, in);
                    break;
                case 3:
                    commonlySupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodePartitionsByHost(assignmentInfo, in);
                    break;
                case 4:
                    commonlySupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodePartitionsByHost(assignmentInfo, in);
                    assignmentInfo.errCode = in.readInt();
                    break;
                case 5:
                    commonlySupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodePartitionsByHostUsingDictionary(assignmentInfo, in);
                    assignmentInfo.errCode = in.readInt();
                    break;
                case 6:
                    commonlySupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodeActiveAndStandbyHostPartitions(assignmentInfo, in);
                    assignmentInfo.errCode = in.readInt();
                    break;
                case 7:
                case 8:
                case 9:
                case 10:
                case 11:
                    commonlySupportedVersion = in.readInt();
                    assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                    decodeActiveTasks(assignmentInfo, in);
                    decodeStandbyTasks(assignmentInfo, in);
                    decodeActiveAndStandbyHostPartitions(assignmentInfo, in);
                    assignmentInfo.errCode = in.readInt();
                    assignmentInfo.nextRebalanceMs = in.readLong();
                    break;
                default:
                    final TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode assignment data: " +
                        "used version: " + usedVersion + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
                    log.error(fatalException.getMessage(), fatalException);
                    throw fatalException;
            }

            return assignmentInfo;
        } catch (final IOException ex) {
            throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
        }
    }

    private static void decodeActiveTasks(final AssignmentInfo assignmentInfo,
                                          final DataInputStream in) throws IOException {
        final int count = in.readInt();
        assignmentInfo.activeTasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            assignmentInfo.activeTasks.add(readTaskIdFrom(in, assignmentInfo.usedVersion));
        }
    }

    private static void decodeStandbyTasks(final AssignmentInfo assignmentInfo,
                                           final DataInputStream in) throws IOException {
        final int count = in.readInt();
        assignmentInfo.standbyTasks = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            final TaskId id = readTaskIdFrom(in, assignmentInfo.usedVersion);
            assignmentInfo.standbyTasks.put(id, readTopicPartitions(in));
        }
    }

    private static void decodePartitionsByHost(final AssignmentInfo assignmentInfo,
                                               final DataInputStream in) throws IOException {
        assignmentInfo.partitionsByHost = new HashMap<>();
        final int numEntries = in.readInt();
        for (int i = 0; i < numEntries; i++) {
            final HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
            assignmentInfo.partitionsByHost.put(hostInfo, readTopicPartitions(in));
        }
    }

    private static Set<TopicPartition> readTopicPartitions(final DataInputStream in) throws IOException {
        final int numPartitions = in.readInt();
        final Set<TopicPartition> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++) {
            partitions.add(new TopicPartition(in.readUTF(), in.readInt()));
        }
        return partitions;
    }

    private static Map<Integer, String> decodeTopicIndexAndGet(final DataInputStream in) throws IOException {
        final int dictSize = in.readInt();
        final Map<Integer, String> topicIndexDict = new HashMap<>(dictSize);
        for (int i = 0; i < dictSize; i++) {
            topicIndexDict.put(in.readInt(), in.readUTF());
        }
        return topicIndexDict;
    }

    private static Map<HostInfo, Set<TopicPartition>> decodeHostPartitionMapUsingDictionary(final DataInputStream in,
                                                                                            final Map<Integer, String> topicIndexDict) throws IOException {
        final Map<HostInfo, Set<TopicPartition>> hostPartitionMap = new HashMap<>();
        final int numEntries = in.readInt();
        for (int i = 0; i < numEntries; i++) {
            final HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
            hostPartitionMap.put(hostInfo, readTopicPartitions(in, topicIndexDict));
        }
        return hostPartitionMap;
    }

    private static void decodePartitionsByHostUsingDictionary(final AssignmentInfo assignmentInfo,
                                                              final DataInputStream in) throws IOException {
        final Map<Integer, String> topicIndexDict = decodeTopicIndexAndGet(in);
        assignmentInfo.partitionsByHost = decodeHostPartitionMapUsingDictionary(in, topicIndexDict);
    }

    private static void decodeActiveAndStandbyHostPartitions(final AssignmentInfo assignmentInfo,
                                                             final DataInputStream in) throws IOException {
        final Map<Integer, String> topicIndexDict = decodeTopicIndexAndGet(in);
        assignmentInfo.partitionsByHost = decodeHostPartitionMapUsingDictionary(in, topicIndexDict);
        assignmentInfo.standbyPartitionsByHost = decodeHostPartitionMapUsingDictionary(in, topicIndexDict);
    }

    private static Set<TopicPartition> readTopicPartitions(final DataInputStream in,
                                                           final Map<Integer, String> topicIndexDict) throws IOException {
        final int numPartitions = in.readInt();
        final Set<TopicPartition> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++) {
            partitions.add(new TopicPartition(topicIndexDict.get(in.readInt()), in.readInt()));
        }
        return partitions;
    }

    @Override
    public int hashCode() {
        final int hostMapHashCode = partitionsByHost.hashCode() ^ standbyPartitionsByHost.hashCode();
        return usedVersion ^ commonlySupportedVersion ^ activeTasks.hashCode() ^ standbyTasks.hashCode()
                ^ hostMapHashCode ^ errCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof AssignmentInfo) {
            final AssignmentInfo other = (AssignmentInfo) o;
            return usedVersion == other.usedVersion &&
                   commonlySupportedVersion == other.commonlySupportedVersion &&
                   errCode == other.errCode &&
                   activeTasks.equals(other.activeTasks) &&
                   standbyTasks.equals(other.standbyTasks) &&
                   partitionsByHost.equals(other.partitionsByHost) &&
                   standbyPartitionsByHost.equals(other.standbyPartitionsByHost);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + usedVersion
            + ", supported version=" + commonlySupportedVersion
            + ", active tasks=" + activeTasks
            + ", standby tasks=" + standbyTasks
            + ", partitions by host=" + partitionsByHost
            + ", standbyPartitions by host=" + standbyPartitionsByHost
            + "]";
    }
}
