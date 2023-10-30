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
package org.apache.kafka.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DirectoryId extends Uuid {

    /**
     * A DirectoryId that is used to identify new or unknown dir assignments.
     */
    public static final DirectoryId UNASSIGNED = new DirectoryId(0L, 0L);

    /**
     * A DirectoryId that is used to represent unspecified offline dirs.
     */
    public static final DirectoryId LOST = new DirectoryId(0L, 1L);

    /**
     * A DirectoryId that is used to represent and unspecified log directory,
     * that is expected to have been previously selected to host an
     * associated replica. This contrasts with {@code UNASSIGNED_DIR},
     * which is associated with (typically new) replicas that may not
     * yet have been placed in any log directory.
     */
    public static final DirectoryId MIGRATING = new DirectoryId(0L, 2L);

    /**
     * The set of reserved UUIDs that will never be returned by the random method.
     */
    public static final Set<DirectoryId> RESERVED;

    static {
        HashSet<DirectoryId> reserved = new HashSet<>();
        // The first 100 DirectoryIds are reserved for future use.
        for (long i = 0L; i < 100L; i++) {
            reserved.add(new DirectoryId(0L, i));
        }
        RESERVED = Collections.unmodifiableSet(reserved);
    }

    /**
     * Constructs a Directory ID from the underlying 128 bits,
     * exactly as a {@link Uuid} is constructed.
     */
    private DirectoryId(long mostSigBits, long leastSigBits) {
        super(mostSigBits, leastSigBits);
    }

    /**
     * Creates a DirectoryId based on a base64 string encoding used in the toString() method.
     */
    public static DirectoryId fromString(String str) {
        return DirectoryId.fromUuid(Uuid.fromString(str));
    }

    /**
     * Creates a DirectoryId based on a {@link Uuid}.
     */
    public static DirectoryId fromUuid(Uuid uuid) {
        return new DirectoryId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Static factory to generate a directory ID.
     *
     * This will not generate a reserved UUID (first 100), or one whose string representation starts with a dash ("-")
     */
    public static DirectoryId random() {
        Uuid uuid = Uuid.randomUuid();
        while (RESERVED.contains(uuid) || uuid.toString().startsWith("-")) {
            uuid = Uuid.randomUuid();
        }
        return DirectoryId.fromUuid(uuid);
    }

    /**
     * Convert a list of Uuid to an array of DirectoryId.
     *
     * @param list          The input list
     * @return              The output array
     */
    public static DirectoryId[] toArray(List<Uuid> list) {
        if (list == null) return null;
        DirectoryId[] array = new DirectoryId[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = DirectoryId.fromUuid(list.get(i));
        }
        return array;
    }

    /**
     * Convert an array of DirectoryIds to a list of Uuid.
     *
     * @param array         The input array
     * @return              The output list
     */
    public static List<Uuid> toList(DirectoryId[] array) {
        if (array == null) return null;
        List<Uuid> list = new ArrayList<>(array.length);
        list.addAll(Arrays.asList(array));
        return list;
    }

    /**
     * Calculate the new directory information based on an existing replica assignment.
     * Replicas for which there already is a directory ID keep the same directory.
     * All other replicas get {@link #UNASSIGNED}.
     * @param currentReplicas               The current replicas, represented by the broker IDs
     * @param currentDirectories            The current directory information
     * @param newReplicas                   The new replica list
     * @return                              The new directory list
     * @throws IllegalArgumentException     If currentReplicas and currentDirectories have different lengths,
     *                                      or if there are duplicate broker IDs in the replica lists
     */
    public static List<Uuid> createDirectoriesFrom(int[] currentReplicas, DirectoryId[] currentDirectories, List<Integer> newReplicas) {
        if (currentReplicas == null) currentReplicas = new int[0];
        if (currentDirectories == null) currentDirectories = new DirectoryId[0];
        Map<Integer, DirectoryId> assignments = createAssignmentMap(currentReplicas, currentDirectories);
        List<Uuid> consolidated = new ArrayList<>(newReplicas.size());
        for (int newReplica : newReplicas) {
            Uuid newDirectory = assignments.getOrDefault(newReplica, UNASSIGNED);
            consolidated.add(newDirectory);
        }
        return consolidated;
    }

    /**
     * Build a mapping from replica to directory based on two lists of the same size and order.
     * @param replicas                      The replicas, represented by the broker IDs
     * @param directories                   The directory information
     * @return                              A map, linking each replica to its assigned directory
     * @throws IllegalArgumentException     If replicas and directories have different lengths,
     *                                      or if there are duplicate broker IDs in the replica list
     */
    public static Map<Integer, DirectoryId> createAssignmentMap(int[] replicas, DirectoryId[] directories) {
        if (replicas.length != directories.length) {
            throw new IllegalArgumentException("The lengths for replicas and directories do not match.");
        }
        Map<Integer, DirectoryId> assignments = new HashMap<>();
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = replicas[i];
            DirectoryId directory = directories[i];
            if (assignments.put(brokerId, directory) != null) {
                throw new IllegalArgumentException("Duplicate broker ID in assignment");
            }
        }
        return assignments;
    }

    /**
     * Create an array with the specified number of entries set to {@link #UNASSIGNED}.
     */
    public static DirectoryId[] unassignedArray(int length) {
        DirectoryId[] array = new DirectoryId[length];
        Arrays.fill(array, UNASSIGNED);
        return array;
    }
}
