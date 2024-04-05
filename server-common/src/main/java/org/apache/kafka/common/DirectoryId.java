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
import java.util.List;
import java.util.Map;

public class DirectoryId {
    /**
     * A Uuid that is used to represent an unspecified log directory,
     * that is expected to have been previously selected to host an
     * associated replica. This contrasts with {@code UNASSIGNED_DIR},
     * which is associated with (typically new) replicas that may not
     * yet have been placed in any log directory.
     */
    public static final Uuid MIGRATING = new Uuid(0L, 0L);

    /**
     * A Uuid that is used to represent directories that are pending an assignment.
     */
    public static final Uuid UNASSIGNED = new Uuid(0L, 1L);

    /**
     * A Uuid that is used to represent unspecified offline dirs.
     */
    public static final Uuid LOST = new Uuid(0L, 2L);

    /**
     * Static factory to generate a directory ID.
     *
     * This will not generate a reserved UUID (first 100), or one whose string representation
     * starts with a dash ("-")
     */
    public static Uuid random() {
        while (true) {
            // Uuid.randomUuid does not generate Uuids whose string representation starts with a
            // dash.
            Uuid uuid = Uuid.randomUuid();
            if (!DirectoryId.reserved(uuid)) {
                return uuid;
            }
        }
    }

    /**
     * Check if a directory ID is part of the first 100 reserved IDs.
     *
     * @param uuid the directory ID to check.
     * @return     true only if the directory ID is reserved.
     */
    public static boolean reserved(Uuid uuid) {
        return uuid.getMostSignificantBits() == 0 &&
            uuid.getLeastSignificantBits() < 100;
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
    public static List<Uuid> createDirectoriesFrom(int[] currentReplicas, Uuid[] currentDirectories, List<Integer> newReplicas) {
        if (currentReplicas == null) currentReplicas = new int[0];
        if (currentDirectories == null) currentDirectories = new Uuid[0];
        Map<Integer, Uuid> assignments = createAssignmentMap(currentReplicas, currentDirectories);
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
    public static Map<Integer, Uuid> createAssignmentMap(int[] replicas, Uuid[] directories) {
        if (replicas.length != directories.length) {
            throw new IllegalArgumentException("The lengths for replicas and directories do not match.");
        }
        Map<Integer, Uuid> assignments = new HashMap<>();
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = replicas[i];
            Uuid directory = directories[i];
            if (assignments.put(brokerId, directory) != null) {
                throw new IllegalArgumentException("Duplicate broker ID in assignment");
            }
        }
        return assignments;
    }

    /**
     * Create an array with the specified number of entries set to {@link #UNASSIGNED}.
     */
    public static Uuid[] unassignedArray(int length) {
        return array(length, UNASSIGNED);
    }

    /**
     * Create an array with the specified number of entries set to {@link #MIGRATING}.
     */
    public static Uuid[] migratingArray(int length) {
        return array(length, MIGRATING);
    }

    /**
     * Create an array with the specified number of entries set to the specified value.
     */
    private static Uuid[] array(int length, Uuid value) {
        Uuid[] array = new Uuid[length];
        Arrays.fill(array, value);
        return array;
    }

    /**
     * Check if a directory is online, given a sorted list of online directories.
     * @param dir              The directory to check
     * @param sortedOnlineDirs The sorted list of online directories
     * @return                 true if the directory is considered online, false otherwise
     */
    public static boolean isOnline(Uuid dir, List<Uuid> sortedOnlineDirs) {
        if (UNASSIGNED.equals(dir) || MIGRATING.equals(dir)) {
            return true;
        }
        if (LOST.equals(dir)) {
            return false;
        }

        // The only time we should have a size be 0 is if we were at a MV prior to 3.7-IV2
        // and the system was upgraded. In this case the original list of directories was purged
        // during broker registration so we don't know if the directory is online. We assume
        // that a broker will halt if all its log directories are down. Eventually the broker
        // will send another registration request with information about all log directories.
        // Refer KAFKA-16162 for more information
        if (sortedOnlineDirs.isEmpty()) {
            return true;
        }
        return Collections.binarySearch(sortedOnlineDirs, dir) >= 0;
    }
}
