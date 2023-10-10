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

package org.apache.kafka.metadata;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Replicas {
    /**
     * An empty replica array.
     */
    public final static int[] NONE = new int[0];

    /**
     * Convert an array of integers to a list of ints.
     *
     * @param array         The input array.
     * @return              The output list.
     */
    public static List<Integer> toList(int[] array) {
        if (array == null) return null;
        ArrayList<Integer> list = new ArrayList<>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    /**
     * Convert an array of Replicas to a list of Replicas.
     *
     * @param array         The input array.
     * @return              The output list.
     */
    public static List<Replica> toList(Replica[] array) {
        if (array == null) return null;
        return Arrays.asList(array);
    }

    /**
     * Convert a list of integers to an array of ints.
     *
     * @param list          The input list.
     * @return              The output array.
     */
    public static int[] toArray(List<Integer> list) {
        if (list == null) return null;
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    /**
     * Convert a list of Replica to the respective array of broker IDs.
     *
     * @param list          The input list.
     * @return              The output array.
     */
    public static Replica[] toReplicaArray(List<Replica> list) {
        if (list == null) return null;
        Replica[] array = new Replica[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    /**
     * Copy an array of ints.
     *
     * @param array         The input array.
     * @return              A copy of the array.
     */
    public static int[] clone(int[] array) {
        int[] clone = new int[array.length];
        System.arraycopy(array, 0, clone, 0, array.length);
        return clone;
    }

    /**
     * Check that an isr set is valid.
     *
     * @param replicas      The replica set.
     * @param isr           The in-sync replica set.
     * @return              True if none of the in-sync replicas are negative, there are
     *                      no duplicates, and all in-sync replicas are also replicas.
     */
    public static boolean validateIsr(Replica[] replicas, int[] isr) {
        if (isr.length == 0) return true;
        if (replicas.length == 0) return false;
        int[] sortedBrokerIds = brokerIds(replicas);
        Arrays.sort(sortedBrokerIds);
        int[] sortedIsr = clone(isr);
        Arrays.sort(sortedIsr);
        int j = 0;
        if (sortedIsr[0] < 0) return false;
        int prevIsr = -1;
        for (int i = 0; i < sortedIsr.length; i++) {
            int curIsr = sortedIsr[i];
            if (prevIsr == curIsr) return false;
            prevIsr = curIsr;
            while (true) {
                if (j == sortedBrokerIds.length) return false;
                int curReplica = sortedBrokerIds[j++];
                if (curReplica == curIsr) break;
            }
        }
        return true;
    }

    /**
     * Returns true if an array of replicas contains a specific value.
     *
     * @param replicas      The replica array.
     * @param value         The value to look for.
     *
     * @return              True only if the value is found in the array.
     */
    public static boolean contains(int[] replicas, int value) {
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i] == value) return true;
        }
        return false;
    }

    /**
     * Check if the first list of broker IDs contains the second.
     *
     * @param a             The first list
     * @param b             The second list
     *
     * @return              True only if the first contains the second.
     */
    public static boolean contains(List<Integer> a, int[] b) {
        List<Integer> aSorted = new ArrayList<>(a);
        aSorted.sort(Integer::compareTo);
        List<Integer> bSorted = Replicas.toList(b);
        bSorted.sort(Integer::compareTo);
        int i = 0;
        for (int replica : bSorted) {
            while (true) {
                if (i >= aSorted.size()) return false;
                int replica2 = aSorted.get(i++);
                if (replica2 == replica) break;
                if (replica2 > replica) return false;
            }
        }
        return true;
    }

    /**
     * Returns true if an array of replicas contains a specific broker ID.
     *
     * @param replicas      The replica array.
     * @param brokerId      The value to look for.
     *
     * @return              True only if the value is found in the array.
     */
    public static boolean contains(Replica[] replicas, int brokerId) {
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i].brokerId == brokerId) return true;
        }
        return false;
    }

    /**
     * Copy a replica array without any occurrences of the given value.
     *
     * @param replicas      The replica array.
     * @param value         The value to filter out.
     *
     * @return              A new array without the given value.
     */
    public static int[] copyWithout(int[] replicas, int value) {
        int size = 0;
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i] != value) {
                size++;
            }
        }
        int[] result = new int[size];
        int j = 0;
        for (int i = 0; i < replicas.length; i++) {
            int replica = replicas[i];
            if (replica != value) {
                result[j++] = replica;
            }
        }
        return result;
    }

    /**
     * Copy a replica array without any occurrences of the given values.
     *
     * @param replicas      The replica array.
     * @param values        The values to filter out.
     *
     * @return              A new array without the given value.
     */
    public static int[] copyWithout(int[] replicas, int[] values) {
        int size = 0;
        for (int i = 0; i < replicas.length; i++) {
            if (!Replicas.contains(values, replicas[i])) {
                size++;
            }
        }
        int[] result = new int[size];
        int j = 0;
        for (int i = 0; i < replicas.length; i++) {
            int replica = replicas[i];
            if (!Replicas.contains(values, replica)) {
                result[j++] = replica;
            }
        }
        return result;
    }

    /**
     * Copy a replica array with the given value.
     *
     * @param replicas      The replica array.
     * @param value         The value to add.
     *
     * @return              A new array with the given value.
     */
    public static int[] copyWith(int[] replicas, int value) {
        int[] newReplicas = new int[replicas.length + 1];
        System.arraycopy(replicas, 0, newReplicas, 0, replicas.length);
        newReplicas[newReplicas.length - 1] = value;
        return newReplicas;
    }

    /**
     * Convert a replica array to a set.
     *
     * @param replicas      The replica array.
     *
     * @return              A new array with the given value.
     */
    public static Set<Integer> toSet(int[] replicas) {
        Set<Integer> result = new HashSet<>();
        for (int replica : replicas) {
            result.add(replica);
        }
        return result;
    }

    /**
     * Consolidated replica information by coalescing directory information,
     * keyed on the broker ID. If the updated replica information does not
     * specify directory information, the existing directory assignment
     * is maintained.
     * @param base the existing replica information
     * @param update the new replica information
     * @throws KafkaException if the base replicas contains duplicate broker IDs
     * @return a copy of the update parameter with coalesced directory information
     */
    public static Replica[] update(Replica[] base, Replica[] update) {
        Map<Integer, Uuid> assignments = new HashMap<>();
        for (Replica replica : base) {
            if (assignments.put(replica.brokerId(), replica.directory()) != null) {
                throw new KafkaException("Duplicate broker ID in assignment");
            }
        }
        Replica[] consolidated = new Replica[update.length];
        for (int i = 0; i < update.length; i++) {
            Replica replica = update[i];
            Uuid existingUuid = assignments.getOrDefault(replica.brokerId(), Uuid.UNKNOWN_DIR);
            Uuid updatedUuid = replica.directory();
            Uuid consolidatedUuid = Uuid.UNKNOWN_DIR.equals(updatedUuid) && !Uuid.UNKNOWN_DIR.equals(existingUuid)
                    ? existingUuid : updatedUuid;
            consolidated[i] = new Replica(replica.brokerId(), consolidatedUuid);
        }
        return consolidated;
    }

    /**
     * Extract an array of broker IDs from an array of Replicas.
     *
     * @param replicas  the array of replicas
     * @return          the array of broker IDs
     */
    public static int[] brokerIds(Replica[] replicas) {
        if (replicas == null) return null;
        int[] brokerIds = new int[replicas.length];
        for (int i = 0; i < brokerIds.length; i++) {
            brokerIds[i] = replicas[i].brokerId();
        }
        return brokerIds;
    }

    /**
     * Extract a list of broker IDs from a list of Replicas.
     *
     * @param replicas  the list of replicas
     * @return          the list of broker IDs
     */
    public static List<Integer> brokerIdsList(List<Replica> replicas) {
        if (replicas == null) return null;
        ArrayList<Integer> brokerIds = new ArrayList<>(replicas.size());
        for (Replica replica : replicas) {
            brokerIds.add(replica.brokerId());
        }
        return brokerIds;
    }

    /**
     * Extract a list of broker IDs from an array of Replicas.
     *
     * @param replicas  the array of replicas
     * @return          the list of broker IDs
     */
    public static List<Integer> brokerIdsList(Replica[] replicas) {
        if (replicas == null) return null;
        ArrayList<Integer> list = new ArrayList<>(replicas.length);
        for (Replica replica : replicas) {
            list.add(replica.brokerId());
        }
        return list;
    }

    /**
     * Build an array of Replicas from an array of broker IDs assuming
     * {@code Uuid.UNKNOWN_DIR} as the directory.
     * @param brokerIds  the array of broker IDs
     * @return           the array of Replicas
     */
    public static Replica[] withUnknownDirs(int[] brokerIds) {
        if (brokerIds == null) return null;
        Replica[] replicas = new Replica[brokerIds.length];
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = brokerIds[i];
            Uuid dir = Uuid.UNKNOWN_DIR;
            replicas[i] = new Replica(brokerId, dir);
        }
        return replicas;
    }

    /**
     * Build an array of Replicas from a list of broker IDs assuming
     * {@code Uuid.UNKNOWN_DIR} as the directory.
     * @param brokerIds  the list of broker IDs
     * @return           the array of Replicas
     */
    public static Replica[] withUnknownDirs(List<Integer> brokerIds) {
        if (brokerIds == null) return null;
        Replica[] replicas = new Replica[brokerIds.size()];
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = brokerIds.get(i);
            Uuid dir = Uuid.UNKNOWN_DIR;
            replicas[i] = new Replica(brokerId, dir);
        }
        return replicas;
    }

    /**
     * Build a Replica from a {@link PartitionRecord}.
     * @param record    the record
     * @return          the Replica
     */
    public static Replica[] fromRecord(PartitionRecord record) {
        if (!record.assignment().isEmpty()) {
            return record.assignment().
                    stream().map(a -> new Replica(a.broker(), a.directory())).
                    toArray(Replica[]::new);
        }
        if (!record.replicas().isEmpty()) {
            return withUnknownDirs(record.replicas());
        }
        return new Replica[0];
    }

    /**
     * Build a Replica from a {@link PartitionChangeRecord}.
     * @param record    the record
     * @return          the Replica
     */
    public static Replica[] fromRecord(PartitionChangeRecord record) {
        if (record.assignment() != null && !record.assignment().isEmpty()) {
            return record.assignment().
                    stream().map(a -> new Replica(a.broker(), a.directory())).
                    toArray(Replica[]::new);
        }
        if (record.replicas() != null && !record.replicas().isEmpty()) {
            return withUnknownDirs(record.replicas());
        }
        return new Replica[0];
    }

    /**
     * Build a list of {@link PartitionRecord.ReplicaAssignment} from an array of {@link Replica}.
     * @param array     the array
     * @return          the list
     */
    public static List<PartitionRecord.ReplicaAssignment> toPartitionRecordReplicaAssignment(Replica[] array) {
        if (array == null) return null;
        List<PartitionRecord.ReplicaAssignment> list = new ArrayList<>(array.length);
        for (Replica replica : array) {
            list.add(new PartitionRecord.ReplicaAssignment().
                    setBroker(replica.brokerId()).
                    setDirectory(replica.directory()));
        }
        return list;
    }

    /**
     * Build a list of {@link PartitionChangeRecord.ReplicaAssignment} from an array of {@link Replica}.
     * @param array     the array
     * @return          the list
     */
    public static List<PartitionChangeRecord.ReplicaAssignment> toPartitionChangeRecordReplicaAssignment(Replica[] array) {
        if (array == null) return null;
        List<PartitionChangeRecord.ReplicaAssignment> list = new ArrayList<>(array.length);
        for (Replica replica : array) {
            list.add(new PartitionChangeRecord.ReplicaAssignment().
                    setBroker(replica.brokerId()).
                    setDirectory(replica.directory()));
        }
        return list;
    }
}
