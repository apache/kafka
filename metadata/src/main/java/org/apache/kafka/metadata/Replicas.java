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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
        List<Integer> list = new ArrayList<>(array.length);
        for (int i : array) {
            list.add(i);
        }
        return list;
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
     * Check that a replica set is valid.
     *
     * @param replicas      The replica set.
     * @return              True if none of the replicas are negative, and there are no
     *                      duplicates.
     */
    public static boolean validate(int[] replicas) {
        if (replicas.length == 0) return true;
        int[] sortedReplicas = clone(replicas);
        Arrays.sort(sortedReplicas);
        int prev = sortedReplicas[0];
        if (prev < 0) return false;
        for (int i = 1; i < sortedReplicas.length; i++) {
            int replica = sortedReplicas[i];
            if (prev == replica) return false;
            prev = replica;
        }
        return true;
    }

    /**
     * Check that an isr set is valid.
     *
     * @param replicas      The replica set.
     * @param isr           The in-sync replica set.
     * @return              True if none of the in-sync replicas are negative, there are
     *                      no duplicates, and all in-sync replicas are also replicas.
     */
    public static boolean validateIsr(int[] replicas, int[] isr) {
        if (isr.length == 0) return true;
        if (replicas.length == 0) return false;
        int[] sortedReplicas = clone(replicas);
        Arrays.sort(sortedReplicas);
        int[] sortedIsr = clone(isr);
        Arrays.sort(sortedIsr);
        int j = 0;
        if (sortedIsr[0] < 0) return false;
        int prevIsr = -1;
        for (int curIsr : sortedIsr) {
            if (prevIsr == curIsr) return false;
            prevIsr = curIsr;
            while (true) {
                if (j == sortedReplicas.length) return false;
                int curReplica = sortedReplicas[j++];
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
        for (int replica : replicas) {
            if (replica == value) return true;
        }
        return false;
    }

    /**
     * Check if the first list of integers contains the second.
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
     * Copy a replica array without any occurrences of the given value.
     *
     * @param replicas      The replica array.
     * @param value         The value to filter out.
     *
     * @return              A new array without the given value.
     */
    public static int[] copyWithout(int[] replicas, int value) {
        int size = 0;
        for (int replica : replicas) {
            if (replica != value) {
                size++;
            }
        }
        int[] result = new int[size];
        int j = 0;
        for (int replica : replicas) {
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
        for (int replica : replicas) {
            if (!Replicas.contains(values, replica)) {
                size++;
            }
        }
        int[] result = new int[size];
        int j = 0;
        for (int replica : replicas) {
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
}
