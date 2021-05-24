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

package org.apache.kafka.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
        for (int i = 0; i < sortedIsr.length; i++) {
            int curIsr = sortedIsr[i];
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
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i] == value) return true;
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
}
