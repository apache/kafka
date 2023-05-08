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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AssignmentTestUtil {
    public static Map.Entry<Uuid, Set<Integer>> mkTopicAssignment(
        Uuid topicId,
        Integer... partitions
    ) {
        return new AbstractMap.SimpleEntry<>(
            topicId,
            new HashSet<>(Arrays.asList(partitions))
        );
    }

    public static Map.Entry<Uuid, Set<Integer>> mkSortedTopicAssignment(
        Uuid topicId,
        Integer... partitions
    ) {
        return new AbstractMap.SimpleEntry<>(
            topicId,
            new TreeSet<>(Arrays.asList(partitions))
        );
    }

    @SafeVarargs
    public static Map<Uuid, Set<Integer>> mkAssignment(Map.Entry<Uuid, Set<Integer>>... entries) {
        Map<Uuid, Set<Integer>> assignment = new HashMap<>();
        for (Map.Entry<Uuid, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }

    @SafeVarargs
    public static Map<Uuid, Set<Integer>> mkSortedAssignment(Map.Entry<Uuid, Set<Integer>>... entries) {
        Map<Uuid, Set<Integer>> assignment = new LinkedHashMap<>();
        for (Map.Entry<Uuid, Set<Integer>> entry : entries) {
            assignment.put(entry.getKey(), entry.getValue());
        }
        return assignment;
    }
}
