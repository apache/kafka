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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;

import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

public class Utils {
    private Utils() {}

    /**
     * @return An OptionalInt containing the value iff the value is different from
     * the sentinel (or default) value -1.
     */
    public static OptionalInt ofSentinel(int value) {
        return value != -1 ? OptionalInt.of(value) : OptionalInt.empty();
    }

    /**
     * @return An OptionalLong containing the value iff the value is different from
     * the sentinel (or default) value -1.
     */
    public static OptionalLong ofSentinel(long value) {
        return value != -1 ? OptionalLong.of(value) : OptionalLong.empty();
    }

    /**
     * @return The provided assignment as a String.
     *
     * Example:
     * [topicid1-0, topicid1-1, topicid2-0, topicid2-1]
     */
    public static String assignmentToString(
        Map<Uuid, Set<Integer>> assignment
    ) {
        StringBuilder builder = new StringBuilder("[");
        Iterator<Map.Entry<Uuid, Set<Integer>>> topicsIterator = assignment.entrySet().iterator();
        while (topicsIterator.hasNext()) {
            Map.Entry<Uuid, Set<Integer>> entry = topicsIterator.next();
            Iterator<Integer> partitionsIterator = entry.getValue().iterator();
            while (partitionsIterator.hasNext()) {
                builder.append(entry.getKey());
                builder.append("-");
                builder.append(partitionsIterator.next());
                if (partitionsIterator.hasNext() || topicsIterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
