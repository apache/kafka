/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.util;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities that connector implementations might find useful. Contains common building blocks
 * for writing connectors.
 */
@InterfaceStability.Unstable
public class ConnectorUtils {
    /**
     * Given a list of elements and a target number of groups, generates list of groups of
     * elements to match the target number of groups, spreading them evenly among the groups.
     * This generates groups with contiguous elements, which results in intuitive ordering if
     * your elements are also ordered (e.g. alphabetical lists of table names if you sort
     * table names alphabetically to generate the raw partitions) or can result in efficient
     * partitioning if elements are sorted according to some criteria that affects performance
     * (e.g. topic partitions with the same leader).
     *
     * @param elements list of elements to partition
     * @param numGroups the number of output groups to generate.
     */
    public static <T> List<List<T>> groupPartitions(List<T> elements, int numGroups) {
        if (numGroups <= 0)
            throw new IllegalArgumentException("Number of groups must be positive.");

        List<List<T>> result = new ArrayList<>(numGroups);

        // Each group has either n+1 or n raw partitions
        int perGroup = elements.size() / numGroups;
        int leftover = elements.size() - (numGroups * perGroup);

        int assigned = 0;
        for (int group = 0; group < numGroups; group++) {
            int numThisGroup = group < leftover ? perGroup + 1 : perGroup;
            List<T> groupList = new ArrayList<>(numThisGroup);
            for (int i = 0; i < numThisGroup; i++) {
                groupList.add(elements.get(assigned));
                assigned++;
            }
            result.add(groupList);
        }

        return result;
    }
}
