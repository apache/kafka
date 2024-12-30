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

package org.apache.kafka.tools.filter;

import java.util.Set;

public interface PartitionFilter {
    /**
     * Used to filter partitions based on a certain criteria, for example, a set of partition ids.
     */
    boolean isPartitionAllowed(int partition);

    class PartitionsSetFilter implements PartitionFilter {
        private final Set<Integer> partitionIds;
        public PartitionsSetFilter(Set<Integer> partitionIds) {
            this.partitionIds = partitionIds;
        }
        @Override
        public boolean isPartitionAllowed(int partition) {
            return partitionIds.isEmpty() || partitionIds.contains(partition);
        }
    }

    class UniquePartitionFilter implements PartitionFilter {
        private final int partition;

        public UniquePartitionFilter(int partition) {
            this.partition = partition;
        }

        @Override
        public boolean isPartitionAllowed(int partition) {
            return partition == this.partition;
        }
    }

    class PartitionRangeFilter implements PartitionFilter {
        private final int lowerRange;
        private final int upperRange;

        public PartitionRangeFilter(int lowerRange, int upperRange) {
            this.lowerRange = lowerRange;
            this.upperRange = upperRange;
        }

        @Override
        public boolean isPartitionAllowed(int partition) {
            return partition >= lowerRange && partition < upperRange;
        }
    }
}
