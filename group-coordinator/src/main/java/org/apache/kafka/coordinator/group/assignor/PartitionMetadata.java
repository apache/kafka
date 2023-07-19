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
package org.apache.kafka.coordinator.group.assignor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PartitionMetadata {

    /**
     * Partition number mapped to a set of racks where
     * its replicas are located.
     */
    Map<Integer, Set<String>> partitionsWithRacks;

    //If rack information isn't available pass an empty set.
    public PartitionMetadata (Map<Integer, Set<String>> partitionsWithRacks) {
        Objects.requireNonNull(partitionsWithRacks);
        this.partitionsWithRacks = partitionsWithRacks;
    }

    /**
     * Returns the number of partitions.
     *
     * @return number of partitions associated with the topic.
     */
    public int numPartitions() {
        return partitionsWithRacks.size();
    }

    /**
     * Returns the rack information for the replicas of the given partition.
     *
     * @param partition partition number.
     * @return Set of racks associated with the replicas of the given partition.
     *         If no rack information is available, an empty set is returned.
     */
    public Set<String> racks(int partition) {
        return partitionsWithRacks.get(partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionMetadata)) return false;
        PartitionMetadata that = (PartitionMetadata) o;
        return Objects.equals(partitionsWithRacks, that.partitionsWithRacks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionsWithRacks);
    }

    @Override
    public String toString() {
        return "PartitionMetadata{" +
            "partitionsWithRacks=" + partitionsWithRacks +
            '}';
    }
}
