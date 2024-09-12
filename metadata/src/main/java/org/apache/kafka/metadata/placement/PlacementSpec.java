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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;


/**
 * Specifies a replica placement that we want to make.
 */
@InterfaceStability.Unstable
public class PlacementSpec {
    private final int startPartition;

    private final int numPartitions;

    private final short numReplicas;

    public PlacementSpec(
        int startPartition,
        int numPartitions,
        short numReplicas
    ) {
        this.startPartition = startPartition;
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
    }

    public int startPartition() {
        return startPartition;
    }

    public int numPartitions() {
        return numPartitions;
    }

    public short numReplicas() {
        return numReplicas;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o.getClass().equals(this.getClass()))) return false;
        PlacementSpec other = (PlacementSpec) o;
        return startPartition == other.startPartition &&
            numPartitions == other.numPartitions &&
            numReplicas == other.numReplicas;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startPartition,
            numPartitions,
            numReplicas);
    }

    @Override
    public String toString() {
        return "PlacementSpec" +
            "(startPartition=" + startPartition +
            ", numPartitions=" + numPartitions +
            ", numReplicas=" + numReplicas +
            ")";
    }
}
