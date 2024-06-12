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

package org.apache.kafka.tools.reassign;

import java.util.Objects;
import java.util.Set;

/**
 * A partition movement.  The source and destination brokers may overlap.
 */
final class PartitionMove {
    public final Set<Integer> sources;

    public final Set<Integer> destinations;

    /**
     * @param sources         The source brokers.
     * @param destinations    The destination brokers.
     */
    public PartitionMove(Set<Integer> sources, Set<Integer> destinations) {
        this.sources = sources;
        this.destinations = destinations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionMove that = (PartitionMove) o;
        return Objects.equals(sources, that.sources) && Objects.equals(destinations, that.destinations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sources, destinations);
    }
}
