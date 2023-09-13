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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class OffsetFetchApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    private final Set<TopicPartition> partitions;

    public OffsetFetchApplicationEvent(final Set<TopicPartition> partitions) {
        super(Type.FETCH_COMMITTED_OFFSET);
        this.partitions = Collections.unmodifiableSet(partitions);
    }

    public Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        OffsetFetchApplicationEvent that = (OffsetFetchApplicationEvent) o;

        return partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                ", partitions=" + partitions +
                '}';
    }
}
