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
package org.apache.kafka.streams.processor.internals.namedtopology;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;

public class NamedTopologyStreamsMetadataImpl extends StreamsMetadataImpl {
    private final Map<String, Collection<String>> namedTopologyToStoreNames;

    public NamedTopologyStreamsMetadataImpl(final HostInfo hostInfo,
        final Set<String> stateStoreNames,
        final Set<TopicPartition> topicPartitions,
        final Set<String> standbyStateStoreNames,
        final Set<TopicPartition> standbyTopicPartitions,
        final Map<String, Collection<String>> namedTopologyToStoreNames
    ) {
        super(hostInfo,
            stateStoreNames,
            topicPartitions,
            standbyStateStoreNames,
            standbyTopicPartitions);
        this.namedTopologyToStoreNames = namedTopologyToStoreNames;
    }

    public Map<String, Collection<String>> namedTopologyToStoreNames() {
        return namedTopologyToStoreNames;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final NamedTopologyStreamsMetadataImpl that = (NamedTopologyStreamsMetadataImpl) o;

        return Objects.equals(namedTopologyToStoreNames, that.namedTopologyToStoreNames);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (namedTopologyToStoreNames() != null ? namedTopologyToStoreNames().hashCode() : 0);
        return result;
    }
}
