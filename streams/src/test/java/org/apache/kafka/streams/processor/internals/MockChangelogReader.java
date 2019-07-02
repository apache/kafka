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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockChangelogReader implements ChangelogReader {
    private final Set<TopicPartition> registered = new HashSet<>();
    private Map<TopicPartition, Long> restoredOffsets = Collections.emptyMap();

    @Override
    public void register(final StateRestorer restorer) {
        registered.add(restorer.partition());
    }

    @Override
    public Collection<TopicPartition> restore(final RestoringTasks active) {
        return registered;
    }

    @Override
    public Map<TopicPartition, Long> restoredOffsets() {
        return restoredOffsets;
    }

    void setRestoredOffsets(final Map<TopicPartition, Long> restoredOffsets) {
        this.restoredOffsets = restoredOffsets;
    }

    @Override
    public Map<TopicPartition, Long> restoredOffsets(final Set<TopicPartition> partitions) {
        final Map<TopicPartition, Long> result = new HashMap<>();
        for (final TopicPartition partition : partitions) {
            if (restoredOffsets.containsKey(partition)) {
                result.put(partition, restoredOffsets.get(partition));
            }
        }
        return result;
    }

    @Override
    public void reset() {
        registered.clear();
    }

    public boolean wasRegistered(final TopicPartition partition) {
        return registered.contains(partition);
    }
}
