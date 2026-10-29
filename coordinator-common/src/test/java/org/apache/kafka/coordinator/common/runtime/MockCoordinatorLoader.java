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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A CoordinatorLoader that always succeeds.
 */
public class MockCoordinatorLoader implements CoordinatorLoader<String> {
    private final LoadSummary summary;
    private final List<Long> lastWrittenOffsets;
    private final List<Long> lastCommittedOffsets;

    public MockCoordinatorLoader(
        LoadSummary summary,
        List<Long> lastWrittenOffsets,
        List<Long> lastCommittedOffsets
    ) {
        this.summary = summary;
        this.lastWrittenOffsets = lastWrittenOffsets;
        this.lastCommittedOffsets = lastCommittedOffsets;
    }

    public MockCoordinatorLoader() {
        this(null, List.of(), List.of());
    }

    @Override
    public CompletableFuture<LoadSummary> load(
        TopicPartition tp,
        CoordinatorPlayback<String> replayable
    ) {
        lastWrittenOffsets.forEach(replayable::updateLastWrittenOffset);
        lastCommittedOffsets.forEach(replayable::updateLastCommittedOffset);
        return CompletableFuture.completedFuture(summary);
    }

    @Override
    public void close() {}
}
