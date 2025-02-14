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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageCondition;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.specs.RemoteDeleteSegmentSpec;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageCondition.expectEvent;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;

public final class DeleteRecordsAction implements TieredStorageTestAction {

    private static final int DELETE_WAIT_TIMEOUT_SEC = 10;
    private final TopicPartition partition;
    private final Long beforeOffset;
    private final List<RemoteDeleteSegmentSpec> deleteSegmentSpecs;

    public DeleteRecordsAction(TopicPartition partition,
                               Long beforeOffset,
                               List<RemoteDeleteSegmentSpec> deleteSegmentSpecs) {
        this.partition = partition;
        this.beforeOffset = beforeOffset;
        this.deleteSegmentSpecs = deleteSegmentSpecs;
    }

    @Override
    public void doExecute(TieredStorageTestContext context)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<LocalTieredStorage> tieredStorages = context.remoteStorageManagers();
        List<LocalTieredStorageCondition> tieredStorageConditions = deleteSegmentSpecs.stream()
                .filter(spec -> spec.getEventType() == DELETE_SEGMENT)
                .map(spec -> expectEvent(
                        tieredStorages,
                        spec.getEventType(),
                        spec.getSourceBrokerId(),
                        spec.getTopicPartition(),
                        false,
                        spec.getEventCount()))
                .toList();

        Map<TopicPartition, RecordsToDelete> recordsToDeleteMap =
                Collections.singletonMap(partition, RecordsToDelete.beforeOffset(beforeOffset));
        context.admin().deleteRecords(recordsToDeleteMap).all().get();

        if (!tieredStorageConditions.isEmpty()) {
            tieredStorageConditions.stream()
                    .reduce(LocalTieredStorageCondition::and)
                    .get()
                    .waitUntilTrue(DELETE_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        }
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("delete-records partition: %s, before-offset: %d%n", partition, beforeOffset);
        deleteSegmentSpecs.forEach(spec -> output.println("    " + spec));
    }
}
