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

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.EpochEntry;

import java.io.PrintStream;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ExpectListOffsetsAction implements TieredStorageTestAction {

    private final TopicPartition partition;
    private final OffsetSpec spec;
    private final EpochEntry expected;

    public ExpectListOffsetsAction(TopicPartition partition,
                                   OffsetSpec spec,
                                   EpochEntry expected) {
        this.partition = partition;
        this.spec = spec;
        this.expected = expected;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResult = context.admin()
                .listOffsets(Collections.singletonMap(partition, spec))
                .all()
                .get()
                .get(partition);
        assertEquals(expected.startOffset, listOffsetsResult.offset());
        if (expected.epoch != -1) {
            assertTrue(listOffsetsResult.leaderEpoch().isPresent());
            assertEquals(expected.epoch, listOffsetsResult.leaderEpoch().get());
        }
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-list-offsets partition: %s, spec: %s, expected-epoch-and-offset: %s%n",
                partition, spec, expected);
    }
}
