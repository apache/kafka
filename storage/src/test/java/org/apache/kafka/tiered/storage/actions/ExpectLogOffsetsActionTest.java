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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExpectLogOffsetsActionTest {

    @ParameterizedTest
    @CsvSource({"3, 4, 4", "4, 3, 4", "4, 4, 3"})
    void testOffsetMismatchIsReported(long highWatermark, long lastStableOffset, long logEndOffset) {
        TopicPartition partition = new TopicPartition("topic", 0);
        TieredStorageTestContext context = mock(TieredStorageTestContext.class);
        UnifiedLog log = mock(UnifiedLog.class);
        when(context.log(0, partition)).thenReturn(Optional.of(log));
        when(log.highWatermark()).thenReturn(4L);
        when(log.lastStableOffset()).thenReturn(4L);
        when(log.logEndOffset()).thenReturn(4L);
        ExpectLogOffsetsAction action = new ExpectLogOffsetsAction(0, partition, highWatermark, lastStableOffset, logEndOffset);

        assertThrows(AssertionError.class, () -> action.execute(context));

        verify(context).fail(action);
        verify(context, never()).succeed(action);
    }
}
