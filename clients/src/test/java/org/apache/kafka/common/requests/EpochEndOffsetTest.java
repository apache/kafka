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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import static org.apache.kafka.common.requests.EpochEndOffset.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.EpochEndOffset.UNDEFINED_EPOCH_OFFSET;
import static org.junit.Assert.assertEquals;

public class EpochEndOffsetTest {

    @Test
    public void testConstructor() {
        int leaderEpoch = 5;
        long endOffset = 10L;
        EpochEndOffset epochEndOffset = new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, leaderEpoch, endOffset);

        verify(leaderEpoch, endOffset, true, Errors.FENCED_LEADER_EPOCH, false, epochEndOffset);
    }

    @Test
    public void testWithUndefinedEpoch() {
        EpochEndOffset epochEndOffset = new EpochEndOffset(-1, 2L);

        verify(UNDEFINED_EPOCH, 2L, false, Errors.NONE, true, epochEndOffset);
    }

    @Test
    public void testWithUndefinedEndOffset() {
        EpochEndOffset epochEndOffset = new EpochEndOffset(3, -1L);

        verify(3, UNDEFINED_EPOCH_OFFSET, false, Errors.NONE, true, epochEndOffset);
    }

    private void verify(int leaderEpoch,
                        long endOffset,
                        boolean hasError,
                        Errors error,
                        boolean hasUndefinedEpochOffset,
                        EpochEndOffset epochEndOffset) {
        assertEquals(leaderEpoch, epochEndOffset.leaderEpoch());
        assertEquals(endOffset, epochEndOffset.endOffset());
        assertEquals(hasError, epochEndOffset.hasError());
        assertEquals(error, epochEndOffset.error());
        assertEquals(hasUndefinedEpochOffset, epochEndOffset.hasUndefinedEpochOrOffset());
    }
}
