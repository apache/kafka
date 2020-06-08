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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EpochEndOffsetTest {

    @Test
    public void testConstructor() {
        int leaderEpoch = 5;
        long endOffset = 10L;
        EpochEndOffset epochEndOffset = new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, leaderEpoch, endOffset);

        assertEquals(leaderEpoch, epochEndOffset.leaderEpoch());
        assertEquals(endOffset, epochEndOffset.endOffset());
        assertTrue(epochEndOffset.hasError());
        assertEquals(Errors.FENCED_LEADER_EPOCH, epochEndOffset.error());
        assertFalse(epochEndOffset.hasUndefinedEpochOrOffset());
    }

    @Test
    public void testWithUndefinedEpoch() {
        EpochEndOffset epochEndOffset = new EpochEndOffset(-1, 2L);

        assertEquals(UNDEFINED_EPOCH, epochEndOffset.leaderEpoch());
        assertEquals(2L, epochEndOffset.endOffset());
        assertFalse(epochEndOffset.hasError());
        assertEquals(Errors.NONE, epochEndOffset.error());
        assertTrue(epochEndOffset.hasUndefinedEpochOrOffset());
    }

    @Test
    public void testWithUndefinedEndOffset() {
        EpochEndOffset epochEndOffset = new EpochEndOffset(3, -1L);

        assertEquals(3, epochEndOffset.leaderEpoch());
        assertEquals(UNDEFINED_EPOCH_OFFSET, epochEndOffset.endOffset());
        assertFalse(epochEndOffset.hasError());
        assertEquals(Errors.NONE, epochEndOffset.error());
        assertTrue(epochEndOffset.hasUndefinedEpochOrOffset());
    }
}
