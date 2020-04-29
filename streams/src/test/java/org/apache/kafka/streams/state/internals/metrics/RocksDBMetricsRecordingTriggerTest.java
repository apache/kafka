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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.resetToDefault;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertThrows;

public class RocksDBMetricsRecordingTriggerTest {

    private final static String STORE_NAME1 = "store-name1";
    private final static String STORE_NAME2 = "store-name2";
    private final static TaskId TASK_ID1 = new TaskId(1, 2);
    private final static TaskId TASK_ID2 = new TaskId(2, 4);
    private final RocksDBMetricsRecorder recorder1 = niceMock(RocksDBMetricsRecorder.class);
    private final RocksDBMetricsRecorder recorder2 = niceMock(RocksDBMetricsRecorder.class);

    private final Time time = new MockTime();
    private final RocksDBMetricsRecordingTrigger recordingTrigger = new RocksDBMetricsRecordingTrigger(time);

    @Before
    public void setUp() {
        expect(recorder1.storeName()).andStubReturn(STORE_NAME1);
        expect(recorder1.taskId()).andStubReturn(TASK_ID1);
        replay(recorder1);
        expect(recorder2.storeName()).andStubReturn(STORE_NAME2);
        expect(recorder2.taskId()).andStubReturn(TASK_ID2);
        replay(recorder2);
    }

    @Test
    public void shouldTriggerAddedMetricsRecorders() {
        recordingTrigger.addMetricsRecorder(recorder1);
        recordingTrigger.addMetricsRecorder(recorder2);

        resetToDefault(recorder1);
        recorder1.record(time.milliseconds());
        replay(recorder1);
        resetToDefault(recorder2);
        recorder2.record(time.milliseconds());
        replay(recorder2);

        recordingTrigger.run();

        verify(recorder1);
        verify(recorder2);
    }

    @Test
    public void shouldThrowIfRecorderToAddHasBeenAlreadyAdded() {
        recordingTrigger.addMetricsRecorder(recorder1);

        assertThrows(
            IllegalStateException.class,
            () -> recordingTrigger.addMetricsRecorder(recorder1)
        );
    }

    @Test
    public void shouldThrowIfRecorderToRemoveCouldNotBeFound() {
        recordingTrigger.addMetricsRecorder(recorder1);
        assertThrows(
            IllegalStateException.class,
            () -> recordingTrigger.removeMetricsRecorder(recorder2)
        );
    }
}