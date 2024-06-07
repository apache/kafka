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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RocksDBMetricsRecordingTriggerTest {

    private final static String STORE_NAME1 = "store-name1";
    private final static String STORE_NAME2 = "store-name2";
    private final static TaskId TASK_ID1 = new TaskId(1, 2);
    private final static TaskId TASK_ID2 = new TaskId(2, 4);
    @Mock
    private RocksDBMetricsRecorder recorder1;
    @Mock
    private RocksDBMetricsRecorder recorder2;


    private final Time time = new MockTime();
    private final RocksDBMetricsRecordingTrigger recordingTrigger = new RocksDBMetricsRecordingTrigger(time);

    @Before
    public void setUp() {
        when(recorder1.storeName()).thenReturn(STORE_NAME1);
        when(recorder1.taskId()).thenReturn(TASK_ID1);
        when(recorder2.storeName()).thenReturn(STORE_NAME2);
        when(recorder2.taskId()).thenReturn(TASK_ID2);
    }

    @Test
    public void shouldTriggerAddedMetricsRecorders() {
        recordingTrigger.addMetricsRecorder(recorder1);
        recordingTrigger.addMetricsRecorder(recorder2);

        doNothing().when(recorder1).record(time.milliseconds());
        doNothing().when(recorder2).record(time.milliseconds());

        recordingTrigger.run();
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