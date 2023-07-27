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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SourceTaskOffsetCommitterTest {

    private final ConcurrentHashMap<ConnectorTaskId, ScheduledFuture<?>> committers = new ConcurrentHashMap<>();

    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ScheduledFuture<?> commitFuture;
    @Mock
    private ScheduledFuture<?> taskFuture;
    @Mock
    private ConnectorTaskId taskId;
    @Mock
    private WorkerSourceTask task;

    private SourceTaskOffsetCommitter committer;

    private static final long DEFAULT_OFFSET_COMMIT_INTERVAL_MS = 1000;

    @Before
    public void setup() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms",
                Long.toString(DEFAULT_OFFSET_COMMIT_INTERVAL_MS));
        WorkerConfig config = new StandaloneConfig(workerProps);
        committer = new SourceTaskOffsetCommitter(config, executor, committers);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSchedule() {
        ArgumentCaptor<Runnable> taskWrapper = ArgumentCaptor.forClass(Runnable.class);

        when(executor.scheduleWithFixedDelay(
                taskWrapper.capture(), eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS),
                eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS), eq(TimeUnit.MILLISECONDS))
        ).thenReturn((ScheduledFuture) commitFuture);

        committer.schedule(taskId, task);
        assertNotNull(taskWrapper.getValue());
        assertEquals(singletonMap(taskId, commitFuture), committers);
    }

    @Test
    public void testCloseTimeout() throws Exception {
        long timeoutMs = 1000;

        // Normal termination, where termination times out.
        when(executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)).thenReturn(false);

        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(ThreadUtils.class)) {
            committer.close(timeoutMs);
            assertTrue(logCaptureAppender.getEvents().stream().anyMatch(e -> e.getLevel().equals("ERROR")));
        }

        verify(executor).shutdown();
    }

    @Test
    public void testCloseInterrupted() throws InterruptedException {
        long timeoutMs = 1000;

        // Termination interrupted
        when(executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException());

        committer.close(timeoutMs);

        verify(executor).shutdown();
    }

    @Test
    public void testRemoveNonExistentTask() {
        assertTrue(committers.isEmpty());
        committer.remove(taskId);
        assertTrue(committers.isEmpty());
    }

    @Test
    public void testRemoveSuccess() {
        expectRemove();
        committers.put(taskId, taskFuture);
        committer.remove(taskId);
        assertTrue(committers.isEmpty());
    }

    @Test
    public void testRemoveCancelledTask() throws ExecutionException, InterruptedException {
        expectRemove();
        when(taskFuture.get()).thenThrow(new CancellationException());

        committers.put(taskId, taskFuture);
        try (LogCaptureAppender logCaptureAppender = LogCaptureAppender.createAndRegister(SourceTaskOffsetCommitter.class)) {
            LogCaptureAppender.setClassLoggerToTrace(SourceTaskOffsetCommitter.class);
            committer.remove(taskId);
            assertTrue(logCaptureAppender.getEvents().stream().anyMatch(e -> e.getLevel().equals("TRACE")));
        }
        assertTrue(committers.isEmpty());
    }

    @Test
    public void testRemoveTaskAndInterrupted() throws ExecutionException, InterruptedException {
        expectRemove();
        when(taskFuture.get()).thenThrow(new InterruptedException());

        committers.put(taskId, taskFuture);
        assertThrows(ConnectException.class, () -> committer.remove(taskId));
    }

    private void expectRemove() {
        when(taskFuture.cancel(false)).thenReturn(false);
        when(taskFuture.isDone()).thenReturn(false);
        when(taskId.connector()).thenReturn("MyConnector");
        when(taskId.task()).thenReturn(1);
    }

}
