/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SourceTaskOffsetCommitter.class, LoggerFactory.class})
public class SourceTaskOffsetCommitterTest extends ThreadedTest {
    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ConcurrentHashMap committers;
    @Mock
    private Logger mockLog;

    private SourceTaskOffsetCommitter committer;

    private static final long DEFAULT_OFFSET_COMMIT_INTERVAL_MS = 1000;

    @Override
    public void setup() {
        super.setup();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms",
                Long.toString(DEFAULT_OFFSET_COMMIT_INTERVAL_MS));
        WorkerConfig config = new StandaloneConfig(workerProps);
        committer = new SourceTaskOffsetCommitter(config, executor, committers);
        Whitebox.setInternalState(SourceTaskOffsetCommitter.class, "log", mockLog);
    }

    @Test
    public void testSchedule() throws Exception {
        Capture<Runnable> taskWrapper = EasyMock.newCapture();

        ScheduledFuture commitFuture = PowerMock.createMock(ScheduledFuture.class);
        EasyMock.expect(executor.scheduleWithFixedDelay(
                EasyMock.capture(taskWrapper), eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS),
                eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS), eq(TimeUnit.MILLISECONDS))
        ).andReturn(commitFuture);

        ConnectorTaskId taskId = PowerMock.createMock(ConnectorTaskId.class);
        WorkerSourceTask task = PowerMock.createMock(WorkerSourceTask.class);

        EasyMock.expect(committers.put(taskId, commitFuture)).andReturn(null);

        PowerMock.replayAll();

        committer.schedule(taskId, task);
        assertTrue(taskWrapper.hasCaptured());
        assertNotNull(taskWrapper.getValue());

        PowerMock.verifyAll();
    }

    @Test
    public void testClose() throws Exception {
        long timeoutMs = 1000;

        // Normal termination, where termination times out.
        executor.shutdown();
        PowerMock.expectLastCall();

        EasyMock.expect(executor.awaitTermination(eq(timeoutMs), eq(TimeUnit.MILLISECONDS)))
                .andReturn(false);
        mockLog.error(EasyMock.anyString());
        PowerMock.expectLastCall();
        PowerMock.replayAll();

        committer.close(timeoutMs);

        PowerMock.verifyAll();
        PowerMock.resetAll();

        // Termination interrupted
        executor.shutdown();
        PowerMock.expectLastCall();

        EasyMock.expect(executor.awaitTermination(eq(timeoutMs), eq(TimeUnit.MILLISECONDS)))
                .andThrow(new InterruptedException());
        PowerMock.replayAll();

        committer.close(timeoutMs);

        PowerMock.verifyAll();
    }

    @Test
    public void testRemove() throws Exception {
        ConnectorTaskId taskId = PowerMock.createMock(ConnectorTaskId.class);
        ScheduledFuture task = PowerMock.createMock(ScheduledFuture.class);

        // Try to remove a non-existing task
        EasyMock.expect(committers.remove(taskId)).andReturn(null);
        PowerMock.replayAll();

        committer.remove(taskId);

        PowerMock.verifyAll();
        PowerMock.resetAll();

        // Try to remove an existing task
        EasyMock.expect(committers.remove(taskId)).andReturn(task);
        EasyMock.expect(task.cancel(eq(false))).andReturn(false);
        EasyMock.expect(task.isDone()).andReturn(false);
        EasyMock.expect(task.get()).andReturn(null);
        PowerMock.replayAll();

        committer.remove(taskId);

        PowerMock.verifyAll();
        PowerMock.resetAll();

        // Try to remove a cancelled task
        EasyMock.expect(committers.remove(taskId)).andReturn(task);
        EasyMock.expect(task.cancel(eq(false))).andReturn(false);
        EasyMock.expect(task.isDone()).andReturn(false);
        EasyMock.expect(task.get()).andThrow(new CancellationException());
        mockLog.trace(EasyMock.anyString(), EasyMock.<Object>anyObject());
        PowerMock.expectLastCall();
        PowerMock.replayAll();

        committer.remove(taskId);

        PowerMock.verifyAll();
        PowerMock.resetAll();

        // Try to remove an interrupted task
        EasyMock.expect(committers.remove(taskId)).andReturn(task);
        EasyMock.expect(task.cancel(eq(false))).andReturn(false);
        EasyMock.expect(task.isDone()).andReturn(false);
        EasyMock.expect(task.get()).andThrow(new InterruptedException());
        PowerMock.replayAll();

        try {
            committer.remove(taskId);
            fail("Expected ConnectException to be raised");
        } catch (ConnectException e) {
            //ignore
        }

        PowerMock.verifyAll();
    }

}
