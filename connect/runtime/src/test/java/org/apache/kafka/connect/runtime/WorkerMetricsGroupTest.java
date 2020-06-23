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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.eq;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.expectLastCall;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Sensor.class})
public class WorkerMetricsGroupTest {
    private final String connector = "org.FakeConnector";
    private final ConnectorTaskId task = new ConnectorTaskId(connector, 0);
    private final RuntimeException exception = new RuntimeException();

    @Test
    public void testConnectorStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final ConnectorStatus.Listener delegate = createStrictMock(ConnectorStatus.Listener.class);
        final WorkerMetricsGroup.ConnectorStatusListener connectorListener = mockWorkerMetricsGroup.new ConnectorStatusListener(delegate);

        delegate.onStartup(connector);
        expectLastCall();

        mockWorkerMetricsGroup.recordConnectorStartupSuccess();
        expectLastCall();

        PowerMock.replayAll();

        connectorListener.onStartup(connector);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorFailureAfterStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final ConnectorStatus.Listener delegate = createStrictMock(ConnectorStatus.Listener.class);
        final WorkerMetricsGroup.ConnectorStatusListener connectorListener = mockWorkerMetricsGroup.new ConnectorStatusListener(delegate);

        delegate.onStartup(eq(connector));
        expectLastCall();

        delegate.onFailure(eq(connector), eq(exception));
        expectLastCall();

        mockWorkerMetricsGroup.recordConnectorStartupSuccess();
        expectLastCall();

        // mockWorkerMetricsGroup.recordConnectorStartupFailure() should not be called if failure happens after a successful startup

        PowerMock.replayAll();

        connectorListener.onStartup(connector);
        connectorListener.onFailure(connector, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorFailureBeforeStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final ConnectorStatus.Listener delegate = createStrictMock(ConnectorStatus.Listener.class);
        final WorkerMetricsGroup.ConnectorStatusListener connectorListener = mockWorkerMetricsGroup.new ConnectorStatusListener(delegate);

        delegate.onFailure(eq(connector), eq(exception));
        expectLastCall();

        mockWorkerMetricsGroup.recordConnectorStartupFailure();
        expectLastCall();

        PowerMock.replayAll();

        connectorListener.onFailure(connector, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final TaskStatus.Listener delegate = createStrictMock(TaskStatus.Listener.class);
        final WorkerMetricsGroup.TaskStatusListener taskListener = mockWorkerMetricsGroup.new TaskStatusListener(delegate);

        delegate.onStartup(task);
        expectLastCall();

        mockWorkerMetricsGroup.recordTaskSuccess();
        expectLastCall();

        PowerMock.replayAll();

        taskListener.onStartup(task);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskFailureAfterStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final TaskStatus.Listener delegate = createStrictMock(TaskStatus.Listener.class);
        final WorkerMetricsGroup.TaskStatusListener taskListener = mockWorkerMetricsGroup.new TaskStatusListener(delegate);

        delegate.onStartup(eq(task));
        expectLastCall();

        delegate.onFailure(eq(task), eq(exception));
        expectLastCall();

        mockWorkerMetricsGroup.recordTaskSuccess();
        expectLastCall();

        // mockWorkerMetricsGroup.recordTaskFailure() should not be called if failure happens after a successful startup

        PowerMock.replayAll();

        taskListener.onStartup(task);
        taskListener.onFailure(task, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskFailureBeforeStartupRecordedMetrics() {
        final WorkerMetricsGroup mockWorkerMetricsGroup = createStrictMock(WorkerMetricsGroup.class);
        final TaskStatus.Listener delegate = createStrictMock(TaskStatus.Listener.class);
        final WorkerMetricsGroup.TaskStatusListener taskListener = mockWorkerMetricsGroup.new TaskStatusListener(delegate);

        delegate.onFailure(eq(task), eq(exception));
        expectLastCall();

        mockWorkerMetricsGroup.recordTaskFailure();
        expectLastCall();

        PowerMock.replayAll();

        taskListener.onFailure(task, exception);

        PowerMock.verifyAll();
    }

}
