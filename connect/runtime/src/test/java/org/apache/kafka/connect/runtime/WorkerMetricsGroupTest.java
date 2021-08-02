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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.powermock.api.easymock.PowerMock.expectLastCall;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Sensor.class, MetricName.class})
public class WorkerMetricsGroupTest {
    private final String connector = "org.FakeConnector";
    private final ConnectorTaskId task = new ConnectorTaskId(connector, 0);
    private final RuntimeException exception = new RuntimeException();

    private ConnectMetrics connectMetrics;
    
    private Sensor connectorStartupResults;
    private Sensor connectorStartupAttempts;
    private Sensor connectorStartupSuccesses;
    private Sensor connectorStartupFailures;

    private Sensor taskStartupResults;
    private Sensor taskStartupAttempts;
    private Sensor taskStartupSuccesses;
    private Sensor taskStartupFailures;

    private ConnectorStatus.Listener delegateConnectorListener;
    private TaskStatus.Listener delegateTaskListener;

    @Before
    public void setup() {
        connectMetrics = PowerMock.createMock(ConnectMetrics.class);
        ConnectMetricsRegistry connectMetricsRegistry = PowerMock.createNiceMock(ConnectMetricsRegistry.class);
        ConnectMetrics.MetricGroup metricGroup = PowerMock.createNiceMock(ConnectMetrics.MetricGroup.class);

        connectMetrics.registry();
        expectLastCall().andReturn(connectMetricsRegistry);

        connectMetrics.group(anyString());
        expectLastCall().andReturn(metricGroup);

        MetricName metricName = PowerMock.createMock(MetricName.class);
        metricGroup.metricName(anyObject(MetricNameTemplate.class));
        expectLastCall().andStubReturn(metricName);

        connectorStartupResults = mockSensor(metricGroup, "connector-startup-results");
        connectorStartupAttempts = mockSensor(metricGroup, "connector-startup-attempts");
        connectorStartupSuccesses = mockSensor(metricGroup, "connector-startup-successes");
        connectorStartupFailures = mockSensor(metricGroup, "connector-startup-failures");

        taskStartupResults = mockSensor(metricGroup, "task-startup-results");
        taskStartupAttempts = mockSensor(metricGroup, "task-startup-attempts");
        taskStartupSuccesses = mockSensor(metricGroup, "task-startup-successes");
        taskStartupFailures = mockSensor(metricGroup, "task-startup-failures");

        delegateConnectorListener = PowerMock.createStrictMock(ConnectorStatus.Listener.class);
        delegateTaskListener = PowerMock.createStrictMock(TaskStatus.Listener.class);
    }

    private Sensor mockSensor(ConnectMetrics.MetricGroup metricGroup, String name) {
        Sensor sensor = PowerMock.createMock(Sensor.class);
        metricGroup.sensor(eq(name));
        expectLastCall().andReturn(sensor);

        sensor.add(anyObject(CompoundStat.class));
        expectLastCall().andStubReturn(true);

        sensor.add(anyObject(MetricName.class), anyObject(CumulativeSum.class));
        expectLastCall().andStubReturn(true);

        return sensor;
    }
    
    @Test
    public void testConnectorStartupRecordedMetrics() {
        delegateConnectorListener.onStartup(eq(connector));
        expectLastCall();

        connectorStartupAttempts.record(eq(1.0));
        expectLastCall();
        connectorStartupSuccesses.record(eq(1.0));
        expectLastCall();
        connectorStartupResults.record(eq(1.0));
        expectLastCall();

        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);

        connectorListener.onStartup(connector);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorFailureAfterStartupRecordedMetrics() {
        delegateConnectorListener.onStartup(eq(connector));
        expectLastCall();

        connectorStartupAttempts.record(eq(1.0));
        expectLastCall();
        connectorStartupSuccesses.record(eq(1.0));
        expectLastCall();
        connectorStartupResults.record(eq(1.0));
        expectLastCall();
        
        delegateConnectorListener.onFailure(eq(connector), eq(exception));
        expectLastCall();

        // recordConnectorStartupFailure() should not be called if failure happens after a successful startup

        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);

        connectorListener.onStartup(connector);
        connectorListener.onFailure(connector, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorFailureBeforeStartupRecordedMetrics() {
        delegateConnectorListener.onFailure(eq(connector), eq(exception));
        expectLastCall();

        connectorStartupAttempts.record(eq(1.0));
        expectLastCall();
        connectorStartupFailures.record(eq(1.0));
        expectLastCall();
        connectorStartupResults.record(eq(0.0));
        expectLastCall();
        
        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);
        
        connectorListener.onFailure(connector, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskStartupRecordedMetrics() {
        delegateTaskListener.onStartup(eq(task));
        expectLastCall();

        taskStartupAttempts.record(eq(1.0));
        expectLastCall();
        taskStartupSuccesses.record(eq(1.0));
        expectLastCall();
        taskStartupResults.record(eq(1.0));
        expectLastCall();

        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onStartup(task);

        PowerMock.verifyAll();
    }
    
    @Test
    public void testTaskFailureAfterStartupRecordedMetrics() {
        delegateTaskListener.onStartup(eq(task));
        expectLastCall();

        taskStartupAttempts.record(eq(1.0));
        expectLastCall();
        taskStartupSuccesses.record(eq(1.0));
        expectLastCall();
        taskStartupResults.record(eq(1.0));
        expectLastCall();

        delegateTaskListener.onFailure(eq(task), eq(exception));
        expectLastCall();

        // recordTaskFailure() should not be called if failure happens after a successful startup

        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onStartup(task);
        taskListener.onFailure(task, exception);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskFailureBeforeStartupRecordedMetrics() {
        delegateTaskListener.onFailure(eq(task), eq(exception));
        expectLastCall();

        taskStartupAttempts.record(eq(1.0));
        expectLastCall();
        taskStartupFailures.record(eq(1.0));
        expectLastCall();
        taskStartupResults.record(eq(0.0));
        expectLastCall();

        PowerMock.replayAll();

        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onFailure(task, exception);

        PowerMock.verifyAll();
    }

}
