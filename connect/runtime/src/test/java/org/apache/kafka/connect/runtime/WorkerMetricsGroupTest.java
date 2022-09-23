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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class WorkerMetricsGroupTest {
    private final String connector = "org.FakeConnector";
    private final ConnectorTaskId task = new ConnectorTaskId(connector, 0);
    private final RuntimeException exception = new RuntimeException();

    @Mock private ConnectMetrics connectMetrics;
    
    private Sensor connectorStartupResults;
    private Sensor connectorStartupAttempts;
    private Sensor connectorStartupSuccesses;
    private Sensor connectorStartupFailures;

    private Sensor taskStartupResults;
    private Sensor taskStartupAttempts;
    private Sensor taskStartupSuccesses;
    private Sensor taskStartupFailures;

    @Mock private ConnectorStatus.Listener delegateConnectorListener;
    @Mock private TaskStatus.Listener delegateTaskListener;
    @Mock private ConnectMetricsRegistry connectMetricsRegistry;
    @Mock private ConnectMetrics.MetricGroup metricGroup;
    @Mock private MetricName metricName;

    @Before
    public void setup() {
        // We don't expect metricGroup.metricName to be invoked with null in practice,
        // but it's easier to test this way, and should have no impact
        // on the efficacy of these tests
        when(metricGroup.metricName((MetricNameTemplate) isNull())).thenReturn(metricName);
        when(connectMetricsRegistry.workerGroupName()).thenReturn(ConnectMetricsRegistry.WORKER_GROUP_NAME);
        when(connectMetrics.registry()).thenReturn(connectMetricsRegistry);
        when(connectMetrics.group(ConnectMetricsRegistry.WORKER_GROUP_NAME)).thenReturn(metricGroup);

        connectorStartupResults = mockSensor(metricGroup, "connector-startup-results");
        connectorStartupAttempts = mockSensor(metricGroup, "connector-startup-attempts");
        connectorStartupSuccesses = mockSensor(metricGroup, "connector-startup-successes");
        connectorStartupFailures = mockSensor(metricGroup, "connector-startup-failures");

        taskStartupResults = mockSensor(metricGroup, "task-startup-results");
        taskStartupAttempts = mockSensor(metricGroup, "task-startup-attempts");
        taskStartupSuccesses = mockSensor(metricGroup, "task-startup-successes");
        taskStartupFailures = mockSensor(metricGroup, "task-startup-failures");
    }

    private Sensor mockSensor(ConnectMetrics.MetricGroup metricGroup, String name) {
        Sensor sensor = mock(Sensor.class);
        when(metricGroup.sensor(name)).thenReturn(sensor);
        when(sensor.add(any(CompoundStat.class))).thenReturn(true);
        when(sensor.add(any(MetricName.class), any(CumulativeSum.class))).thenReturn(true);
        return sensor;
    }
    
    @Test
    public void testConnectorStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);

        connectorListener.onStartup(connector);

        verifyRecordConnectorStartupSuccess();
        verify(delegateConnectorListener).onStartup(connector);
    }

    @Test
    public void testConnectorFailureAfterStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);

        connectorListener.onStartup(connector);
        connectorListener.onFailure(connector, exception);

        verify(delegateConnectorListener).onStartup(connector);
        verifyRecordConnectorStartupSuccess();
        verify(delegateConnectorListener).onFailure(connector, exception);
        // recordConnectorStartupFailure() should not be called if failure happens after a successful startup.
        verify(connectorStartupFailures, never()).record(anyDouble());
    }

    @Test
    public void testConnectorFailureBeforeStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final ConnectorStatus.Listener connectorListener = workerMetricsGroup.wrapStatusListener(delegateConnectorListener);
        
        connectorListener.onFailure(connector, exception);

        verify(delegateConnectorListener).onFailure(connector, exception);
        verifyRecordConnectorStartupFailure();
    }

    @Test
    public void testTaskStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onStartup(task);

        verify(delegateTaskListener).onStartup(task);
        verifyRecordTaskSuccess();
    }
    
    @Test
    public void testTaskFailureAfterStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onStartup(task);
        taskListener.onFailure(task, exception);

        verify(delegateTaskListener).onStartup(task);
        verifyRecordTaskSuccess();
        verify(delegateTaskListener).onFailure(task, exception);
        // recordTaskFailure() should not be called if failure happens after a successful startup.
        verify(taskStartupFailures, never()).record(anyDouble());
    }

    @Test
    public void testTaskFailureBeforeStartupRecordedMetrics() {
        WorkerMetricsGroup workerMetricsGroup = new WorkerMetricsGroup(new HashMap<>(), new HashMap<>(), connectMetrics);
        final TaskStatus.Listener taskListener = workerMetricsGroup.wrapStatusListener(delegateTaskListener);

        taskListener.onFailure(task, exception);

        verify(delegateTaskListener).onFailure(task, exception);
        verifyRecordTaskFailure();
    }

    private void verifyRecordTaskFailure() {
        verify(taskStartupAttempts).record(1.0);
        verify(taskStartupFailures).record(1.0);
        verify(taskStartupResults).record(0.0);
    }

    private void verifyRecordTaskSuccess() {
        verify(taskStartupAttempts).record(1.0);
        verify(taskStartupSuccesses).record(1.0);
        verify(taskStartupResults).record(1.0);
    }

    private void verifyRecordConnectorStartupSuccess() {
        verify(connectorStartupAttempts).record(1.0);
        verify(connectorStartupSuccesses).record(1.0);
        verify(connectorStartupResults).record(1.0);
    }

    private void verifyRecordConnectorStartupFailure() {
        verify(connectorStartupAttempts).record(1.0);
        verify(connectorStartupFailures).record(1.0);
        verify(connectorStartupResults).record(0.0);
    }
}
