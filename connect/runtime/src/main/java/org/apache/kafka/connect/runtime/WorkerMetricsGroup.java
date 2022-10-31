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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Map;

class WorkerMetricsGroup {
    private final ConnectMetrics.MetricGroup metricGroup;
    private final Sensor connectorStartupAttempts;
    private final Sensor connectorStartupSuccesses;
    private final Sensor connectorStartupFailures;
    private final Sensor connectorStartupResults;
    private final Sensor taskStartupAttempts;
    private final Sensor taskStartupSuccesses;
    private final Sensor taskStartupFailures;
    private final Sensor taskStartupResults;

    public WorkerMetricsGroup(final Map<String, WorkerConnector> connectors, Map<ConnectorTaskId, WorkerTask> tasks, ConnectMetrics connectMetrics) {
        ConnectMetricsRegistry registry = connectMetrics.registry();
        metricGroup = connectMetrics.group(registry.workerGroupName());

        metricGroup.addValueMetric(registry.connectorCount, now -> (double) connectors.size());
        metricGroup.addValueMetric(registry.taskCount, now -> (double) tasks.size());

        MetricName connectorFailurePct = metricGroup.metricName(registry.connectorStartupFailurePercentage);
        MetricName connectorSuccessPct = metricGroup.metricName(registry.connectorStartupSuccessPercentage);
        Frequencies connectorStartupResultFrequencies = Frequencies.forBooleanValues(connectorFailurePct, connectorSuccessPct);
        connectorStartupResults = metricGroup.sensor("connector-startup-results");
        connectorStartupResults.add(connectorStartupResultFrequencies);

        connectorStartupAttempts = metricGroup.sensor("connector-startup-attempts");
        connectorStartupAttempts.add(metricGroup.metricName(registry.connectorStartupAttemptsTotal), new CumulativeSum());

        connectorStartupSuccesses = metricGroup.sensor("connector-startup-successes");
        connectorStartupSuccesses.add(metricGroup.metricName(registry.connectorStartupSuccessTotal), new CumulativeSum());

        connectorStartupFailures = metricGroup.sensor("connector-startup-failures");
        connectorStartupFailures.add(metricGroup.metricName(registry.connectorStartupFailureTotal), new CumulativeSum());

        MetricName taskFailurePct = metricGroup.metricName(registry.taskStartupFailurePercentage);
        MetricName taskSuccessPct = metricGroup.metricName(registry.taskStartupSuccessPercentage);
        Frequencies taskStartupResultFrequencies = Frequencies.forBooleanValues(taskFailurePct, taskSuccessPct);
        taskStartupResults = metricGroup.sensor("task-startup-results");
        taskStartupResults.add(taskStartupResultFrequencies);

        taskStartupAttempts = metricGroup.sensor("task-startup-attempts");
        taskStartupAttempts.add(metricGroup.metricName(registry.taskStartupAttemptsTotal), new CumulativeSum());

        taskStartupSuccesses = metricGroup.sensor("task-startup-successes");
        taskStartupSuccesses.add(metricGroup.metricName(registry.taskStartupSuccessTotal), new CumulativeSum());

        taskStartupFailures = metricGroup.sensor("task-startup-failures");
        taskStartupFailures.add(metricGroup.metricName(registry.taskStartupFailureTotal), new CumulativeSum());
    }

    void close() {
        metricGroup.close();
    }

    void recordConnectorStartupFailure() {
        connectorStartupAttempts.record(1.0);
        connectorStartupFailures.record(1.0);
        connectorStartupResults.record(0.0);
    }

    void recordConnectorStartupSuccess() {
        connectorStartupAttempts.record(1.0);
        connectorStartupSuccesses.record(1.0);
        connectorStartupResults.record(1.0);
    }

    void recordTaskFailure() {
        taskStartupAttempts.record(1.0);
        taskStartupFailures.record(1.0);
        taskStartupResults.record(0.0);
    }

    void recordTaskSuccess() {
        taskStartupAttempts.record(1.0);
        taskStartupSuccesses.record(1.0);
        taskStartupResults.record(1.0);
    }

    protected ConnectMetrics.MetricGroup metricGroup() {
        return metricGroup;
    }

    ConnectorStatus.Listener wrapStatusListener(ConnectorStatus.Listener delegateListener) {
        return new ConnectorStatusListener(delegateListener);
    }

    TaskStatus.Listener wrapStatusListener(TaskStatus.Listener delegateListener) {
        return new TaskStatusListener(delegateListener);
    }

    class ConnectorStatusListener implements ConnectorStatus.Listener {
        private final ConnectorStatus.Listener delegateListener;
        private volatile boolean startupSucceeded = false;

        ConnectorStatusListener(ConnectorStatus.Listener delegateListener) {
            this.delegateListener = delegateListener;
        }

        @Override
        public void onStartup(final String connector) {
            startupSucceeded = true;
            recordConnectorStartupSuccess();
            delegateListener.onStartup(connector);
        }

        @Override
        public void onPause(final String connector) {
            delegateListener.onPause(connector);
        }

        @Override
        public void onResume(final String connector) {
            delegateListener.onResume(connector);
        }

        @Override
        public void onFailure(final String connector, final Throwable cause) {
            if (!startupSucceeded) {
                recordConnectorStartupFailure();
            }
            delegateListener.onFailure(connector, cause);
        }

        @Override
        public void onRestart(String connector) {
            delegateListener.onRestart(connector);
        }

        @Override
        public void onShutdown(final String connector) {
            delegateListener.onShutdown(connector);
        }

        @Override
        public void onDeletion(final String connector) {
            delegateListener.onDeletion(connector);
        }
    }

    class TaskStatusListener implements TaskStatus.Listener {
        private final TaskStatus.Listener delegatedListener;
        private volatile boolean startupSucceeded = false;

        TaskStatusListener(TaskStatus.Listener delegatedListener) {
            this.delegatedListener = delegatedListener;
        }

        @Override
        public void onStartup(final ConnectorTaskId id) {
            recordTaskSuccess();
            startupSucceeded = true;
            delegatedListener.onStartup(id);
        }

        @Override
        public void onPause(final ConnectorTaskId id) {
            delegatedListener.onPause(id);
        }

        @Override
        public void onResume(final ConnectorTaskId id) {
            delegatedListener.onResume(id);
        }

        @Override
        public void onFailure(final ConnectorTaskId id, final Throwable cause) {
            if (!startupSucceeded) {
                recordTaskFailure();
            }
            delegatedListener.onFailure(id, cause);
        }

        @Override
        public void onRestart(ConnectorTaskId id) {
            delegatedListener.onRestart(id);
        }

        @Override
        public void onShutdown(final ConnectorTaskId id) {
            delegatedListener.onShutdown(id);
        }

        @Override
        public void onDeletion(final ConnectorTaskId id) {
            delegatedListener.onDeletion(id);
        }
    }

}
