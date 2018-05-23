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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectMetricsRegistry;
import org.apache.kafka.connect.util.ConnectorTaskId;

public class ErrorHandlingMetrics {

    private final Time time = new SystemTime();

    private final ConnectMetrics.MetricGroup metricGroup;

    // metrics
    private final Sensor recordProcessingFailures;
    private final Sensor recordProcessingErrors;
    private final Sensor recordsSkipped;
    private final Sensor retries;
    private final Sensor errorsLogged;
    private final Sensor dlqProduceRequests;
    private final Sensor dlqProduceFailures;
    private long lastErrorTime = 0;

    public ErrorHandlingMetrics(ConnectorTaskId id, ConnectMetrics connectMetrics) {

        ConnectMetricsRegistry registry = connectMetrics.registry();
        metricGroup = connectMetrics.group(registry.taskErrorHandlingGroupName(),
                registry.connectorTagName(), id.connector(), registry.taskTagName(), Integer.toString(id.task()));

        // prevent collisions by removing any previously created metrics in this group.
        metricGroup.close();

        recordProcessingFailures = metricGroup.sensor("processing-failures");
        recordProcessingFailures.add(metricGroup.metricName(registry.recordProcessingFailures), new Total());

        recordProcessingErrors = metricGroup.sensor("processing-errors");
        recordProcessingErrors.add(metricGroup.metricName(registry.recordProcessingErrors), new Total());

        recordsSkipped = metricGroup.sensor("record-skipped");
        recordsSkipped.add(metricGroup.metricName(registry.recordsSkipped), new Total());

        retries = metricGroup.sensor("retries");
        retries.add(metricGroup.metricName(registry.retries), new Total());

        errorsLogged = metricGroup.sensor("errors-logged");
        errorsLogged.add(metricGroup.metricName(registry.errorsLogged), new Total());

        dlqProduceRequests = metricGroup.sensor("deadletterqueue-produce-requests");
        dlqProduceRequests.add(metricGroup.metricName(registry.dlqProduceRequests), new Total());

        dlqProduceFailures = metricGroup.sensor("deadletterqueue-produce-failures");
        dlqProduceFailures.add(metricGroup.metricName(registry.dlqProduceFailures), new Total());

        metricGroup.addValueMetric(registry.lastErrorTimestamp, now -> lastErrorTime);
    }

    public void recordFailure() {
        recordProcessingFailures.record();
    }

    public void recordError() {
        recordProcessingErrors.record();
    }

    public void recordSkipped() {
        recordsSkipped.record();
    }

    public void recordRetry() {
        retries.record();
    }

    public void recordErrorLogged() {
        errorsLogged.record();
    }

    public void recordDeadLetterQueueProduceRequest() {
        dlqProduceRequests.record();
    }

    public void recordDeadLetterQueueProduceFailed() {
        dlqProduceFailures.record();
    }

    public void recordErrorTimestamp() {
        this.lastErrorTime = time.milliseconds();
    }

    public ConnectMetrics.MetricGroup metricGroup() {
        return metricGroup;
    }
}
