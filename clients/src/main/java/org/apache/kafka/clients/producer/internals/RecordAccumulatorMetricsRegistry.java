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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

public class RecordAccumulatorMetricsRegistry extends MetricsRegistry {

    final static String metricGrpName = "producer-metrics";

    public final MetricName waitingThreads;
    public final MetricName bufferTotalBytes;
    public final MetricName bufferAvailableBytes;
    public final MetricName bufferExhaustedRate;
    public final MetricName bufferExhaustedTotal;

    public RecordAccumulatorMetricsRegistry(Metrics metrics) {
        super(metrics);

        this.waitingThreads = createMetricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records", this.tags);
        this.bufferTotalBytes = createMetricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).", tags);
        this.bufferAvailableBytes = createMetricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).", tags);
        this.bufferExhaustedRate = createMetricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion", tags);
        this.bufferExhaustedTotal = createMetricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion", tags);
    }

}
