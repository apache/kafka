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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.InternalMockProcessorContext;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Simple in-memory segments implementation for testing AbstractSegments.
 */
class TestSegments extends AbstractSegments<TestSegment> {

    TestSegments(final String name,
                 final long retentionPeriod,
                 final long segmentInterval) {
        super(name, retentionPeriod, segmentInterval);
    }

    @Override
    protected TestSegment createSegment(final long segmentId, final String segmentName) {
        return new TestSegment(segmentName, segmentId);
    }

    @Override
    protected void openSegmentDB(final TestSegment segment, final StateStoreContext context) {
        segment.openDB(context.appConfigs(), context.stateDir());
    }

    static void assertMetricExists(final String metricName,
                                    final String storeName,
                                    final String metricsScope,
                                    final InternalMockProcessorContext<?, ?> context) {
        final StreamsMetricsImpl metricsImpl = ProcessorContextUtils.metricsImpl(context);
        final Map<String, String> tags = new LinkedHashMap<>();
        tags.put("thread-id", Thread.currentThread().getName());
        tags.put("task-id", context.taskId().toString());
        tags.put(metricsScope + "-state-id", storeName);

        final MetricName name = metricsImpl.metricsRegistry().metricName(
            metricName,
            StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
            "Metrics for RocksDB store",
            tags
        );

        final KafkaMetric metric = (KafkaMetric) metricsImpl.metrics().get(name);
        assertNotNull(metric, "Metric '" + metricName + "' should be registered after init()");
    }
}
