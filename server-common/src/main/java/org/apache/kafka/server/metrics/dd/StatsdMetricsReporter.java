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

package org.apache.kafka.server.metrics.dd;

import com.timgroup.statsd.NonBlockingStatsDClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsdMetricsReporter implements MetricsReporter {

    private static final String METRIC_NAME = "dd.kafka.produce.time";

    private static final Logger log = LoggerFactory.getLogger(StatsdMetricsReporter.class);

    private final ScheduledExecutorService executor;

    private static final Duration PERIOD = Duration.ofSeconds(5);

    public StatsdMetricsReporter() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Runnable emitter = new Runnable() {
            @Override
            public void run() {
                Snapshot snapshot = DatadogMetrics.getInstance().getSnapshot();
                // Could be a race condition here and some measurements could be lost
                // but at the usual rate of produce requests this is okay
                double samplingRate = snapshot.getSamplingRate();
                log.info("Sampling rate {}", samplingRate);
                DatadogMetrics.getInstance().clear();
                NonBlockingStatsDClient statsd = StatsDClient.getInstance();
                for (long v : snapshot.getValues()) {
                    statsd.recordDistributionValue(METRIC_NAME, v, samplingRate);
                }
            }
        };
        executor.scheduleAtFixedRate(emitter, PERIOD.getSeconds(), PERIOD.getSeconds(), TimeUnit.SECONDS);
        log.info("Configured StatsdReporter to report every {} seconds", PERIOD.getSeconds());
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
    }

    @Override
    public void metricChange(KafkaMetric metric) {
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
    }

    @Override
    public void close() {
        if (executor != null)
            executor.shutdown();
    }
}
