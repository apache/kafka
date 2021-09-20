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

package org.apache.kafka.streams.processor.internals;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

public class StreamThreadTotalBlockedTime {
    private final Consumer<?, ?> consumer;
    private final Consumer<?, ?> restoreConsumer;
    private final Supplier<Double> producerTotalBlockedTime;

    StreamThreadTotalBlockedTime(
        final Consumer<?, ?> consumer,
        final Consumer<?, ?> restoreConsumer,
        final Supplier<Double> producerTotalBlockedTime) {
        this.consumer = consumer;
        this.restoreConsumer = restoreConsumer;
        this.producerTotalBlockedTime = producerTotalBlockedTime;
    }

    private double metricValue(
        final Map<MetricName, ? extends Metric> metrics,
        final String name) {
        return metrics.keySet().stream()
            .filter(n -> n.name().equals(name))
            .findFirst()
            .map(n -> (Double) metrics.get(n).metricValue())
            .orElse(0.0);
    }

    public double compute() {
        return metricValue(consumer.metrics(), "io-wait-time-ns-total")
            + metricValue(consumer.metrics(), "io-time-ns-total")
            + metricValue(consumer.metrics(), "committed-time-ns-total")
            + metricValue(consumer.metrics(), "commit-sync-time-ns-total")
            + metricValue(restoreConsumer.metrics(), "io-wait-time-ns-total")
            + metricValue(restoreConsumer.metrics(), "io-time-ns-total")
            + producerTotalBlockedTime.get();
    }
}
