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

public class StreamsThreadTotalBlockedTime {
    final Consumer<?, ?> consumer;
    final Consumer<?, ?> restoreConsumer;
    final Supplier<Double> producerTotalBlockedTime;

    StreamsThreadTotalBlockedTime(
        final Consumer<?, ?> consumer,
        final Consumer<?, ?> restoreConsumer,
        final Supplier<Double> producerTotalBlockedTime) {
        this.consumer = consumer;
        this.restoreConsumer = restoreConsumer;
        this.producerTotalBlockedTime = producerTotalBlockedTime;
    }

    final double getMetricValue(
        final Map<MetricName, ? extends Metric> metrics,
        final String name) {
        return metrics.keySet().stream()
            .filter(n -> n.name().equals(name))
            .findFirst()
            .map(n -> (Double) metrics.get(n).metricValue())
            .orElse(0.0);
    }

    public double getTotalBlockedTime() {
        return getMetricValue(consumer.metrics(), "io-waittime-total")
            + getMetricValue(consumer.metrics(), "iotime-total")
            + getMetricValue(consumer.metrics(), "committed-time-total")
            + getMetricValue(consumer.metrics(), "commit-sync-time-total")
            + getMetricValue(restoreConsumer.metrics(), "io-waittime-total")
            + getMetricValue(restoreConsumer.metrics(), "iotime-total")
            + producerTotalBlockedTime.get();
    }
}
