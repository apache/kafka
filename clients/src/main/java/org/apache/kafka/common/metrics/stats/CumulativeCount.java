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
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A non-sampled version of {@link WindowedCount} maintained over all time.
 *
 * This is a special kind of {@link CumulativeSum} that always records {@code 1} instead of the provided value.
 * In other words, it counts the number of
 * {@link CumulativeCount#record(MetricConfig, double, long)} invocations,
 * instead of summing the recorded values.
 */
public class CumulativeCount extends CumulativeSum {
    @Override
    public void record(final MetricConfig config, final double value, final long timeMs) {
        super.record(config, 1, timeMs);
    }
}
