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
 * A simple rate the rate is incrementally calculated
 * based on the elapsed time between the earliest reading
 * and now.
 *
 * An exception is made for the first window, which is
 * considered of fixed size. This avoids the issue of
 * an artificially high rate when the gap between readings
 * is close to 0.
 */
public class SimpleRate extends Rate {

    @Override
    public long windowSize(MetricConfig config, long now) {
        stat.purgeObsoleteSamples(config, now);
        long elapsed = now - stat.oldest(now).lastWindowMs;
        return Math.max(elapsed, config.timeWindowMs());
    }
}
