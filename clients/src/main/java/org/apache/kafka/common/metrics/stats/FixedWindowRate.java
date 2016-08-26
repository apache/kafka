/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * A rate that uses a window which is always an exact multiple
 * of the configured window size, even when there are fewer
 * windows than samples.
 *
 * This rate is thus conservative. If we get a set of values
 * early in a window, they will be represented over the full
 * window's duration.
 */
public class FixedWindowRate extends Rate {

    @Override
    public long windowSize(MetricConfig config, long now) {
        stat.purgeObsoleteSamples(config, now);
        long size = now - stat.oldest(now).lastWindowMs;
        return roundUp(size, config.timeWindowMs());
    }

    long roundUp(long n, long window) {
        long result = window;
        if (n % window != 0)
            result = ((n / window) + 1) * window;
        return result;
    }
}
