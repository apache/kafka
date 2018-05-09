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

import java.util.List;

import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A {@link SampledStat} that maintains a simple count of what it has seen.
 */
public class Count extends SampledStat {

    public Count() {
        super(0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += 1.0;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        for (Sample sample : samples)
            total += sample.value;
        return total;
    }

}
