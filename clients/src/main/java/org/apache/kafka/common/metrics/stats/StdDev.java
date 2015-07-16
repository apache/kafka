/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import java.util.List;

import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A {@link SampledStat} that maintains a simple standard deviation over its samples.
 */
public class StdDev extends SampledStat {

    public StdDev() {
        super(0.0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.secondValue += value * value;
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        double totalSquare = 0.0;
        long count = 0;
        for (Sample s:samples) {
            total += s.value;
            totalSquare += s.secondValue;
            count += s.eventCount;
        }
        double mean = total / count;
        return Math.sqrt(totalSquare / count - mean * mean);
    }

}
