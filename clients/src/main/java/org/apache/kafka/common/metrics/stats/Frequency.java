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


import org.apache.kafka.common.MetricName;

/**
 * Definition of a frequency metric used in a {@link Frequencies} compound statistic.
 */
public class Frequency {

    private final MetricName name;
    private final double centerValue;

    /**
     * Create an instance with the given name and center point value.
     *
     * @param name        the name of the frequency metric; may not be null
     * @param centerValue the value identifying the {@link Frequencies} bucket to be reported
     */
    public Frequency(MetricName name, double centerValue) {
        this.name = name;
        this.centerValue = centerValue;
    }

    /**
     * Get the name of this metric.
     *
     * @return the metric name; never null
     */
    public MetricName name() {
        return this.name;
    }

    /**
     * Get the value of this metrics center point.
     *
     * @return the center point value
     */
    public double centerValue() {
        return this.centerValue;
    }

    @Override
    public String toString() {
        return "Frequency(" +
            "name=" + name +
            ", centerValue=" + centerValue +
            ')';
    }
}
