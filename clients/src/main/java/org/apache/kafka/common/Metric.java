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
package org.apache.kafka.common;

/**
 * A metric tracked for monitoring purposes.
 */
public interface Metric {

    /**
     * A name for this metric
     */
    MetricName metricName();

    /**
     * The value of the metric as double if the metric is measurable and `0.0` otherwise.
     *
     * @deprecated As of 1.0.0, use {@link #metricValue()} instead. This will be removed in a future major release.
     */
    @Deprecated
    double value();

    /**
     * The value of the metric, which may be measurable or a non-measurable gauge
     */
    Object metricValue();

}
