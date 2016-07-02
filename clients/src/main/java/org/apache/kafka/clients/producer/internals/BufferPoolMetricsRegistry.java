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
package org.apache.kafka.clients.producer.internals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class BufferPoolMetricsRegistry {

    private String groupName;
    public MetricNameTemplate bufferpoolWaitRatio;

    public BufferPoolMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<String>(), "producer-metrics");
    }
    
    public BufferPoolMetricsRegistry(Set<String> tags) {
        this(tags, "producer-metrics");
    }

    public BufferPoolMetricsRegistry(Set<String> tags, String metricsGrpName) {
        this.groupName = metricsGrpName;
        
        this.bufferpoolWaitRatio = new MetricNameTemplate("bufferpool-wait-ratio",
                groupName,
                "The fraction of time an appender waits for space allocation.", tags);

    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(this.bufferpoolWaitRatio);
    }

}
