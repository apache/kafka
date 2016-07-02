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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class RecordAccumulatorMetricsRegistry {

    private String groupName;
    public MetricNameTemplate waitingThreads;
    public MetricNameTemplate bufferTotalBytes;
    public MetricNameTemplate bufferAvailableBytes;
    public MetricNameTemplate bufferExhaustedRate;
    public BufferPoolMetricsRegistry bufferPoolMetrics;

    public RecordAccumulatorMetricsRegistry() {
        this(new HashSet<String>());
    }
    
    public RecordAccumulatorMetricsRegistry(Set<String> tags) {

        this.groupName = "producer-metrics";

        this.waitingThreads = new MetricNameTemplate("waiting-threads", groupName, "The number of user threads blocked waiting for buffer memory to enqueue their records", tags);
        this.bufferTotalBytes = new MetricNameTemplate("buffer-total-bytes", groupName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).", tags);
        this.bufferAvailableBytes = new MetricNameTemplate("buffer-available-bytes", groupName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).", tags);
        this.bufferExhaustedRate = new MetricNameTemplate("buffer-exhausted-rate", groupName, "The average per-second number of record sends that are dropped due to buffer exhaustion", tags);
        
        this.bufferPoolMetrics = new BufferPoolMetricsRegistry(tags);
    }

    public List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>();
        l.addAll(this.bufferPoolMetrics.getAllTemplates());
        l.addAll(Arrays.asList(this.waitingThreads,
                this.bufferTotalBytes,
                this.bufferAvailableBytes,
                this.bufferExhaustedRate
                ));
        return l;
    }


}
