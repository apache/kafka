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
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

public class ConsumerMetrics {
    
    public FetcherMetricsRegistry fetcherMetrics;
    
    public ConsumerMetrics(Set<String> metricsTags, String metricGrpPrefix) {
        this.fetcherMetrics = new FetcherMetricsRegistry(metricsTags, metricGrpPrefix);
    }

    public ConsumerMetrics(String metricGroupPrefix) {
        this(new HashSet<String>(), metricGroupPrefix);
    }

    private List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>(this.fetcherMetrics.getAllTemplates());
        return l;
    }

    public static void main(String[] args) {
        Set<String> tags = new HashSet<>();
        tags.add("client-id");
        ConsumerMetrics metrics = new ConsumerMetrics(tags, "consumer");
        System.out.println(Metrics.toHtmlTable("kafka.consumer", metrics.getAllTemplates()));
    }
}
