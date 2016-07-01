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
package org.apache.kafka.clients.producer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.SelectorMetricsRegistry;

public class ProducerMetrics {

    public SelectorMetricsRegistry selectorMetrics;

    public ProducerMetrics(Set<String> keySet, String metricGrpPrefix) {
        this.selectorMetrics = new SelectorMetricsRegistry(keySet, metricGrpPrefix);
    }

    private List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>();
        l.addAll(this.selectorMetrics.getAllTemplates());
        return l;
    }

    public static void main(String[] args) {
        Set<String> tags = new HashSet<>();
        tags.add("client-id");
        ProducerMetrics metrics = new ProducerMetrics(tags, "producer");
        System.out.println(Metrics.toHtmlTable("kafka.producer", metrics.getAllTemplates()));
    }

}