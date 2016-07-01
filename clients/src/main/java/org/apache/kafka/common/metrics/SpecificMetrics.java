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
package org.apache.kafka.common.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Time;

/**
 *         A metrics factory that lets you generate metrics based on a template.
 *         The template is used to generate documentation.
 */
public class SpecificMetrics extends Metrics {

    public SpecificMetrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        super(defaultConfig, reporters, time);
    }
    
    public SpecificMetrics(Time time) {
        super(time);
    }
    
    public SpecificMetrics() {
        super();
    }
    
    public SpecificMetrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time,
            boolean enableExpiration) {
        super(defaultConfig, reporters, time, enableExpiration);

    }

    private static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new HashMap<String, String>();

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

    /**
     * Deprecated this simply so that I could drop this class in as a
     * replacement for Metrics, but have my IDE tell me all the references that
     * are instantiating metrics directly, rather than using references to my
     * instances.
     */
    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description, Map<String, String> tags) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description, tags);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group) {
        // TODO Auto-generated method stub
        return super.metricName(name, group);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, String description, String... keyValue) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, description, keyValue);
    }

    @Deprecated
    @Override
    public MetricName metricName(String name, String group, Map<String, String> tags) {
        // TODO Auto-generated method stub
        return super.metricName(name, group, tags);
    }

}
