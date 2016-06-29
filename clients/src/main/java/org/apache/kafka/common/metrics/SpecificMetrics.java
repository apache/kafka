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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
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

    public MetricName metricInstance(MetricNameTemplate template, String... keyValue) {
        return metricInstance(template, getTags(keyValue));
    }

    private static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new HashMap<String, String>();

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        // check to make sure that the runtime defined tags contain all the template tags.
        Set<String> runtimeTagKeys = new HashSet<>(tags.keySet());
        runtimeTagKeys.addAll(config().tags().keySet());
        
        Set<String> templateTagKeys = template.tags();
        
        if (!runtimeTagKeys.equals(templateTagKeys)) {
            throw new IllegalArgumentException("For '" + template.name() + "', runtime-defined metric tags do not match compile-time defined metric tags. " + ""
                    + "Runtime = " + runtimeTagKeys.toString() + " Compile-time = " + templateTagKeys.toString() );
        }
                
        return super.metricName(template.name(), template.group(), template.description(), tags);
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

    public static String toHtmlTable(String domain, MetricNameTemplate[] allMetrics) {
        Map<String, Map<String, String>> beansAndAttributes = new HashMap<String, Map<String, String>>();

        try (Metrics metrics = new Metrics() ) {
            for (MetricNameTemplate template : allMetrics) {
                Map<String, String> tags = new HashMap<String, String>();
                for (String s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }

                MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(), tags);
                String mBeanName = JmxReporter.getMBeanName(domain, metricName);
                if (!beansAndAttributes.containsKey(mBeanName)) {
                    beansAndAttributes.put(mBeanName, new HashMap<String, String>());
                }
                Map<String, String> attrAndDesc = beansAndAttributes.get(mBeanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException("mBean '" + mBeanName + "' attribute '" + template.name() + "' is defined twice.");
                }
            }
        }
        
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        b.append("<th>Mbean name</th>\n");
        b.append("<th>Attribute name</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>\n");

        for (Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            b.append("<tr>\n");
            b.append("<td colspan=3>");
            b.append(e.getKey());
            b.append("</td>");
            b.append("</tr>\n");

            for (Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td></td>");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>");
                b.append("</tr>\n");
            }

        }
        b.append("</tbody></table>");

        return b.toString();

    }

}
