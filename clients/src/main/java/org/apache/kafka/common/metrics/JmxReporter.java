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
package org.apache.kafka.common.metrics;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names
 */
public class JmxReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(JmxReporter.class);
    private static final Object LOCK = new Object();
    private String prefix;
    private final Map<String, KafkaMbean> mbeans = new HashMap<String, KafkaMbean>();

    public JmxReporter() {
        this("");
    }

    /**
     * Create a JMX reporter that prefixes all metrics with the given string.
     */
    public JmxReporter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void init(List<KafkaMetric> metrics) {
        synchronized (LOCK) {
            for (KafkaMetric metric : metrics)
                addAttribute(metric);
            for (KafkaMbean mbean : mbeans.values())
                reregister(mbean);
        }
    }

    boolean containsMbean(String mbeanName) {
        return mbeans.containsKey(mbeanName);
    }
    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            KafkaMbean mbean = addAttribute(metric);
            reregister(mbean);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            MetricName metricName = metric.metricName();
            String mBeanName = getMBeanName(prefix, metricName);
            KafkaMbean mbean = removeAttribute(metric, mBeanName);
            if (mbean != null) {
                if (mbean.metrics.isEmpty()) {
                    unregister(mbean);
                    mbeans.remove(mBeanName);
                } else
                    reregister(mbean);
            }
        }
    }

    private KafkaMbean removeAttribute(KafkaMetric metric, String mBeanName) {
        MetricName metricName = metric.metricName();
        KafkaMbean mbean = this.mbeans.get(mBeanName);
        if (mbean != null)
            mbean.removeAttribute(metricName.name());
        return mbean;
    }

    private KafkaMbean addAttribute(KafkaMetric metric) {
        try {
            MetricName metricName = metric.metricName();
            String mBeanName = getMBeanName(prefix, metricName);
            if (!this.mbeans.containsKey(mBeanName))
                mbeans.put(mBeanName, new KafkaMbean(mBeanName));
            KafkaMbean mbean = this.mbeans.get(mBeanName);
            mbean.setAttribute(metricName.name(), metric);
            return mbean;
        } catch (JMException e) {
            throw new KafkaException("Error creating mbean attribute for metricName :" + metric.metricName(), e);
        }
    }

    /**
     * @param metricName
     * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
     */
    static String getMBeanName(String prefix, MetricName metricName) {
        StringBuilder mBeanName = new StringBuilder();
        mBeanName.append(prefix);
        mBeanName.append(":type=");
        mBeanName.append(metricName.group());
        for (Map.Entry<String, String> entry : metricName.tags().entrySet()) {
            if (entry.getKey().length() <= 0 || entry.getValue().length() <= 0)
                continue;
            mBeanName.append(",");
            mBeanName.append(entry.getKey());
            mBeanName.append("=");
            mBeanName.append(Sanitizer.jmxSanitize(entry.getValue()));
        }
        return mBeanName.toString();
    }

    public void close() {
        synchronized (LOCK) {
            for (KafkaMbean mbean : this.mbeans.values())
                unregister(mbean);
        }
    }

    private void unregister(KafkaMbean mbean) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            if (server.isRegistered(mbean.name()))
                server.unregisterMBean(mbean.name());
        } catch (JMException e) {
            throw new KafkaException("Error unregistering mbean", e);
        }
    }

    private void reregister(KafkaMbean mbean) {
        unregister(mbean);
        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, mbean.name());
        } catch (JMException e) {
            throw new KafkaException("Error registering mbean " + mbean.name(), e);
        }
    }

    private static class KafkaMbean implements DynamicMBean {
        private final ObjectName objectName;
        private final Map<String, KafkaMetric> metrics;

        public KafkaMbean(String mbeanName) throws MalformedObjectNameException {
            this.metrics = new HashMap<>();
            this.objectName = new ObjectName(mbeanName);
        }

        public ObjectName name() {
            return objectName;
        }

        public void setAttribute(String name, KafkaMetric metric) {
            this.metrics.put(name, metric);
        }

        @Override
        public Object getAttribute(String name) throws AttributeNotFoundException, MBeanException, ReflectionException {
            if (this.metrics.containsKey(name))
                return this.metrics.get(name).metricValue();
            else
                throw new AttributeNotFoundException("Could not find attribute " + name);
        }

        @Override
        public AttributeList getAttributes(String[] names) {
            AttributeList list = new AttributeList();
            for (String name : names) {
                try {
                    list.add(new Attribute(name, getAttribute(name)));
                } catch (Exception e) {
                    log.warn("Error getting JMX attribute '{}'", name, e);
                }
            }
            return list;
        }

        public KafkaMetric removeAttribute(String name) {
            return this.metrics.remove(name);
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            return new LazyMBeanInfo(this.getClass().getName(), "", null, null, null) {
                @Override
                protected MBeanAttributeInfo[] buildAttributes() {
                    MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[metrics.size()];
                    int i = 0;
                    for (Map.Entry<String, KafkaMetric> entry : metrics.entrySet()) {
                        String attribute = entry.getKey();
                        KafkaMetric metric = entry.getValue();
                        attrs[i] = new MBeanAttributeInfo(attribute,
                            double.class.getName(),
                            metric.metricName().description(),
                            true,
                            false,
                            false);
                        i += 1;
                    }
                    return attrs;
                }
            };
        }

        @Override
        public Object invoke(String name, Object[] params, String[] sig) throws MBeanException, ReflectionException {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        @Override
        public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
                                                     InvalidAttributeValueException,
                                                     MBeanException,
                                                     ReflectionException {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        @Override
        public AttributeList setAttributes(AttributeList list) {
            throw new UnsupportedOperationException("Set not allowed.");
        }

    }

}
