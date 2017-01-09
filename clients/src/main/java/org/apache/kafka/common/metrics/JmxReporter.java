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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names.
 */
public class JmxReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(JmxReporter.class);
    private static final Object LOCK = new Object();
    private final String prefix;
    private final Map<ObjectName, KafkaMbean> mbeans = new HashMap<>();

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
            for (KafkaMetric metric : metrics) {
                final KafkaMbean mbean = addAttribute(metric);
                if (!this.mbeans.containsKey(mbean.name())) {
                    this.mbeans.put(mbean.name(), mbean);
                }
            }
            for (KafkaMbean mbean : this.mbeans.values()) {
                reregister(mbean);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            final KafkaMbean mbean = addAttribute(metric);
            if (!this.mbeans.containsKey(mbean.name())) {
                this.mbeans.put(mbean.name(), mbean);
            }
            reregister(mbean);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            final KafkaMbean mbean = removeAttribute(metric);
            if (mbean.isEmpty()) {
                unregister(mbean);
                this.mbeans.remove(mbean.name());
            } else {
                reregister(mbean);
            }
        }
    }

    private KafkaMbean removeAttribute(KafkaMetric metric) {
        final MetricName metricName = metric.metricName();
        final KafkaMbean mbean = getMBean(metricName);
        if (!mbean.isEmpty()) {
            mbean.removeAttribute(metricName.name());
        }
        return mbean;
    }

    private KafkaMbean addAttribute(KafkaMetric metric) {
        final MetricName metricName = metric.metricName();
        final KafkaMbean mbean = getMBean(metricName);
        mbean.setAttribute(metricName.name(), metric);
        return mbean;
    }

    /**
     * Get {@link KafkaMbean} instance if metric with the given {@link MetricName} is already registered or create a new instance otherwise.
     *
     * @param metricName identifier for {@link KafkaMetric}
     * @return standard JMX MBean for the given {@link MetricName}
     */
    protected KafkaMbean getMBean(MetricName metricName) {
        final ObjectName name = KafkaMbean.getObjectName(metricName, this.prefix);
        KafkaMbean mbean = this.mbeans.get(name);
        if (mbean == null) {
            return KafkaMbean.of(name);
        }
        return mbean;
    }

    public void close() {
        synchronized (LOCK) {
            for (KafkaMbean mbean : this.mbeans.values()) {
                unregister(mbean);
            }
            this.mbeans.clear();
        }
    }

    private void unregister(KafkaMbean mbean) {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            if (server.isRegistered(mbean.name())) {
                server.unregisterMBean(mbean.name());
            }
        } catch (JMException e) {
            throw new KafkaException("Error de-registering mbean " + mbean.name(), e);
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

    public int mbeansQty() {
        return this.mbeans.size();
    }

    protected static class KafkaMbean implements DynamicMBean {

        private final ObjectName objectName;
        private final Map<String, KafkaMetric> metrics;

        public static KafkaMbean of(ObjectName objectName) {
            try {
                return new KafkaMbean(objectName);
            } catch (MalformedObjectNameException e) {
                throw new KafkaException("Error creating mbean with " + objectName, e);
            }
        }

        private KafkaMbean(ObjectName objectName) throws MalformedObjectNameException {
            this.metrics = new HashMap<>();
            this.objectName = objectName;
        }

        public ObjectName name() {
            return this.objectName;
        }

        public void setAttribute(String name, KafkaMetric metric) {
            this.metrics.put(name, metric);
        }

        @Override
        public Object getAttribute(String name) throws AttributeNotFoundException, MBeanException, ReflectionException {
            if (this.metrics.containsKey(name)) {
                return this.metrics.get(name).value();
            } else {
                throw new AttributeNotFoundException("Could not find attribute " + name);
            }
        }

        @Override
        public AttributeList getAttributes(String[] names) {
            try {
                AttributeList list = new AttributeList();
                for (String name : names) {
                    list.add(new Attribute(name, getAttribute(name)));
                }
                return list;
            } catch (Exception e) {
                log.error("Error getting JMX attribute: ", e);
                return new AttributeList();
            }
        }

        public KafkaMetric removeAttribute(String name) {
            return this.metrics.remove(name);
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[this.metrics.size()];
            int i = 0;
            for (Map.Entry<String, KafkaMetric> entry : this.metrics.entrySet()) {
                attrs[i++] = new MBeanAttributeInfo(entry.getKey(), double.class.getName(), entry.getValue().metricName().description(), true, false, false);
            }
            return new MBeanInfo(this.getClass().getName(), "", attrs, null, null, null);
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

        public boolean isEmpty() {
            return this.metrics.isEmpty();
        }

        public int attributesQty() {
            return this.metrics.size();
        }

        /**
         * Calculate name of {@link KafkaMbean} instance, using the given {@link MetricName}.
         *
         * @param metricName identifier for {@link KafkaMetric}
         * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
         */
        protected static ObjectName getObjectName(MetricName metricName, String prefix) {
            try {
                Objects.requireNonNull(metricName, "Parameter 'metricName' is mandatory");
                StringBuilder mBeanName = new StringBuilder();
                mBeanName.append(prefix);
                mBeanName.append(":type=");
                mBeanName.append(metricName.group());
                for (Map.Entry<String, String> entry : metricName.tags().entrySet()) {
                    if (entry.getKey().isEmpty() || entry.getValue().isEmpty()) {
                        continue;
                    }
                    mBeanName.append(",");
                    mBeanName.append(entry.getKey());
                    mBeanName.append("=");
                    mBeanName.append(entry.getValue());
                }
                return ObjectName.getInstance(mBeanName.toString());
            } catch (MalformedObjectNameException e) {
                throw new KafkaException("Error creating mbean for " + metricName, e);
            }
        }
    }
}
