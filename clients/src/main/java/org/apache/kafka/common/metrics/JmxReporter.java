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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names.
 *
 * There may be multiple JmxReporter objects in the same Java process.  For example,
 * if you have both a Consumer and a Producer in the same Java process, they will
 * each have their own distinct JmxReporters.  We expect that each JmxReporter will
 * manage a distinct, non-overlapping set of Java Mbeans.  To enforce that invariant,
 * there is a global Registry object which keeps track of which reporters
 * own which beans and does not allow multiple JmxReporters to claim the same bean.
 *
 * The global registry has its own lock.  Since this lock could cause a lot of contention,
 * we try to minimize the length of time it is held.  In particular, we do not perform JMX
 * operations while holding the global registry lock.  Instead, each MBean object has its
 * own lock, which we hold when performing operations on that specific MBean.
 * The main complexity here is handling deletion via the deleted flag.
 */
public class JmxReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(JmxReporter.class);
    private final String prefix;
    private final Registry registry;
    private final MBeanServer mbeanServer;

    /**
     * Create a JMX reporter.
     */
    public JmxReporter() {
        this("");
    }

    /**
     * Create a JMX reporter that prefixes all metrics with the given string.
     *
     * @param prefix        The string to prefix all metrics with.
     */
    public JmxReporter(String prefix) {
        this(prefix, Registry.INSTANCE, ManagementFactory.getPlatformMBeanServer());
    }

    /**
     * Create a JMX reporter that prefixes all metrics with the given string
     * and uses the given MBeanServer.
     *
     * @param prefix        The string to prefix all metrics with.
     * @param registry      The registry that stores beans.
     * @param mbeanServer   The MBean server to use for JMX operations.
     */
    public JmxReporter(String prefix, Registry registry, MBeanServer mbeanServer) {
        this.prefix = prefix;
        this.registry = registry;
        this.mbeanServer = mbeanServer;
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void init(List<KafkaMetric> metrics) {
        addMetrics(metrics);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        addMetrics(Collections.singleton(metric));
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        removeMetrics(Collections.singleton(metric));
    }

    // Visible for testing
    public boolean containsMbean(String mbeanName) {
        return registry.findMBeanOwner(mbeanName) == this;
    }

    void addMetrics(Collection<KafkaMetric> metrics) {
        Map<String, List<KafkaMetric>> beanNameToMetrics = groupMetrics(metrics);
        for (Map.Entry<String, List<KafkaMetric>> entry : beanNameToMetrics.entrySet()) {
            KafkaMbean mbean = registry.getOrCreateLockedMBean(entry.getKey(), this);
            if (mbean == null) {
                log.warn("Bean name conflict: {} is already registered to a different " +
                    "JmxReporter", entry.getKey());
            } else {
                try {
                    mbean.addMetrics(entry.getValue());
                } finally {
                    mbean.lock.unlock();
                }
            }
        }
    }

    void removeMetrics(Collection<KafkaMetric> metrics) {
        Map<String, List<KafkaMetric>> beanNameToMetrics = groupMetrics(metrics);
        for (Map.Entry<String, List<KafkaMetric>> entry : beanNameToMetrics.entrySet()) {
            String mbeanName = entry.getKey();
            KafkaMbean mbean = registry.getOrCreateLockedMBean(mbeanName, this);
            if (mbean != null) {
                boolean deleting = false;
                try {
                    mbean.removeMetrics(entry.getValue());
                    if (mbean.beanMetrics.isEmpty()) {
                        // We can't delete the mbean from the registry here, since we
                        // don't hold the registry lock here.  However, we want to make sure
                        // that nobody else tries to use this mbean object, since it's
                        // about to be removed from the registry.  Therefore, we set the
                        // deleting flag.
                        mbean.deleting = true;
                        deleting = true;
                    }
                } finally {
                    mbean.lock.unlock();
                }
                if (deleting) {
                    // If the mbean was empty, we finish deleting it here.  Registry#removeBean
                    // will take the registry lock and remove the mbean object.
                    registry.removeMbean(mbeanName, mbean);
                }
            }
        }
    }

    /**
     * Group metrics which are on the same mbean together.
     *
     * @param metrics   A collection of KafkaMetric objects.
     * @return          A map from bean names to lists of KafkaMetric objects.
     */
    private Map<String, List<KafkaMetric>> groupMetrics(Collection<KafkaMetric> metrics) {
        Map<String, List<KafkaMetric>> result = new HashMap<>();
        for (KafkaMetric metric : metrics) {
            String name = getMBeanName(prefix, metric.metricName());
            result.computeIfAbsent(name, __ -> new ArrayList<>()).add(metric);
        }
        return result;
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

    @Override
    public void close() {
        List<KafkaMbean> mbeans = registry.removeMBeans(this);

        for (KafkaMbean mbean : mbeans) {
            mbean.lock.lock();
            try {
                mbean.removeAllMetrics();
            } finally {
                mbean.lock.unlock();
            }
        }
    }

    static class Registry {
        final static Registry INSTANCE = new Registry();
        private final Map<String, KafkaMbean> mbeans = new HashMap<>();

        synchronized JmxReporter findMBeanOwner(String mbeanName) {
            KafkaMbean mbean = mbeans.get(mbeanName);
            return mbean == null ? null : mbean.owner;
        }

        synchronized void removeMbean(String mbeanName, KafkaMbean mbean) {
            mbeans.remove(mbeanName, mbean);
        }

        /**
         * Remove all the mbeans that belong to a given JmxReporter.
         *
         * @param reporter      The JmxReporter.
         * @return              The mbeans that were removed.
         */
        List<KafkaMbean> removeMBeans(JmxReporter reporter) {
            List<KafkaMbean> results = new ArrayList<>();
            synchronized (this) {
                for (KafkaMbean mbean : mbeans.values()) {
                    if (mbean.owner == reporter) {
                        results.add(mbean);
                    }
                }
            }
            // Just like in JmxReporter#removeMetrics, we need to set the
            // deleting flag before actually removing a bean from the
            // registry.  Otherwise, we could run into a scenario where
            // somebody makes modifications to an mbean object that is
            // already dead (i.e., a distinct newer object exists for the
            // same mbean name).
            for (KafkaMbean mbean : results) {
                mbean.lock.lock();
                try {
                    mbean.deleting = true;
                } finally {
                    mbean.lock.unlock();
                }
            }
            synchronized (this) {
                for (KafkaMbean mbean : results) {
                    mbeans.remove(mbean.mbeanName, mbean);
                }
            }
            return results;
        }

        /**
         * Given an mbean name, get the existing KafkaMbean object, or create a new
         * one if needed.
         *
         * @param mbeanName The mbean name.
         * @param reporter  The JMXReporter trying to get this mbean.
         * @return          null if the mbean exists and belongs to a different JmxReporter;
         *                  the bean object otherwise.  The bean object will be locked.
         */
        KafkaMbean getOrCreateLockedMBean(String mbeanName, JmxReporter reporter) {
            while (true) {
                KafkaMbean mbean;
                synchronized (this) {
                    mbean = mbeans.get(mbeanName);
                    if (mbean != null) {
                        if (mbean.owner != reporter) {
                            return null;
                        }
                    } else {
                        try {
                            mbean = new KafkaMbean(reporter, mbeanName);
                        } catch (MalformedObjectNameException e) {
                            throw new RuntimeException(e);
                        }
                        mbeans.put(mbeanName, mbean);
                    }
                }
                mbean.lock.lock();
                if (!mbean.deleting) {
                    return mbean;
                }
                mbean.lock.unlock();
            }
        }
    }

    private static class KafkaMbean implements DynamicMBean {
        private final JmxReporter owner;
        private final String mbeanName;
        private final ObjectName objectName;
        private final Map<String, KafkaMetric> beanMetrics = new ConcurrentHashMap<>();
        private final ReentrantLock lock = new ReentrantLock();
        private boolean deleting = false;

        KafkaMbean(JmxReporter owner, String mbeanName) throws MalformedObjectNameException {
            this.owner = owner;
            this.mbeanName = mbeanName;
            this.objectName = new ObjectName(mbeanName);
        }

        public ObjectName name() {
            return objectName;
        }

        @Override
        public Object getAttribute(String name) throws AttributeNotFoundException {
            KafkaMetric metric = beanMetrics.get(name);
            if (metric == null) {
                throw new AttributeNotFoundException("Could not find attribute " + name);
            }
            return metric.metricValue();
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

        @Override
        public MBeanInfo getMBeanInfo() {
            // Note: the size could change in the ConcurrentMap between calling size
            // here and iterating.  That's OK, though, since the size is just used to
            // optimize the list capacity allocation anyway.
            List<MBeanAttributeInfo> attrList = new ArrayList<>(beanMetrics.size());
            for (Map.Entry<String, KafkaMetric> entry : this.beanMetrics.entrySet()) {
                String attribute = entry.getKey();
                KafkaMetric metric = entry.getValue();
                attrList.add(new MBeanAttributeInfo(attribute,
                                                    double.class.getName(),
                                                    metric.metricName().description(),
                                                    true,
                                                    false,
                                                    false));
            }
            return new MBeanInfo(this.getClass().getName(), "",
                attrList.toArray(new MBeanAttributeInfo[0]), null, null, null);
        }

        @Override
        public Object invoke(String name, Object[] params, String[] sig) {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        @Override
        public void setAttribute(Attribute attribute) {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        @Override
        public AttributeList setAttributes(AttributeList list) {
            throw new UnsupportedOperationException("Set not allowed.");
        }

        /**
         * Add some new metrics to this mbean, then unregister and re-register the mbean.
         */
        void addMetrics(Collection<KafkaMetric> metrics) {
            checkLocked();
            if (metrics.isEmpty()) {
                return;
            }
            if (!beanMetrics.isEmpty()) {
                unregister();
            }
            for (KafkaMetric metric : metrics) {
                beanMetrics.put(metric.metricName().name(), metric);
            }
            register();
        }

        void removeMetrics(Collection<KafkaMetric> metrics) {
            checkLocked();
            if (metrics.isEmpty()) {
                return;
            }
            if (!beanMetrics.isEmpty()) {
                unregister();
            }
            for (KafkaMetric metric : metrics) {
                beanMetrics.remove(metric.metricName().name(), metric);
            }
            if (!beanMetrics.isEmpty()) {
                register();
            }
        }

        void register() {
            checkLocked();
            try {
                owner.mbeanServer.registerMBean(this, objectName);
            } catch (Exception e) {
                throw new KafkaException("Failed to register bean " + mbeanName, e);
            }
        }

        void unregister() {
            checkLocked();
            try {
                owner.mbeanServer.unregisterMBean(objectName);
            } catch (Exception e) {
                throw new KafkaException("Failed to unreregister bean " + mbeanName, e);
            }
        }

        void removeAllMetrics() {
            checkLocked();
            removeMetrics(beanMetrics.values());
        }

        private void checkLocked() {
            if (!lock.isLocked()) {
                throw new RuntimeException("The MBean lock must be held here.");
            }
        }
    }
}
