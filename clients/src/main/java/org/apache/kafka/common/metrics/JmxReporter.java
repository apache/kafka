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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names
 */
public class JmxReporter implements MetricsReporter {

    public static final String METRICS_CONFIG_PREFIX = "metrics.jmx.";

    public static final String EXCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "exclude";
    public static final String EXCLUDE_CONFIG_ALIAS = METRICS_CONFIG_PREFIX + "blacklist";

    public static final String INCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "include";
    public static final String INCLUDE_CONFIG_ALIAS = METRICS_CONFIG_PREFIX + "whitelist";


    public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(INCLUDE_CONFIG,
                                                                         INCLUDE_CONFIG_ALIAS,
                                                                         EXCLUDE_CONFIG,
                                                                         EXCLUDE_CONFIG_ALIAS);

    public static final String DEFAULT_INCLUDE = ".*";
    public static final String DEFAULT_EXCLUDE = "";

    private static final Logger log = LoggerFactory.getLogger(JmxReporter.class);
    private static final Object LOCK = new Object();
    private String prefix;
    private final Map<String, KafkaMbean> mbeans = new HashMap<>();
    private Predicate<String> mbeanPredicate = s -> true;

    public JmxReporter() {
        this("");
    }

    /**
     * Create a JMX reporter that prefixes all metrics with the given string.
     *  @deprecated Since 2.6.0. Use {@link JmxReporter#JmxReporter()}
     *  Initialize JmxReporter with {@link JmxReporter#contextChange(MetricsContext)}
     *  Populate prefix by adding _namespace/prefix key value pair to {@link MetricsContext}
     */
    @Deprecated
    public JmxReporter(String prefix) {
        this.prefix = prefix != null ? prefix : "";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        reconfigure(configs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        compilePredicate(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        synchronized (LOCK) {
            this.mbeanPredicate = JmxReporter.compilePredicate(configs);

            mbeans.forEach((name, mbean) -> {
                if (mbeanPredicate.test(name)) {
                    reregister(mbean);
                } else {
                    unregister(mbean);
                }
            });
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        synchronized (LOCK) {
            for (KafkaMetric metric : metrics)
                addAttribute(metric);

            mbeans.forEach((name, mbean) -> {
                if (mbeanPredicate.test(name)) {
                    reregister(mbean);
                }
            });
        }
    }

    public boolean containsMbean(String mbeanName) {
        return mbeans.containsKey(mbeanName);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            String mbeanName = addAttribute(metric);
            if (mbeanName != null && mbeanPredicate.test(mbeanName)) {
                reregister(mbeans.get(mbeanName));
            }
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
                } else if (mbeanPredicate.test(mBeanName))
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

    private String addAttribute(KafkaMetric metric) {
        try {
            MetricName metricName = metric.metricName();
            String mBeanName = getMBeanName(prefix, metricName);
            if (!this.mbeans.containsKey(mBeanName))
                mbeans.put(mBeanName, new KafkaMbean(mBeanName));
            KafkaMbean mbean = this.mbeans.get(mBeanName);
            mbean.setAttribute(metricName.name(), metric);
            return mBeanName;
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
            if (entry.getKey().isEmpty() || entry.getValue().isEmpty())
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

        KafkaMbean(String mbeanName) throws MalformedObjectNameException {
            this.metrics = new HashMap<>();
            this.objectName = new ObjectName(mbeanName);
        }

        public ObjectName name() {
            return objectName;
        }

        void setAttribute(String name, KafkaMetric metric) {
            this.metrics.put(name, metric);
        }

        @Override
        public Object getAttribute(String name) throws AttributeNotFoundException {
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

        KafkaMetric removeAttribute(String name) {
            return this.metrics.remove(name);
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[metrics.size()];
            int i = 0;
            for (Map.Entry<String, KafkaMetric> entry : this.metrics.entrySet()) {
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
            return new MBeanInfo(this.getClass().getName(), "", attrs, null, null, null);
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

    }

    public static Predicate<String> compilePredicate(Map<String, ?> originalConfig) {
        Map<String, ?> configs = ConfigUtils.translateDeprecatedConfigs(
            originalConfig, new String[][]{{INCLUDE_CONFIG, INCLUDE_CONFIG_ALIAS},
                                           {EXCLUDE_CONFIG, EXCLUDE_CONFIG_ALIAS}});
        String include = (String) configs.get(INCLUDE_CONFIG);
        String exclude = (String) configs.get(EXCLUDE_CONFIG);

        if (include == null) {
            include = DEFAULT_INCLUDE;
        }

        if (exclude == null) {
            exclude = DEFAULT_EXCLUDE;
        }

        try {
            Pattern includePattern = Pattern.compile(include);
            Pattern excludePattern = Pattern.compile(exclude);

            return s -> includePattern.matcher(s).matches()
                        && !excludePattern.matcher(s).matches();
        } catch (PatternSyntaxException e) {
            throw new ConfigException("JMX filter for configuration" + METRICS_CONFIG_PREFIX
                                      + ".(include/exclude) is not a valid regular expression");
        }
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        String namespace = metricsContext.contextLabels().get(MetricsContext.NAMESPACE);
        Objects.requireNonNull(namespace);
        synchronized (LOCK) {
            if (!mbeans.isEmpty()) {
                throw new IllegalStateException("JMX MetricsContext can only be updated before JMX metrics are created");
            }

            // prevent prefix from getting reset back to empty for backwards compatibility
            // with the deprecated JmxReporter(String prefix) constructor, in case contextChange gets called
            // via one of the Metrics() constructor with a default empty MetricsContext()
            if (namespace.isEmpty()) {
                return;
            }

            prefix = namespace;
        }
    }
}
