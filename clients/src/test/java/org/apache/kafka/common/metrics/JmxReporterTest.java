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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        Metrics metrics = new Metrics();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            JmxReporter reporter = new JmxReporter();
            metrics.addReporter(reporter);

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));

            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), new Avg());
            sensor.add(metrics.metricName("pack.bean2.total", "grp2"), new CumulativeSum());

            assertTrue(server.isRegistered(new ObjectName(":type=grp1")));
            assertEquals(Double.NaN, server.getAttribute(new ObjectName(":type=grp1"), "pack.bean1.avg"));
            assertTrue(server.isRegistered(new ObjectName(":type=grp2")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=grp2"), "pack.bean2.total"));

            MetricName metricName = metrics.metricName("pack.bean1.avg", "grp1");
            String mBeanName = JmxReporter.getMBeanName("", metricName);
            assertTrue(reporter.containsMbean(mBeanName));
            metrics.removeMetric(metricName);
            assertFalse(reporter.containsMbean(mBeanName));

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));
            assertTrue(server.isRegistered(new ObjectName(":type=grp2")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=grp2"), "pack.bean2.total"));

            metricName = metrics.metricName("pack.bean2.total", "grp2");
            metrics.removeMetric(metricName);
            assertFalse(reporter.containsMbean(mBeanName));

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));
            assertFalse(server.isRegistered(new ObjectName(":type=grp2")));
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testJmxRegistrationSanitization() throws Exception {
        Metrics metrics = new Metrics();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            metrics.addReporter(new JmxReporter());

            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo*"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo+"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo?"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo:"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo%"), new CumulativeSum());

            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo\\*\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo\\*\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo+\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo+\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo\\?\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo\\?\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo:\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo:\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=foo%")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=foo%"), "name"));

            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo*"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo+"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo?"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo:"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo%"));

            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo\\*\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=foo+")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo\\?\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo:\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=foo%")));
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testPredicateAndDynamicReload() throws Exception {
        Metrics metrics = new Metrics();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        Map<String, String> configs = new HashMap<>();

        configs.put(JmxReporter.EXCLUDE_CONFIG,
                    JmxReporter.getMBeanName("", metrics.metricName("pack.bean2.total", "grp2")));

        try {
            JmxReporter reporter = new JmxReporter();
            reporter.configure(configs);
            metrics.addReporter(reporter);

            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("pack.bean2.avg", "grp1"), new Avg());
            sensor.add(metrics.metricName("pack.bean2.total", "grp2"), new CumulativeSum());
            sensor.record();

            assertTrue(server.isRegistered(new ObjectName(":type=grp1")));
            assertEquals(1.0, server.getAttribute(new ObjectName(":type=grp1"), "pack.bean2.avg"));
            assertFalse(server.isRegistered(new ObjectName(":type=grp2")));

            sensor.record();

            configs.put(JmxReporter.EXCLUDE_CONFIG,
                        JmxReporter.getMBeanName("", metrics.metricName("pack.bean2.avg", "grp1")));

            reporter.reconfigure(configs);

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));
            assertTrue(server.isRegistered(new ObjectName(":type=grp2")));
            assertEquals(2.0, server.getAttribute(new ObjectName(":type=grp2"), "pack.bean2.total"));

            metrics.removeMetric(metrics.metricName("pack.bean2.total", "grp2"));
            assertFalse(server.isRegistered(new ObjectName(":type=grp2")));
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testJmxPrefix() throws Exception {
        JmxReporter reporter = new JmxReporter();
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.server");
        MetricConfig metricConfig = new MetricConfig();
        Metrics metrics = new Metrics(metricConfig, new ArrayList<>(Collections.singletonList(reporter)), Time.SYSTEM, metricsContext);

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), new Avg());
            assertEquals("kafka.server", server.getObjectInstance(new ObjectName("kafka.server:type=grp1")).getObjectName().getDomain());
        } finally {
            metrics.close();
        }
    }
}
