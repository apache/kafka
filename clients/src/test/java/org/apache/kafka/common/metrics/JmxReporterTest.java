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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JmxReporterTest {
    private static final Logger log = LoggerFactory.getLogger(JmxReporterTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(1200);

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

    private static void assertExceptionContains(Throwable e, String substring) {
        assertTrue("Expected exception message to contain substring '" + substring +
                "', but it was " + e.getMessage(),
            e.getMessage().contains(substring));
    }

    private static class MultipleReportersTestContext {
        private final JmxReporter.Registry registry;
        private final MockMBeanServer server;
        private final List<JmxReporter> reporters;

        MultipleReportersTestContext() {
            this.registry = new JmxReporter.Registry();
            this.server = new MockMBeanServer();
            this.reporters = new ArrayList<>();
        }

        MultipleReportersTestContext addReporter(String prefix) {
            this.reporters.add(new JmxReporter(prefix, registry, server));
            return this;
        }

        static KafkaMetric newMetric(String name) {
            MetricName metricName = new MetricName(name, "network", "", Collections.emptyMap());
            return new KafkaMetric(new Object(), metricName, new Gauge<Integer>() {
                @Override
                public Integer value(MetricConfig config, long now) {
                    return 1;
                }
            }, new MetricConfig(), Time.SYSTEM);
        }
    }

    /**
     * Test the case where two separate JmxReporter instances try to use the same mbean.
     */
    @Test
    public void testMultipleReportersUsingTheSameMbean() throws Exception {
        final String prefix = "kafka.mock.client";
        MultipleReportersTestContext context = new MultipleReportersTestContext().
            addReporter(prefix).addReporter(prefix);
        KafkaMetric foo = MultipleReportersTestContext.newMetric("foo");

        // The first reporter claims the bean.
        context.reporters.get(0).addMetrics(Collections.singletonList(foo));
        assertEquals(context.reporters.get(0), context.registry.
            findMBeanOwner(JmxReporter.getMBeanName(prefix, foo.metricName())));

        // The second reporter does not own the bean.
        context.reporters.get(1).addMetrics(Collections.singletonList(foo));
        assertEquals(context.reporters.get(0), context.registry.
            findMBeanOwner(JmxReporter.getMBeanName(prefix, foo.metricName())));
        context.reporters.get(1).removeMetrics(Collections.singletonList(foo));
        assertEquals(context.reporters.get(0), context.registry.
            findMBeanOwner(JmxReporter.getMBeanName(prefix, foo.metricName())));

        // Remove the bean.
        context.reporters.get(0).removeMetrics(Collections.singletonList(foo));
        assertEquals(null, context.registry.
            findMBeanOwner(JmxReporter.getMBeanName(prefix, foo.metricName())));
    }

    /**
     * Test removing MBeans from the registry.
     */
    @Test
    public void testRemovingBeans() throws Exception {
        MockMBeanServer server = new MockMBeanServer();
        MultipleReportersTestContext context = new MultipleReportersTestContext().
            addReporter("kafka.mock.client");
        KafkaMetric foo = MultipleReportersTestContext.newMetric("foo");
        KafkaMetric bar = MultipleReportersTestContext.newMetric("bar");
        context.reporters.get(0).metricChange(foo);
        server.unregisterMbeanLatch = new CountDownLatch(1);
        final AtomicBoolean addMetricThreadDone = new AtomicBoolean(false);
        // Adding a new attribute to a bean should be blocked by the removal of
        // that same bean.
        Thread checkThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long startNs = Time.SYSTEM.nanoseconds();
                do {
                    assertTrue(!addMetricThreadDone.get());
                } while (Time.SYSTEM.nanoseconds() < startNs + 10000);
                server.unregisterMbeanLatch.countDown();
            }
        });
        Thread addMetricThread = new Thread(new Runnable() {
            @Override
            public void run() {
                context.reporters.get(0).metricChange(bar);
                addMetricThreadDone.set(true);
            }
        });
        try {
            checkThread.start();
            addMetricThread.start();
            context.reporters.get(0).metricRemoval(foo);
        } finally {
            checkThread.join();
            addMetricThread.join();
        }
    }
}
