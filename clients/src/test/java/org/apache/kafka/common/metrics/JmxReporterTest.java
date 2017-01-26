/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.metrics.stats.Total;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        final JmxReporter reporter = new JmxReporter();
        MetricName name;
        try (Metrics metrics = new Metrics()) {
            assertEquals(0, reporter.mbeansQty());
            metrics.addReporter(reporter);
            assertEquals(1, reporter.mbeansQty());
            Sensor sensor = metrics.sensor("kafka.requests");
            name = metrics.metricName("pack.bean1.avg", "grp1");
            sensor.add(name, new Avg());
            assertEquals(2, reporter.mbeansQty());
            assertEquals(1, reporter.getMBean(name).attributesQty());
            name = metrics.metricName("pack.bean2.total", "grp2");
            sensor.add(name, new Total());
            assertEquals(3, reporter.mbeansQty());
            assertEquals(1, reporter.getMBean(name).attributesQty());
            Sensor sensor2 = metrics.sensor("kafka.blah");
            name = metrics.metricName("pack.bean1.some", "grp1");
            sensor2.add(name, new Total());
            assertEquals(3, reporter.mbeansQty());
            assertEquals(2, reporter.getMBean(name).attributesQty());
            name = metrics.metricName("pack.bean2.some", "grp1");
            sensor2.add(name, new Total());
            assertEquals(3, reporter.mbeansQty());
            assertEquals(3, reporter.getMBean(name).attributesQty());
            metrics.removeSensor(sensor2.name());
            assertEquals(3, reporter.mbeansQty());
            assertEquals(1, reporter.getMBean(name).attributesQty());
        }
        assertEquals(0, reporter.mbeansQty());
        assertEquals(0, reporter.getMBean(name).attributesQty());
    }

    @Test
    public void testGetMBeanWithNull() {
        final JmxReporter reporter = new JmxReporter();
        try {
            reporter.getMBean(null);
        } catch (RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(NullPointerException.class));
            assertThat(e.getMessage(), CoreMatchers.is("Parameter 'metricName' is mandatory"));
        }
    }
}
