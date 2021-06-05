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
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaMbeanTest {

    private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    private Sensor sensor;
    private MetricName countMetricName;
    private MetricName sumMetricName;
    private Metrics metrics;

    @BeforeEach
    public void setup() throws Exception {
        metrics = new Metrics();
        metrics.addReporter(new JmxReporter());
        sensor = metrics.sensor("kafka.requests");
        countMetricName = metrics.metricName("pack.bean1.count", "grp1");
        sensor.add(countMetricName, new WindowedCount());
        sumMetricName = metrics.metricName("pack.bean1.sum", "grp1");
        sensor.add(sumMetricName, new WindowedSum());
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testGetAttribute() throws Exception {
        sensor.record(2.5);
        Object counterAttribute = getAttribute(countMetricName);
        assertEquals(1.0, counterAttribute);
        Object sumAttribute = getAttribute(sumMetricName);
        assertEquals(2.5, sumAttribute);
    }

    @Test
    public void testGetAttributeUnknown() throws Exception {
        sensor.record(2.5);
        try {
            getAttribute(sumMetricName, "name");
            fail("Should have gotten attribute not found");
        } catch (AttributeNotFoundException e) {
            // Expected
        }
    }

    @Test
    public void testGetAttributes() throws Exception {
        sensor.record(3.5);
        sensor.record(4.0);
        AttributeList attributeList = getAttributes(countMetricName, countMetricName.name(), sumMetricName.name());
        List<Attribute> attributes = attributeList.asList();
        assertEquals(2, attributes.size());
        for (Attribute attribute : attributes) {
            if (countMetricName.name().equals(attribute.getName()))
                assertEquals(2.0, attribute.getValue());
            else if (sumMetricName.name().equals(attribute.getName()))
                assertEquals(7.5, attribute.getValue());
            else
                fail("Unexpected attribute returned: " + attribute.getName());
        }
    }

    @Test
    public void testGetAttributesWithUnknown() throws Exception {
        sensor.record(3.5);
        sensor.record(4.0);
        AttributeList attributeList = getAttributes(countMetricName, countMetricName.name(),
                sumMetricName.name(), "name");
        List<Attribute> attributes = attributeList.asList();
        assertEquals(2, attributes.size());
        for (Attribute attribute : attributes) {
            if (countMetricName.name().equals(attribute.getName()))
                assertEquals(2.0, attribute.getValue());
            else if (sumMetricName.name().equals(attribute.getName()))
                assertEquals(7.5, attribute.getValue());
            else
                fail("Unexpected attribute returned: " + attribute.getName());
        }
    }

    @Test
    public void testInvoke() throws Exception {
        RuntimeMBeanException e = assertThrows(RuntimeMBeanException.class,
            () -> mBeanServer.invoke(objectName(countMetricName), "something", null, null));
        assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
    }

    @Test
    public void testSetAttribute() throws Exception {
        RuntimeMBeanException e = assertThrows(RuntimeMBeanException.class,
            () -> mBeanServer.setAttribute(objectName(countMetricName), new Attribute("anything", 1)));
        assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
    }

    @Test
    public void testSetAttributes() throws Exception {
        RuntimeMBeanException e = assertThrows(RuntimeMBeanException.class,
            () -> mBeanServer.setAttributes(objectName(countMetricName), new AttributeList(1)));
        assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
    }

    private ObjectName objectName(MetricName metricName) throws Exception {
        return new ObjectName(JmxReporter.getMBeanName("", metricName));
    }

    private Object getAttribute(MetricName metricName, String attribute) throws Exception {
        return mBeanServer.getAttribute(objectName(metricName), attribute);
    }

    private Object getAttribute(MetricName metricName) throws Exception {
        return getAttribute(metricName, metricName.name());
    }

    private AttributeList getAttributes(MetricName metricName, String... attributes) throws Exception {
        return mBeanServer.getAttributes(objectName(metricName), attributes);
    }

}
