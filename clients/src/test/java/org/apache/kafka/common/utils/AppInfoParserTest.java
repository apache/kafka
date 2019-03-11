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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.metrics.Metrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class AppInfoParserTest {
    private static final String expectedCommitVersion = AppInfoParser.DEFAULT_VALUE;
    private static final String expectedVersion = AppInfoParser.DEFAULT_VALUE;
    private static final Long expectedStartMs = 1552313875722L;
    private static final String metricsPrefix = "app-info-test";
    private static final String metricsId = "test";

    private Metrics metrics;
    private MBeanServer mBeanServer;

    @Before
    public void setUp() {
        metrics = new Metrics(new MockTime(1));
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @After
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testRegisterAppInfoRegistersMetrics() throws JMException {
        registerAppInfo();
    }

    @Test
    public void testUnregisterAppInfoUnregistersMetrics() throws JMException {
        registerAppInfo();
        AppInfoParser.unregisterAppInfo(metricsPrefix, metricsId, metrics);

        assertFalse(mBeanServer.isRegistered(expectedAppObjectName()));
        assertNull(metrics.metric(metrics.metricName("commit-id", "app-info")));
        assertNull(metrics.metric(metrics.metricName("version", "app-info")));
        assertNull(metrics.metric(metrics.metricName("start-time-ms", "app-info")));
    }

    private void registerAppInfo() throws JMException {
        assertEquals(expectedCommitVersion, AppInfoParser.getCommitId());
        assertEquals(expectedVersion, AppInfoParser.getVersion());

        AppInfoParser.registerAppInfo(metricsPrefix, metricsId, metrics, expectedStartMs);

        assertTrue(mBeanServer.isRegistered(expectedAppObjectName()));
        assertEquals(expectedCommitVersion, metrics.metric(metrics.metricName("commit-id", "app-info")).metricValue());
        assertEquals(expectedVersion, metrics.metric(metrics.metricName("version", "app-info")).metricValue());
        assertEquals(expectedStartMs, metrics.metric(metrics.metricName("start-time-ms", "app-info")).metricValue());
    }

    private ObjectName expectedAppObjectName() throws MalformedObjectNameException {
        return new ObjectName(metricsPrefix + ":type=app-info,id=" + metricsId);
    }
}
