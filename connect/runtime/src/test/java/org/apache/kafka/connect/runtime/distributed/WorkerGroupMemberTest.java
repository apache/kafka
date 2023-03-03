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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.junit.Test;
import org.mockito.Mock;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class WorkerGroupMemberTest {
    @Mock
    private ConfigBackingStore configBackingStore;

    @Test
    public void testMetrics() throws Exception {
        WorkerGroupMember member;
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("group.id", "group-1");
        workerProps.put("offset.storage.topic", "topic-1");
        workerProps.put("config.storage.topic", "topic-1");
        workerProps.put("status.storage.topic", "topic-1");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockConnectMetrics.MockMetricsReporter.class.getName());
        DistributedConfig config = spy(new DistributedConfig(workerProps));
        doReturn("cluster-1").when(config).kafkaClusterId();

        LogContext logContext = new LogContext("[Worker clientId=client-1 + groupId= group-1]");
        member = new WorkerGroupMember(config, "", configBackingStore, null, Time.SYSTEM, "client-1", logContext);

        verify(config, atLeastOnce()).kafkaClusterId();
        boolean foundMockReporter = false;
        boolean foundJmxReporter = false;
        assertEquals(2, member.metrics().reporters().size());
        for (MetricsReporter reporter : member.metrics().reporters()) {
            if (reporter instanceof MockConnectMetrics.MockMetricsReporter) {
                foundMockReporter = true;
                MockConnectMetrics.MockMetricsReporter mockMetricsReporter = (MockConnectMetrics.MockMetricsReporter) reporter;
                assertEquals("cluster-1", mockMetricsReporter.getMetricsContext().contextLabels().get(WorkerConfig.CONNECT_KAFKA_CLUSTER_ID));
                assertEquals("group-1", mockMetricsReporter.getMetricsContext().contextLabels().get(WorkerConfig.CONNECT_GROUP_ID));
            }
            if (reporter instanceof JmxReporter) {
                foundJmxReporter = true;
            }
        }
        assertTrue("Failed to find MockMetricsReporter", foundMockReporter);
        assertTrue("Failed to find JmxReporter", foundJmxReporter);

        MetricName name = member.metrics().metricName("test.avg", "grp1");
        member.metrics().addMetric(name, new Avg());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        //verify metric exists with correct prefix
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.connect:type=grp1,client-id=client-1")));
    }

    @Test
    public void testDisableJmxReporter() {
        WorkerGroupMember member;
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("group.id", "group-1");
        workerProps.put("offset.storage.topic", "topic-1");
        workerProps.put("config.storage.topic", "topic-1");
        workerProps.put("status.storage.topic", "topic-1");
        workerProps.put("auto.include.jmx.reporter", "false");
        DistributedConfig config = spy(new DistributedConfig(workerProps));
        doReturn("cluster-1").when(config).kafkaClusterId();

        LogContext logContext = new LogContext("[Worker clientId=client-1 + groupId= group-1]");
        member = new WorkerGroupMember(config, "", configBackingStore, null, Time.SYSTEM, "client-1", logContext);

        verify(config, atLeastOnce()).kafkaClusterId();
        assertTrue(member.metrics().reporters().isEmpty());
        member.stop();
    }

}
