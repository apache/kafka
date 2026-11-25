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
package org.apache.kafka.network.metrics;

import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestMetricsTest {

    private static final String REQUEST_NAME = "RequestMetricsTest";

    @Test
    public void testDeprecatedRequestRateMetersAreLimited() {
        RequestMetrics metrics = new RequestMetrics(REQUEST_NAME);
        try {
            for (int i = 0; i < RequestMetrics.MAX_DEPRECATED_REQUEST_RATE_METERS; i++) {
                metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation("client" + i, "1.0"));
            }
            Meter overflow = metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation("other1", "1.0"));
            assertSame(overflow, metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation("other2", "1.0")));
            assertNotSame(overflow, metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation("client0", "1.0")));
            Set<String> names = deprecatedRequestRateMBeanNames();
            assertEquals(RequestMetrics.MAX_DEPRECATED_REQUEST_RATE_METERS + 1, names.size());
            assertTrue(names.contains(mBeanName("client0", "1.0")));
            assertTrue(names.contains(mBeanName("_overflow_", "_overflow_")));
        } finally {
            metrics.removeMetrics();
        }
        assertEquals(Set.of(), deprecatedRequestRateMBeanNames());
    }

    @Test
    public void testDeprecatedRequestRateTagsAreTruncated() {
        RequestMetrics metrics = new RequestMetrics(REQUEST_NAME);
        try {
            String name = "a".repeat(RequestMetrics.MAX_CLIENT_INFORMATION_TAG_LENGTH);
            String version = "1".repeat(RequestMetrics.MAX_CLIENT_INFORMATION_TAG_LENGTH);
            Meter meter = metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation(name, "1.0"));
            assertSame(meter, metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation(name + "b", "1.0")));
            metrics.deprecatedRequestRateMeter((short) 0, new ClientInformation("client", version + "1"));
            assertEquals(Set.of(mBeanName(name, "1.0"), mBeanName("client", version)), deprecatedRequestRateMBeanNames());
        } finally {
            metrics.removeMetrics();
        }
        assertEquals(Set.of(), deprecatedRequestRateMBeanNames());
    }

    private static String mBeanName(String softwareName, String softwareVersion) {
        return "kafka.network:type=RequestMetrics,name=" + RequestMetrics.DEPRECATED_REQUESTS_PER_SEC +
            ",request=" + REQUEST_NAME + ",version=0,clientSoftwareName=" + softwareName + ",clientSoftwareVersion=" + softwareVersion;
    }

    private static Set<String> deprecatedRequestRateMBeanNames() {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(name -> name.getName().equals(RequestMetrics.DEPRECATED_REQUESTS_PER_SEC))
            .map(name -> name.getMBeanName())
            .filter(name -> name.contains(",request=" + REQUEST_NAME + ","))
            .collect(Collectors.toSet());
    }
}
