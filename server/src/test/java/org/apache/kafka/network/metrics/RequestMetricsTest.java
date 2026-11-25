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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RequestMetricsTest {

    @Test
    public void testDeprecatedRequestRateMetersAreLimited() {
        RequestMetrics metrics = new RequestMetrics("Produce");
        try {
            for (int i = 0; i < RequestMetrics.MAX_DEPRECATED_REQUEST_RATE_METERS; i++) {
                metrics.deprecatedRequestRate((short) 0, new ClientInformation("client" + i, "1.0"));
            }
            Meter overflow = metrics.deprecatedRequestRate((short) 0, new ClientInformation("other1", "1.0"));
            assertSame(overflow, metrics.deprecatedRequestRate((short) 0, new ClientInformation("other2", "1.0")));
            assertNotSame(overflow, metrics.deprecatedRequestRate((short) 0, new ClientInformation("client0", "1.0")));
            assertEquals(RequestMetrics.MAX_DEPRECATED_REQUEST_RATE_METERS + 1, deprecatedRequestRateMeterCount());
        } finally {
            metrics.removeMetrics();
        }
        assertEquals(0, deprecatedRequestRateMeterCount());
    }

    @Test
    public void testDeprecatedRequestRateTagsAreTruncated() {
        RequestMetrics metrics = new RequestMetrics("Produce");
        try {
            String name = "a".repeat(RequestMetrics.MAX_CLIENT_INFORMATION_TAG_LENGTH);
            Meter meter = metrics.deprecatedRequestRate((short) 0, new ClientInformation(name + "b", "1.0"));
            assertSame(meter, metrics.deprecatedRequestRate((short) 0, new ClientInformation(name + "c", "1.0")));
            assertEquals(1, deprecatedRequestRateMeterCount());
        } finally {
            metrics.removeMetrics();
        }
        assertEquals(0, deprecatedRequestRateMeterCount());
    }

    private static long deprecatedRequestRateMeterCount() {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(name -> name.getName().equals(RequestMetrics.DEPRECATED_REQUESTS_PER_SEC))
            .count();
    }
}
