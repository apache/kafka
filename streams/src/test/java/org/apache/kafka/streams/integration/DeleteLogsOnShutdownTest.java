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
package org.apache.kafka.streams.integration;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.stream.IntStream;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class DeleteLogsOnShutdownTest {
    public static final int TOPICS = 100;
    public static final int ITERS = 100;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @Test
    public void test() throws Exception {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        assert os instanceof UnixOperatingSystemMXBean : "Debug test not available";

        for (int i = 0; i < ITERS; ++i) {
            CLUSTER.start();
            try {
                IntStream.range(0, TOPICS)
                    .mapToObj(topicIdx -> "topic" + topicIdx)
                    .forEach(t -> {
                        try {
                            CLUSTER.createTopic(t, 10, 3);
                        } catch (final InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
            } finally {
                CLUSTER.stop();
                System.out.println("+++ Iter: " + i + ", open fds: " + ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
            }
        }
    }
}