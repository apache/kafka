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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeConfigsTest {

    @ClusterTest
    public void testDescribeConfigsNonexistent(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // BROKER and BROKER_LOGGER requests are routed to the node named by the resource, so a nonexistent node id
            // can only be reported as a timeout. Use a short timeout to avoid waiting for the default API timeout.
            DescribeConfigsOptions shortTimeout = new DescribeConfigsOptions().timeoutMs(1000);

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "-1");
            ExecutionException brokerException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(brokerResource), shortTimeout).all().get());
            assertInstanceOf(TimeoutException.class, brokerException.getCause());

            ConfigResource brokerLoggerResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "-1");
            ExecutionException brokerLoggerException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(brokerLoggerResource), shortTimeout).all().get());
            assertInstanceOf(TimeoutException.class, brokerLoggerException.getCause());

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "none_topic");
            ExecutionException topicException = assertThrows(ExecutionException.class,
                () -> admin.describeConfigs(List.of(topicResource)).all().get());
            assertInstanceOf(UnknownTopicOrPartitionException.class, topicException.getCause());

            // a nonexistent group is not an error: the default group configs are returned
            ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, "none_group");
            Config groupConfig = admin.describeConfigs(List.of(groupResource)).all().get().get(groupResource);
            assertNotEquals(0, groupConfig.entries().size());
        }
    }
}
