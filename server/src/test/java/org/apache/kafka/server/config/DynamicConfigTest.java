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
package org.apache.kafka.server.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicConfigTest {
    @ClusterTest
    public void testGroupIsRemovedWhenDynamicConfigsAreRemoved(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        try (Admin admin = clusterInstance.admin()) {
            var cr = new ConfigResource(ConfigResource.Type.GROUP, "gp");
            assertEquals(List.of(), admin.listConfigResources(Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all().get());

            // add dynamic config
            admin.incrementalAlterConfigs(Map.of(cr, List.of(new AlterConfigOp(
                new ConfigEntry("consumer.session.timeout.ms", "45001"), AlterConfigOp.OpType.SET))))
                .all()
                .get();
            TestUtils.waitForCondition(() -> !admin.listConfigResources(Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all().get().isEmpty(),
                "Should include a group with dynamic config");

            // remove dynamic config
            admin.incrementalAlterConfigs(Map.of(cr, List.of(new AlterConfigOp(
                new ConfigEntry("consumer.session.timeout.ms", null), AlterConfigOp.OpType.DELETE))))
                .all()
                .get();
            TestUtils.waitForCondition(() -> admin.listConfigResources(Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all().get().isEmpty(),
                "Should not include any group");
        }
    }
}
