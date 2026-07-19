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

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateTopicsTest {

    @ClusterTest(types = {Type.KRAFT}, brokers = 1, controllers = 1)
    public void testOverlyLargeCreateTopics(ClusterInstance cluster) {
        try (Admin admin = cluster.admin()) {
            var newTopics = new ArrayList<NewTopic>();
            for (int i = 0; i <= 10000; i++) {
                newTopics.add(new NewTopic("foo" + i, 100000, (short) 1));
            }
            var executionException = assertThrows(ExecutionException.class,
                () -> admin.createTopics(newTopics).all().get());
            assertNotNull(executionException.getCause());
            assertEquals(PolicyViolationException.class, executionException.getCause().getClass());
            assertEquals("Excessively large number of partitions per request.",
                executionException.getCause().getMessage());
        }
    }
}
