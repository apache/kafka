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

package org.apache.kafka.common.test.junit;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.AutoStart;
import org.apache.kafka.common.test.api.ClusterTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ClusterTestExtensions.class)
public class ClusterTestBeforeEachTest {
    private final ClusterInstance clusterInstance;

    ClusterTestBeforeEachTest(ClusterInstance clusterInstance) {     // Constructor injections
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    void before() {
        assertNotNull(clusterInstance);
        if (!clusterInstance.started()) {
            clusterInstance.start();
        }
        assertDoesNotThrow(clusterInstance::waitForReadyBrokers);
    }

    @ClusterTest
    public void testDefault() {
        assertTrue(true);
        assertNotNull(clusterInstance);
    }

    @ClusterTest(autoStart = AutoStart.NO)
    public void testNoAutoStart() {
        assertTrue(true);
        assertNotNull(clusterInstance);
    }
}
