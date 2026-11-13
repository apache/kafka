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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MinInSyncReplicasTest {

    @Test
    public void testConfiguredValueBelowReplicaCount() {
        assertEquals(2, MinInSyncReplicas.effectiveMinIsr(2, 3));
    }

    @Test
    public void testConfiguredValueEqualToReplicaCount() {
        assertEquals(3, MinInSyncReplicas.effectiveMinIsr(3, 3));
    }

    @Test
    public void testConfiguredValueCappedAtReplicaCount() {
        assertEquals(2, MinInSyncReplicas.effectiveMinIsr(3, 2));
        assertEquals(1, MinInSyncReplicas.effectiveMinIsr(2, 1));
    }
}
