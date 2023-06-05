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
package org.apache.kafka.streams.processor.internals.assignment;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

public class StandbyTaskAssignorFactoryTest {
    private static final long ACCEPTABLE_RECOVERY_LAG = 0L;
    private static final int MAX_WARMUP_REPLICAS = 1;
    private static final int NUMBER_OF_STANDBY_REPLICAS = 1;
    private static final long PROBING_REBALANCE_INTERVAL_MS = 60000L;

    @Test
    public void shouldReturnClientTagAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsSet() {
        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(newAssignmentConfigs(singletonList("az")));
        assertTrue(standbyTaskAssignor instanceof ClientTagAwareStandbyTaskAssignor);
    }

    @Test
    public void shouldReturnDefaultStandbyTaskAssignorWhenRackAwareAssignmentTagsIsEmpty() {
        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(newAssignmentConfigs(Collections.emptyList()));
        assertTrue(standbyTaskAssignor instanceof DefaultStandbyTaskAssignor);
    }

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final List<String> rackAwareAssignmentTags) {
        return new AssignorConfiguration.AssignmentConfigs(ACCEPTABLE_RECOVERY_LAG,
                                                           MAX_WARMUP_REPLICAS,
                                                           NUMBER_OF_STANDBY_REPLICAS,
                                                           PROBING_REBALANCE_INTERVAL_MS,
                                                           rackAwareAssignmentTags);
    }
}