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

import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.configProps;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.junit.jupiter.api.Test;

public class RackAwareGraphConstructorFactoryTest {

    @Test
    public void shouldReturnMinCostConstructor() {
        final AssignmentConfigs config = new AssignorConfiguration(
            new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)).originals()).assignmentConfigs();
        final RackAwareGraphConstructor constructor = RackAwareGraphConstructorFactory.create(config, mkMap());
        assertThat(constructor, instanceOf(MinTrafficGraphConstructor.class));
    }

    @Test
    public void shouldReturnBalanceSubtopologyConstructor() {
        final AssignmentConfigs config = new AssignorConfiguration(
            new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)).originals()).assignmentConfigs();
        final RackAwareGraphConstructor constructor = RackAwareGraphConstructorFactory.create(config, mkMap());
        assertThat(constructor, instanceOf(BalanceSubtopologyGraphConstructor.class));
    }
}
