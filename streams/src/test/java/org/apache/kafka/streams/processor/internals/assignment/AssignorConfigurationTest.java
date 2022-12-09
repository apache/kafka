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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

public class AssignorConfigurationTest {

    @Test
    public void configsShouldRejectZeroWarmups() {
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> new AssignorConfiguration.AssignmentConfigs(1L, 0, 1, false, 1L, 1L, EMPTY_RACK_AWARE_ASSIGNMENT_TAGS)
        );

        assertThat(exception.getMessage(), containsString("Invalid value 0 for configuration max.warmup.replicas"));
    }
}
