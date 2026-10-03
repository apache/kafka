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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorSourceOffsetValidationConfigTest {

    @Test
    public void testOffsetValidationDefaultsToDisabled() {
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps());

        assertFalse(config.offsetValidationEnabled());
        assertTrue(MirrorSourceConfig.CONNECTOR_CONFIG_DEF.names()
                .contains(MirrorSourceConfig.OFFSET_VALIDATION_ENABLED));
    }

    @Test
    public void testOffsetValidationCanBeEnabledAndPropagatesToTask() {
        MirrorSourceConfig config = new MirrorSourceConfig(
                makeProps(MirrorSourceConfig.OFFSET_VALIDATION_ENABLED, "true"));

        assertTrue(config.offsetValidationEnabled());

        Map<String, String> taskProps = config.taskConfigForTopicPartitions(
                List.of(new TopicPartition("topic", 0)), 0);
        MirrorSourceTaskConfig taskConfig = new MirrorSourceTaskConfig(taskProps);
        assertTrue(taskConfig.offsetValidationEnabled());
    }
}
