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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinatorStreamsRecordHelpersTest {

    @Test
    public void testNewStreamsGroupTopologyRecord() {
        List<StreamsGroupInitializeRequestData.Subtopology> topology =
            Collections.singletonList(new StreamsGroupInitializeRequestData.Subtopology()
                .setSubtopologyId("subtopology-id")
                .setRepartitionSinkTopics(Collections.singletonList("foo"))
                .setSourceTopics(Collections.singletonList("bar"))
                .setRepartitionSourceTopics(
                    Collections.singletonList(
                        new StreamsGroupInitializeRequestData.TopicInfo()
                            .setName("repartition")
                            .setPartitions(4)
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupInitializeRequestData.TopicConfig()
                                    .setKey("config-name1")
                                    .setValue("config-value1")
                            ))
                    )
                )
                .setStateChangelogTopics(
                    Collections.singletonList(
                        new StreamsGroupInitializeRequestData.TopicInfo()
                            .setName("changelog")
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupInitializeRequestData.TopicConfig()
                                    .setKey("config-name2")
                                    .setValue("config-value2")
                            ))
                    )
                )
            );

        List<StreamsGroupTopologyValue.Subtopology> expectedTopology =
            Collections.singletonList(new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId("subtopology-id")
                .setRepartitionSinkTopics(Collections.singletonList("foo"))
                .setSourceTopics(Collections.singletonList("bar"))
                .setRepartitionSourceTopics(
                    Collections.singletonList(
                        new StreamsGroupTopologyValue.TopicInfo()
                            .setName("repartition")
                            .setPartitions(4)
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupTopologyValue.TopicConfig()
                                    .setKey("config-name1")
                                    .setValue("config-value1")
                            ))
                    )
                )
                .setStateChangelogTopics(
                    Collections.singletonList(
                        new StreamsGroupTopologyValue.TopicInfo()
                            .setName("changelog")
                            .setTopicConfigs(Collections.singletonList(
                                new StreamsGroupTopologyValue.TopicConfig()
                                    .setKey("config-name2")
                                    .setValue("config-value2")
                            ))
                    )
                )
            );

        CoordinatorRecord expectedRecord = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new StreamsGroupTopologyKey()
                    .setGroupId("group-id"),
                (short) 21),
            new ApiMessageAndVersion(
                new StreamsGroupTopologyValue()
                    .setTopology(expectedTopology),
                (short) 0));

        assertEquals(expectedRecord, CoordinatorStreamsRecordHelpers.newStreamsGroupTopologyRecord(
            "group-id",
            topology
        ));
    }
}