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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetFetchRequestTest {
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testWithMultipleGroups(short version) {
        var builder = new OffsetFetchRequest.Builder(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setPartitionIndexes(List.of(0, 1, 2))
                        )),
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp2")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("bar")
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                )),
            false
        );

        if (version < 8) {
            assertThrows(OffsetFetchRequest.NoBatchedOffsetFetchRequestException.class, () -> builder.build(version));
        } else {
            var request = builder.build(version);
            assertEquals(builder.data, request.data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testThrowOnFetchStableOffsetsUnsupported(short version) {
        var builder = new OffsetFetchRequest.Builder(
            new OffsetFetchRequestData()
                .setRequireStable(true)
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                )),
            true
        );

        if (version < 7) {
            assertThrows(UnsupportedVersionException.class, () -> builder.build(version));
        } else {
            builder.build(version);
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testSingleGroup(short version) {
        var builder = new OffsetFetchRequest.Builder(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                )),
            false
        );

        if (version < 8) {
            var expectedRequest = new OffsetFetchRequestData()
                .setGroupId("grp1")
                .setTopics(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestTopic()
                        .setName("foo")
                        .setPartitionIndexes(List.of(0, 1, 2))
                ));
            assertEquals(expectedRequest, builder.build(version).data());
        } else {
            assertEquals(builder.data, builder.build(version).data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testSingleGroupWithAllTopics(short version) {
        var builder = new OffsetFetchRequest.Builder(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(null)
                )),
            false
        );

        if (version < 2) {
            assertThrows(UnsupportedVersionException.class, () -> builder.build(version));
        } else if (version < 8) {
            var expectedRequest = new OffsetFetchRequestData()
                .setGroupId("grp1")
                .setTopics(null);
            assertEquals(expectedRequest, builder.build(version).data());
        } else {
            assertEquals(builder.data, builder.build(version).data());
        }
    }
}
