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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.image.writer.UnwritableMetadataException;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@Timeout(value = 40)
public class ImageDowngradeTest {
    static class MockLossConsumer implements Consumer<UnwritableMetadataException> {
        private final MetadataVersion expectedMetadataVersion;
        private final List<String> losses;

        MockLossConsumer(MetadataVersion expectedMetadataVersion) {
            this.expectedMetadataVersion = expectedMetadataVersion;
            this.losses = new ArrayList<>();
        }

        @Override
        public void accept(UnwritableMetadataException e) {
            assertEquals(expectedMetadataVersion, e.metadataVersion());
            losses.add(e.loss());
        }
    }

    static final List<ApiMessageAndVersion> TEST_RECORDS = Arrays.asList(
            new ApiMessageAndVersion(new TopicRecord().
                    setName("foo").
                    setTopicId(Uuid.fromString("5JPuABiJTPu2pQjpZWM6_A")), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord().
                    setTopicId(Uuid.fromString("5JPuABiJTPu2pQjpZWM6_A")).
                    setReplicas(Arrays.asList(0, 1)).
                    setIsr(Arrays.asList(0, 1)).
                    setLeader(0).
                    setLeaderEpoch(1).
                    setPartitionEpoch(2), (short) 0));

    static ApiMessageAndVersion metadataVersionRecord(MetadataVersion metadataVersion) {
        return new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), (short) 0);
    }

    /**
     * Test downgrading to a MetadataVersion that doesn't support FeatureLevelRecord.
     */
    @Test
    public void testPremodernVersion() {
        writeWithExpectedLosses(MetadataVersion.IBP_3_2_IV0,
            Arrays.asList(
                "feature flag(s): foo.feature"),
            Arrays.asList(
                metadataVersionRecord(MetadataVersion.IBP_3_3_IV0),
                TEST_RECORDS.get(0),
                TEST_RECORDS.get(1),
                new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName("foo.feature").
                        setFeatureLevel((short) 4), (short) 0)),
            Arrays.asList(
                TEST_RECORDS.get(0),
                TEST_RECORDS.get(1)));
    }

    /**
     * Test downgrading to a MetadataVersion that doesn't support inControlledShutdown.
     */
    @Test
    public void testPreControlledShutdownStateVersion() {
        writeWithExpectedLosses(MetadataVersion.IBP_3_3_IV2,
                Arrays.asList(
                        "the inControlledShutdown state of one or more brokers"),
                Arrays.asList(
                        metadataVersionRecord(MetadataVersion.IBP_3_3_IV3),
                        new ApiMessageAndVersion(new RegisterBrokerRecord().
                            setBrokerId(123).
                            setIncarnationId(Uuid.fromString("XgjKo16hRWeWrTui0iR5Nw")).
                            setBrokerEpoch(456).
                            setRack(null).
                            setFenced(false).
                            setInControlledShutdown(true), (short) 1),
                        TEST_RECORDS.get(0),
                        TEST_RECORDS.get(1)),
                Arrays.asList(
                        metadataVersionRecord(MetadataVersion.IBP_3_3_IV2),
                        new ApiMessageAndVersion(new RegisterBrokerRecord().
                            setBrokerId(123).
                            setIncarnationId(Uuid.fromString("XgjKo16hRWeWrTui0iR5Nw")).
                            setBrokerEpoch(456).
                            setRack(null).
                            setFenced(false), (short) 0),
                        TEST_RECORDS.get(0),
                        TEST_RECORDS.get(1)));
    }

    @Test
    void testDirectoryAssignmentState() {
        MetadataVersion outputMetadataVersion = MetadataVersion.IBP_3_7_IV0;
        MetadataVersion inputMetadataVersion = spy(outputMetadataVersion); // TODO replace with actual MV after bump for KIP-858
        when(inputMetadataVersion.isDirectoryAssignmentSupported()).thenReturn(true);
        PartitionRecord testPartitionRecord = (PartitionRecord) TEST_RECORDS.get(1).message();
        writeWithExpectedLosses(outputMetadataVersion,
                Collections.singletonList("the directory assignment state of one or more replicas"),
                Arrays.asList(
                        metadataVersionRecord(inputMetadataVersion),
                        TEST_RECORDS.get(0),
                        new ApiMessageAndVersion(
                                testPartitionRecord.duplicate().setDirectories(Arrays.asList(
                                        Uuid.fromString("c7QfSi6xSIGQVh3Qd5RJxA"),
                                        Uuid.fromString("rWaCHejCRRiptDMvW5Xw0g"))),
                                (short) 2)),
                Arrays.asList(
                        metadataVersionRecord(outputMetadataVersion),
                        new ApiMessageAndVersion(new ZkMigrationStateRecord(), (short) 0),
                        TEST_RECORDS.get(0),
                        new ApiMessageAndVersion(
                                testPartitionRecord.duplicate().setDirectories(Collections.emptyList()),
                                (short) 0)));
    }

    private static void writeWithExpectedLosses(
        MetadataVersion metadataVersion,
        List<String> expectedLosses,
        List<ApiMessageAndVersion> inputs,
        List<ApiMessageAndVersion> expectedOutputs
    ) {
        MockLossConsumer lossConsumer = new MockLossConsumer(metadataVersion);
        MetadataDelta delta = new MetadataDelta.Builder().build();
        RecordTestUtils.replayAll(delta, inputs);
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().
                setMetadataVersion(metadataVersion).
                setLossHandler(lossConsumer).
                build());
        assertEquals(expectedLosses, lossConsumer.losses, "Failed to get expected metadata losses.");
        assertEquals(expectedOutputs, writer.records(), "Failed to get expected output records.");
    }
}
