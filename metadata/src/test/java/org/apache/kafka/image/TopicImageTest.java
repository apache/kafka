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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.MirrorTopicStateChangeRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.image.writer.UnwritableMetadataException;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class TopicImageTest {
    private static final String MIRROR_LOSS = "cluster mirror topic state change";

    private static final Uuid TOPIC_ID = Uuid.fromString("0hHJ3X5ZQ-CFfQ5xgpj90w");

    private static final PartitionRegistration PARTITION0 = new PartitionRegistration.Builder().
            setReplicas(new int[] {1, 2, 3}).
            setDirectories(DirectoryId.migratingArray(3)).
            setIsr(new int[] {1, 2, 3}).
            setLeader(1).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(1).
            setPartitionEpoch(1).
            build();

    private static final PartitionRegistration PARTITION1 = new PartitionRegistration.Builder().
            setReplicas(new int[] {2, 3, 1}).
            setDirectories(DirectoryId.migratingArray(3)).
            setIsr(new int[] {2, 3, 1}).
            setLeader(2).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(1).
            setPartitionEpoch(1).
            build();

    private static final TopicImage MIRRORED_TOPIC = new TopicImage("foo", TOPIC_ID,
            new TopicImage.ActiveMirror("test-mirror", MirrorPartitionState.MIRRORING), Map.of(0, PARTITION0, 1, PARTITION1));

    private static final TopicImage DISASSOCIATED_TOPIC = new TopicImage("foo", TOPIC_ID,
            Map.of(0, PARTITION0, 1, PARTITION1));

    private static List<ApiMessageAndVersion> write(TopicImage image, MetadataVersion metadataVersion, List<String> losses) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(metadataVersion).
                setLossHandler(e -> {
                    assertEquals(metadataVersion, e.metadataVersion());
                    losses.add(e.loss());
                }).
                build());
        return writer.records();
    }

    private static List<ApiMessageAndVersion> expectedRecords(TopicImage image, MetadataVersion metadataVersion, boolean withMirrorRecord) {
        ImageWriterOptions options = new ImageWriterOptions.Builder(metadataVersion).build();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new TopicRecord().setName(image.name()).setTopicId(image.id()), (short) 0));
        if (withMirrorRecord) {
            records.add(new ApiMessageAndVersion(new MirrorTopicStateChangeRecord().
                    setTopicName(image.name()).
                    setMirrorName(((TopicImage.ActiveMirror) image.mirror()).name()).
                    setDesiredState(((TopicImage.ActiveMirror) image.mirror()).desiredState().value()), (short) 0));
        }
        image.partitions().forEach((partitionId, partition) ->
                records.add(partition.toRecord(image.id(), partitionId, options)));
        return records;
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testWriteMirroredTopic(MetadataVersion metadataVersion) {
        boolean mirrorSupported = metadataVersion.isClusterMirroringSupported();
        List<String> losses = new ArrayList<>();

        List<ApiMessageAndVersion> records = write(MIRRORED_TOPIC, metadataVersion, losses);

        // Below IBP_4_5_IV1 the mirror record is dropped and reported as a loss, but the topic and its partitions
        // are still written.
        assertEquals(mirrorSupported ? List.of() : List.of(MIRROR_LOSS), losses);
        assertEquals(expectedRecords(MIRRORED_TOPIC, metadataVersion, mirrorSupported), records);
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testWriteTopicWithoutMirrorHasNoLoss(MetadataVersion metadataVersion) {
        List<String> losses = new ArrayList<>();

        List<ApiMessageAndVersion> records = write(DISASSOCIATED_TOPIC, metadataVersion, losses);

        assertEquals(List.of(), losses);
        assertEquals(expectedRecords(DISASSOCIATED_TOPIC, metadataVersion, false), records);
    }

    @Test
    public void testDefaultLossHandlerThrowsForMirroredTopicBeforeMirrorSupport() {
        ImageWriterOptions options = new ImageWriterOptions.Builder(MetadataVersion.IBP_4_5_IV0).build();
        UnwritableMetadataException e = assertThrows(UnwritableMetadataException.class,
                () -> MIRRORED_TOPIC.write(new RecordListWriter(), options));
        assertEquals(MetadataVersion.IBP_4_5_IV0, e.metadataVersion());
        assertEquals(MIRROR_LOSS, e.loss());
        assertTrue(e.getMessage().contains(MIRROR_LOSS), e.getMessage());
    }

    @Test
    public void testDefaultLossHandlerAllowsTopicWithoutMirrorBeforeMirrorSupport() {
        ImageWriterOptions options = new ImageWriterOptions.Builder(MetadataVersion.IBP_4_5_IV0).build();
        RecordListWriter writer = new RecordListWriter();
        DISASSOCIATED_TOPIC.write(writer, options);
        assertEquals(expectedRecords(DISASSOCIATED_TOPIC, MetadataVersion.IBP_4_5_IV0, false), writer.records());
    }
}