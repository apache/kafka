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
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class MetadataImageTest {
    public static final MetadataImage IMAGE1 = MetadataImageFixtures.IMAGE1;

    public static final MetadataDelta DELTA1 = MetadataImageFixtures.DELTA1;

    public static final MetadataImage IMAGE2 = MetadataImageFixtures.IMAGE2;

    @Test
    public void testEmptyImageRoundTrip() {
        var image = MetadataImage.EMPTY;
        var metadataVersion = MetadataVersion.MINIMUM_VERSION;
        ImageWriterOptions options = new ImageWriterOptions.Builder(metadataVersion).build();
        // A metadata version is required for writing, so the expected image is not actually empty
        var expectedImage = new MetadataImage(
            MetadataProvenance.EMPTY,
            new FeaturesImage(Map.of(), metadataVersion),
            ClusterImage.EMPTY,
            TopicsImage.EMPTY,
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY,
            DelegationTokenImage.EMPTY);
        testToImage(expectedImage, getImageRecords(image, options));
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply(IMAGE2.provenance()));
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        ImageWriterOptions options = new ImageWriterOptions.Builder(IMAGE1.features().metadataVersionOrThrow()).build();
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1, options);
        records.addAll(FeaturesImageTest.DELTA1_RECORDS);
        records.addAll(ClusterImageTest.DELTA1_RECORDS);
        records.addAll(TopicsImageTest.DELTA1_RECORDS);
        records.addAll(ConfigurationsImageTest.DELTA1_RECORDS);
        records.addAll(ClientQuotasImageTest.DELTA1_RECORDS);
        records.addAll(ProducerIdsImageTest.DELTA1_RECORDS);
        records.addAll(AclsImageTest.DELTA1_RECORDS);
        records.addAll(ScramImageTest.DELTA1_RECORDS);
        records.addAll(DelegationTokenImageTest.DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    /**
     * This can be dropped when Cluster Mirror's MV is stable for changes to
     * be added to {@link TopicsImageFixtures#IMAGE1}.
     */
    @Test
    public void testMirrorTopicRoundTrip() {
        MetadataVersion metadataVersion = MetadataVersion.IBP_4_5_IV1;
        TopicImage mirrorTopic = TopicsImageFixtures.newTopicImage(
            "mirror-topic",
            Uuid.fromString("Kz4eV9sJTi6oEcxW1bnK3A"),
            new TopicImage.ActiveMirror("test-mirror", MirrorPartitionState.MIRRORING.value()),
            new PartitionRegistration.Builder().setReplicas(new int[] {1, 2, 3}).
                setDirectories(DirectoryId.migratingArray(3)).
                setIsr(new int[] {1, 2, 3}).setLeader(1).setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(1).setPartitionEpoch(1).build());
        List<TopicImage> topics = new ArrayList<>(IMAGE1.topics().topicsById().values());
        topics.add(mirrorTopic);
        MetadataImage image = new MetadataImage(
            IMAGE1.provenance(),
            new FeaturesImage(IMAGE1.features().finalizedVersions(), metadataVersion),
            IMAGE1.cluster(),
            new TopicsImage(TopicsImageFixtures.newTopicsByIdMap(topics), TopicsImageFixtures.newTopicsByNameMap(topics)),
            IMAGE1.configs(),
            IMAGE1.clientQuotas(),
            IMAGE1.producerIds(),
            IMAGE1.acls(),
            IMAGE1.scram(),
            IMAGE1.delegationTokens());

        List<ApiMessageAndVersion> records = getImageRecords(image, new ImageWriterOptions.Builder(metadataVersion).build());
        assertEquals(
            List.of(new MirrorTopicStateChangeRecord().
                setTopicName("mirror-topic").
                setMirrorName("test-mirror").
                setDesiredState(MirrorPartitionState.MIRRORING.value())),
            records.stream().
                map(ApiMessageAndVersion::message).
                filter(message -> message instanceof MirrorTopicStateChangeRecord).
                toList());

        testToImage(image, records);
    }

    private static void testToImage(MetadataImage image) {
        testToImage(image, new ImageWriterOptions.Builder(image.features().metadataVersionOrThrow()).build(), Optional.empty());
    }

    static void testToImage(MetadataImage image, ImageWriterOptions options, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image, options)));
    }

    private static void testToImage(MetadataImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> MetadataImage.EMPTY,
            img -> new MetadataDelta.Builder().setImage(img).build()
        ) {
            @Override
            public MetadataImage createImageByApplyingDelta(MetadataDelta delta) {
                return delta.apply(image.provenance());
            }
        }.test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(MetadataImage image, ImageWriterOptions options) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, options);
        return writer.records();
    }
}
