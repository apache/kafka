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

package org.apache.kafka.image.writer;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.AclsImageTest;
import org.apache.kafka.image.ClientQuotasImageTest;
import org.apache.kafka.image.ClusterImageTest;
import org.apache.kafka.image.ConfigurationsImageTest;
import org.apache.kafka.image.DelegationTokenImageTest;
import org.apache.kafka.image.FeaturesDelta;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImageTest;
import org.apache.kafka.image.ScramImageTest;
import org.apache.kafka.image.TopicsImageTest;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ImageWriterOptionsTest {
    @Test
    public void testDefaultLossHandler() {
        ImageWriterOptions options = new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build();
        assertEquals("stuff", assertThrows(UnwritableMetadataException.class,
                () -> options.handleLoss("stuff")).loss());
    }

    @Test
    public void testHandleLoss() {
        String expectedMessage = "stuff";

        for (int i = MetadataVersion.MINIMUM_VERSION.ordinal();
             i < MetadataVersion.VERSIONS.length;
             i++) {
            MetadataVersion version = MetadataVersion.VERSIONS[i];
            String formattedMessage = String.format("Metadata has been lost because the following could not be represented in metadata.version %s: %s", version, expectedMessage);
            Consumer<UnwritableMetadataException> customLossHandler = e -> assertEquals(formattedMessage, e.getMessage());
            ImageWriterOptions options = new ImageWriterOptions.Builder(version)
                    .setLossHandler(customLossHandler)
                    .build();
            options.handleLoss(expectedMessage);
        }
    }

    @Test
    public void testSetEligibleLeaderReplicasEnabled() {
        MetadataVersion version = MetadataVersion.MINIMUM_VERSION;
        ImageWriterOptions options = new ImageWriterOptions.Builder(version).
            setEligibleLeaderReplicasEnabled(true).build();
        assertEquals(true, options.isEligibleLeaderReplicasEnabled());

        options = new ImageWriterOptions.Builder(version).build();
        assertEquals(false, options.isEligibleLeaderReplicasEnabled());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConstructionWithImage(boolean isElrEnabled) {
        FeaturesDelta featuresDelta = new FeaturesDelta(FeaturesImage.EMPTY);
        featuresDelta.replay(new FeatureLevelRecord().
            setName(EligibleLeaderReplicasVersion.FEATURE_NAME).
            setFeatureLevel(isElrEnabled ?
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel() : EligibleLeaderReplicasVersion.ELRV_0.featureLevel()
            )
        );
        featuresDelta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_4_0_IV1.featureLevel())
        );
        MetadataImage metadataImage = new MetadataImage(
            new MetadataProvenance(100, 4, 2000, true),
            featuresDelta.apply(),
            ClusterImageTest.IMAGE1,
            TopicsImageTest.IMAGE1,
            ConfigurationsImageTest.IMAGE1,
            ClientQuotasImageTest.IMAGE1,
            ProducerIdsImageTest.IMAGE1,
            AclsImageTest.IMAGE1,
            ScramImageTest.IMAGE1,
            DelegationTokenImageTest.IMAGE1
        );

        ImageWriterOptions options = new ImageWriterOptions.Builder(metadataImage).build();
        assertEquals(MetadataVersion.IBP_4_0_IV1, options.metadataVersion());
        if (isElrEnabled) {
            assertEquals(true, options.isEligibleLeaderReplicasEnabled());
        } else {
            assertEquals(false, options.isEligibleLeaderReplicasEnabled());
        }
    }
}
