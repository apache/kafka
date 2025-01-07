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

package kafka.server;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.FaultHandler;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MetadataVersionConfigValidatorTest {

    private static final LogDeltaManifest TEST_MANIFEST = LogDeltaManifest.newBuilder()
            .provenance(MetadataProvenance.EMPTY)
            .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
            .numBatches(1)
            .elapsedNs(90)
            .numBytes(88)
            .build();
    public static final MetadataProvenance TEST_PROVENANCE =
            new MetadataProvenance(50, 3, 8000, true);

    void testWith(MetadataVersion metadataVersion, KafkaConfig config, FaultHandler faultHandler) throws Exception {
        try (MetadataVersionConfigValidator validator = new MetadataVersionConfigValidator(config, faultHandler)) {
            MetadataDelta delta = new MetadataDelta.Builder()
                    .setImage(MetadataImage.EMPTY)
                    .build();
            if (metadataVersion != null) {
                delta.replay(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(metadataVersion.featureLevel()));
            }
            MetadataImage image = delta.apply(TEST_PROVENANCE);

            validator.onMetadataUpdate(delta, image, TEST_MANIFEST);
        }
    }

    @Test
    void testValidatesConfigOnMetadataChange() throws Exception {
        MetadataVersion metadataVersion = MetadataVersion.IBP_3_7_IV2;
        KafkaConfig config = mock(KafkaConfig.class);
        FaultHandler faultHandler = mock(FaultHandler.class);

        when(config.brokerId()).thenReturn(8);

        testWith(metadataVersion, config, faultHandler);

        verify(config, times(1)).validateWithMetadataVersion(eq(metadataVersion));
        verifyNoMoreInteractions(faultHandler);
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    void testInvokesFaultHandlerOnException() throws Exception {
        MetadataVersion metadataVersion = MetadataVersion.IBP_3_7_IV2;
        Exception exception = new Exception();
        KafkaConfig config = mock(KafkaConfig.class);
        FaultHandler faultHandler = mock(FaultHandler.class);

        when(faultHandler.handleFault(any(), any())).thenReturn(new RuntimeException("returned exception"));
        when(config.brokerId()).thenReturn(8);
        willAnswer(invocation -> {
            throw exception;
        }).given(config).validateWithMetadataVersion(eq(metadataVersion));

        testWith(metadataVersion, config, faultHandler);

        verify(config, times(1)).validateWithMetadataVersion(eq(metadataVersion));
        verify(faultHandler, times(1)).handleFault(
                eq("Broker configuration does not support the cluster MetadataVersion"),
                eq(exception));
    }
}
