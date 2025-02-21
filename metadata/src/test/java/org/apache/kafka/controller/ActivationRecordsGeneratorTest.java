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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is for testing the log message or exception produced by ActivationRecordsGenerator. For tests that
 * verify the semantics of the returned records, see QuorumControllerTest.
 */
public class ActivationRecordsGeneratorTest {

    @Test
    public void testActivationMessageForEmptyLog() {
        ControllerResult<Void> result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.3-IV3 from bootstrap source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.MINIMUM_VERSION, "test"),
            MetadataVersion.MINIMUM_VERSION,
            2
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());
    }

    @Test
    public void testActivationMessageForEmptyLogAtMv3_4() {
        ControllerResult<Void> result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.4-IV0 from bootstrap " +
                "source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "test"),
            MetadataVersion.IBP_3_4_IV0,
            2
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());
    }

    @Test
    public void testActivationMessageForEmptyLogAtMv3_6() {
        ControllerResult<Void> result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) in metadata transaction at metadata.version 3.6-IV1 from bootstrap " +
                "source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1,
            2
        );
        assertFalse(result.isAtomic());
        assertEquals(3, result.records().size());
    }

    @Test
    public void testActivationMessageForEmptyLogAtMv3_6WithTransaction() {
        ControllerResult<Void> result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 1 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 3.6-IV1 from bootstrap source 'test'.", logMsg),
            0L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1,
            2
        );
        assertFalse(result.isAtomic());
        assertEquals(4, result.records().size());
    }

    @Test
    public void testActivationMessageForEmptyLogAtMv3_6WithTransactionAndElr() {
        ControllerResult<Void> result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 2 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 4.0-IV1 from bootstrap source 'test'.", logMsg),
            0L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_4_0_IV1, "test").copyWithFeatureRecord(
                EligibleLeaderReplicasVersion.FEATURE_NAME,
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            MetadataVersion.IBP_4_0_IV1,
            2
        );
        assertFalse(result.isAtomic());
        assertEquals(6, result.records().size());
        assertTrue(result.records().contains(new ApiMessageAndVersion(new ConfigRecord().
            setResourceType(ConfigResource.Type.BROKER.id()).
            setResourceName("").
            setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
            setValue("2"), (short) 0)));
    }
}
