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

package org.apache.kafka.metadata.bootstrap;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.apache.kafka.server.common.MetadataVersion.FEATURE_NAME;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(60)
public class BootstrapMetadataTest {
    static final List<ApiMessageAndVersion> SAMPLE_RECORDS1 = List.of(
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(FEATURE_NAME).
            setFeatureLevel((short) 7), (short) 0),
        new ApiMessageAndVersion(new NoOpRecord(), (short) 0),
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(FEATURE_NAME).
            setFeatureLevel((short) 6), (short) 0));

    @Test
    public void testFromVersion() {
        assertEquals(new BootstrapMetadata(Collections.singletonList(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(FEATURE_NAME).
                setFeatureLevel((short) 6), (short) 0)),
                    IBP_3_3_IV2, "foo"),
            BootstrapMetadata.fromVersion(IBP_3_3_IV2, "foo"));
    }

    @Test
    public void testFromRecordsList() {
        assertEquals(new BootstrapMetadata(SAMPLE_RECORDS1, IBP_3_3_IV2, "bar"),
            BootstrapMetadata.fromRecords(SAMPLE_RECORDS1, "bar"));
    }

    @Test
    public void testFromRecordsListWithoutMetadataVersion() {
        assertEquals("No FeatureLevelRecord for metadata.version was found in the bootstrap " +
            "metadata from quux", assertThrows(RuntimeException.class,
                () -> BootstrapMetadata.fromRecords(emptyList(), "quux")).getMessage());
    }

    private static final ApiMessageAndVersion MV_10 =
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(FEATURE_NAME).
            setFeatureLevel((short) 10), (short) 0);

    private static final ApiMessageAndVersion MV_11 =
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(FEATURE_NAME).
            setFeatureLevel((short) 11), (short) 0);

    private static final ApiMessageAndVersion FOO_1 =
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").
            setFeatureLevel((short) 1), (short) 0);

    private static final ApiMessageAndVersion FOO_2 =
        new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").
            setFeatureLevel((short) 2), (short) 0);

    @Test
    public void testCopyWithNewFeatureRecord() {
        assertEquals(BootstrapMetadata.fromRecords(Arrays.asList(MV_10, FOO_1), "src"),
            BootstrapMetadata.fromRecords(Arrays.asList(MV_10), "src").
                copyWithFeatureRecord("foo", (short) 1));
    }

    @Test
    public void testFeatureLevelForMetadataVersion() {
        assertEquals((short) 11, BootstrapMetadata.
            fromRecords(Arrays.asList(MV_10, MV_11), "src").
                featureLevel(FEATURE_NAME));
    }

    @Test
    public void testCopyWithModifiedFeatureRecord() {
        assertEquals(BootstrapMetadata.fromRecords(Arrays.asList(MV_10, FOO_2), "src"),
            BootstrapMetadata.fromRecords(Arrays.asList(MV_10, FOO_1), "src").
                copyWithFeatureRecord("foo", (short) 2));
    }

    @Test
    public void testFeatureLevelForFeatureThatIsNotSet() {
        assertEquals((short) 0, BootstrapMetadata.
            fromRecords(Arrays.asList(MV_10), "src").featureLevel("foo"));
    }

    @Test
    public void testFeatureLevelForFeature() {
        assertEquals((short) 2, BootstrapMetadata.
            fromRecords(Arrays.asList(MV_10, FOO_2), "src").featureLevel("foo"));
    }

    static final List<ApiMessageAndVersion> RECORDS_WITH_OLD_METADATA_VERSION = Collections.singletonList(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(FEATURE_NAME).
                setFeatureLevel(IBP_3_0_IV1.featureLevel()), (short) 0));

    @Test
    public void testFromRecordsListWithOldMetadataVersion() {
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> BootstrapMetadata.fromRecords(RECORDS_WITH_OLD_METADATA_VERSION, "quux"));
        assertEquals("Bootstrap metadata.version before 3.3-IV0 are not supported. Can't load " +
            "metadata from quux", exception.getMessage());
    }
}
