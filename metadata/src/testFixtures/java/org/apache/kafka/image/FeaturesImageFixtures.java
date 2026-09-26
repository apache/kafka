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

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.List;
import java.util.Map;

public final class FeaturesImageFixtures {

    public static final FeaturesImage IMAGE1 = new FeaturesImage(
        Map.of("foo", (short) 2, "bar", (short) 1),
        MetadataVersion.latestTesting()
    );

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 3), (short) 0),
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("bar").setFeatureLevel((short) 0), (short) 0),
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("baz").setFeatureLevel((short) 8), (short) 0)
    );

    public static final FeaturesDelta DELTA1 = RecordTestUtils.replayAll(
        new FeaturesDelta(IMAGE1),
        DELTA1_RECORDS
    );

    public static final FeaturesImage IMAGE2 = new FeaturesImage(
        Map.of("foo", (short) 3, "baz", (short) 8),
        MetadataVersion.latestTesting()
    );

    public static final List<ApiMessageAndVersion> DELTA2_RECORDS = List.of(
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("foo").setFeatureLevel((short) 0), (short) 0),
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("baz").setFeatureLevel((short) 0), (short) 0),
        new ApiMessageAndVersion(new FeatureLevelRecord().setName("bar").setFeatureLevel((short) 1), (short) 0)
    );

    public static final FeaturesDelta DELTA2 = RecordTestUtils.replayAll(
        new FeaturesDelta(IMAGE2),
        DELTA2_RECORDS
    );

    public static final FeaturesImage IMAGE3 = new FeaturesImage(
        Map.of("bar", (short) 1),
        MetadataVersion.latestTesting()
    );

    private FeaturesImageFixtures() {
    }
}
