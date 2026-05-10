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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FeaturesImageTestFixtures {
    public static final FeaturesImage IMAGE1;
    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;
    public static final FeaturesDelta DELTA1;
    public static final FeaturesImage IMAGE2;
    public static final List<ApiMessageAndVersion> DELTA2_RECORDS;
    public static final FeaturesDelta DELTA2;
    public static final FeaturesImage IMAGE3;

    static {
        Map<String, Short> map1 = new HashMap<>();
        map1.put("foo", (short) 2);
        map1.put("bar", (short) 1);
        IMAGE1 = new FeaturesImage(map1, MetadataVersion.latestTesting());

        DELTA1_RECORDS = new ArrayList<>();
        // change feature level
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 3),
            (short) 0));
        // remove feature
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("bar").setFeatureLevel((short) 0),
            (short) 0));
        // add feature
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("baz").setFeatureLevel((short) 8),
            (short) 0));

        DELTA1 = new FeaturesDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, Short> map2 = new HashMap<>();
        map2.put("foo", (short) 3);
        map2.put("baz", (short) 8);
        IMAGE2 = new FeaturesImage(map2, MetadataVersion.latestTesting());

        DELTA2_RECORDS = new ArrayList<>();
        // remove all features
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setFeatureLevel((short) 0),
            (short) 0));
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("baz").setFeatureLevel((short) 0),
            (short) 0));
        // add feature back with different feature level
        DELTA2_RECORDS.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("bar").setFeatureLevel((short) 1),
            (short) 0));

        DELTA2 = new FeaturesDelta(IMAGE2);
        RecordTestUtils.replayAll(DELTA2, DELTA2_RECORDS);

        Map<String, Short> map3 = Map.of("bar", (short) 1);
        IMAGE3 = new FeaturesImage(map3, MetadataVersion.latestTesting());
    }

    private FeaturesImageTestFixtures() {
    }
}
