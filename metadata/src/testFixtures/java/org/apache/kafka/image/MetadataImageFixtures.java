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

import org.apache.kafka.metadata.RecordTestUtils;

public final class MetadataImageFixtures {

    public static final MetadataImage IMAGE1 = new MetadataImage(
            new MetadataProvenance(100, 4, 2000, true),
            FeaturesImageFixtures.IMAGE1,
            ClusterImageFixtures.IMAGE1,
            TopicsImageFixtures.IMAGE1,
            ConfigurationsImageFixtures.IMAGE1,
            ClientQuotasImageFixtures.IMAGE1,
            ProducerIdsImageFixtures.IMAGE1,
            AclsImageFixtures.IMAGE1,
            ScramImageFixtures.IMAGE1,
            DelegationTokenImageFixtures.IMAGE1);

    public static final MetadataDelta DELTA1 = buildDelta1();

    public static final MetadataImage IMAGE2 = new MetadataImage(
            new MetadataProvenance(200, 5, 4000, true),
            FeaturesImageFixtures.IMAGE2,
            ClusterImageFixtures.IMAGE2,
            TopicsImageFixtures.IMAGE2,
            ConfigurationsImageFixtures.IMAGE2,
            ClientQuotasImageFixtures.IMAGE2,
            ProducerIdsImageFixtures.IMAGE2,
            AclsImageFixtures.IMAGE2,
            ScramImageFixtures.IMAGE2,
            DelegationTokenImageFixtures.IMAGE2);

    private static MetadataDelta buildDelta1() {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(IMAGE1).build();
        RecordTestUtils.replayAll(delta, FeaturesImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, ClusterImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, TopicsImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, ConfigurationsImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, ClientQuotasImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, ProducerIdsImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, AclsImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, ScramImageFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(delta, DelegationTokenImageFixtures.DELTA1_RECORDS);
        return delta;
    }

    private MetadataImageFixtures() {
    }
}
