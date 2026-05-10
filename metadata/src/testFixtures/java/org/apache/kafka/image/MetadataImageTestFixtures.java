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

public final class MetadataImageTestFixtures {
    public static final MetadataImage IMAGE1;
    public static final MetadataDelta DELTA1;
    public static final MetadataImage IMAGE2;

    static {
        IMAGE1 = new MetadataImage(
            new MetadataProvenance(100, 4, 2000, true),
            FeaturesImageTestFixtures.IMAGE1,
            ClusterImageTestFixtures.IMAGE1,
            TopicsImageTestFixtures.IMAGE1,
            ConfigurationsImageTestFixtures.IMAGE1,
            ClientQuotasImageTestFixtures.IMAGE1,
            ProducerIdsImageTestFixtures.IMAGE1,
            AclsImageTestFixtures.IMAGE1,
            ScramImageTestFixtures.IMAGE1,
            DelegationTokenImageTestFixtures.IMAGE1);

        DELTA1 = new MetadataDelta.Builder().
                setImage(IMAGE1).
                build();
        RecordTestUtils.replayAll(DELTA1, FeaturesImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ClusterImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, TopicsImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ConfigurationsImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ClientQuotasImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ProducerIdsImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, AclsImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, ScramImageTestFixtures.DELTA1_RECORDS);
        RecordTestUtils.replayAll(DELTA1, DelegationTokenImageTestFixtures.DELTA1_RECORDS);

        IMAGE2 = new MetadataImage(
            new MetadataProvenance(200, 5, 4000, true),
            FeaturesImageTestFixtures.IMAGE2,
            ClusterImageTestFixtures.IMAGE2,
            TopicsImageTestFixtures.IMAGE2,
            ConfigurationsImageTestFixtures.IMAGE2,
            ClientQuotasImageTestFixtures.IMAGE2,
            ProducerIdsImageTestFixtures.IMAGE2,
            AclsImageTestFixtures.IMAGE2,
            ScramImageTestFixtures.IMAGE2,
            DelegationTokenImageTestFixtures.IMAGE2);
    }

    private MetadataImageTestFixtures() {
    }
}
