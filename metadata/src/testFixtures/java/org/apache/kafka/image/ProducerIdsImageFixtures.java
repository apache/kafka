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

import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;

public final class ProducerIdsImageFixtures {

    public static final ProducerIdsImage IMAGE1 = new ProducerIdsImage(123);

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
        new ApiMessageAndVersion(new ProducerIdsRecord().setBrokerId(2).setBrokerEpoch(100).setNextProducerId(456), (short) 0),
        new ApiMessageAndVersion(new ProducerIdsRecord().setBrokerId(3).setBrokerEpoch(100).setNextProducerId(780), (short) 0),
        new ApiMessageAndVersion(new ProducerIdsRecord().setBrokerId(3).setBrokerEpoch(100).setNextProducerId(785), (short) 0),
        new ApiMessageAndVersion(new ProducerIdsRecord().setBrokerId(2).setBrokerEpoch(100).setNextProducerId(800), (short) 0)
    );

    public static final ProducerIdsDelta DELTA1 = RecordTestUtils.replayAll(
        new ProducerIdsDelta(IMAGE1),
        DELTA1_RECORDS
    );

    public static final ProducerIdsImage IMAGE2 = new ProducerIdsImage(800);

    private ProducerIdsImageFixtures() {
    }
}
