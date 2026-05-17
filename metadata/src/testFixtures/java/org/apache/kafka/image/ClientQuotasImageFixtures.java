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

import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.QuotaConfig;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metadata.MetadataRecordType.CLIENT_QUOTA_RECORD;

public final class ClientQuotasImageFixtures {

    public static final ClientQuotasImage IMAGE1 = new ClientQuotasImage(Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo")),
            new ClientQuotaImage(Map.of(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 123.0)),
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "bar", ClientQuotaEntity.IP, "127.0.0.1")),
            new ClientQuotaImage(Map.of(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 456.0))
    ));

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
            // remove quota
            new ApiMessageAndVersion(new ClientQuotaRecord().
                    setEntity(List.of(
                            new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("bar"),
                            new EntityData().setEntityType(ClientQuotaEntity.IP).setEntityName("127.0.0.1"))).
                    setKey(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).
                    setRemove(true), CLIENT_QUOTA_RECORD.highestSupportedVersion()),
            // alter quota
            new ApiMessageAndVersion(new ClientQuotaRecord().
                    setEntity(List.of(
                            new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("foo"))).
                    setKey(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG).
                    setValue(234.0), CLIENT_QUOTA_RECORD.highestSupportedVersion()),
            // add quota to entity with existing quota
            new ApiMessageAndVersion(new ClientQuotaRecord().
                    setEntity(List.of(
                            new EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName("foo"))).
                    setKey(QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG).
                    setValue(999.0), CLIENT_QUOTA_RECORD.highestSupportedVersion())
    );

    public static final ClientQuotasDelta DELTA1 = RecordTestUtils.replayAll(
            new ClientQuotasDelta(IMAGE1),
            DELTA1_RECORDS
    );

    public static final ClientQuotasImage IMAGE2 = new ClientQuotasImage(Map.of(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "foo")),
            new ClientQuotaImage(Map.of(
                    QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 234.0,
                    QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 999.0
            ))
    ));

    private ClientQuotasImageFixtures() {
    }
}
