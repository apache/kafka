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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.SupportedConfigChecker;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;

public final class ConfigurationsImageFixtures {

    public static final ConfigurationsImage IMAGE1 = new ConfigurationsImage(Map.of(
        new ConfigResource(BROKER, "0"), new ConfigurationImage(new ConfigResource(BROKER, "0"), Map.of("foo", "bar", "baz", "quux")),
        new ConfigResource(BROKER, "1"), new ConfigurationImage(new ConfigResource(BROKER, "1"), Map.of("foobar", "foobaz"))
    ));

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
        new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id())
            .setResourceName("0").setName("foo").setValue(null), CONFIG_RECORD.highestSupportedVersion()),
        new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id())
            .setResourceName("0").setName("baz").setValue(null), CONFIG_RECORD.highestSupportedVersion()),
        new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id())
            .setResourceName("1").setName("foobar").setValue(null), CONFIG_RECORD.highestSupportedVersion()),
        new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id())
            .setResourceName("1").setName("barfoo").setValue("bazfoo"), CONFIG_RECORD.highestSupportedVersion()),
        new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id())
            .setResourceName("2").setName("foo").setValue("bar"), CONFIG_RECORD.highestSupportedVersion())
    );

    public static final ConfigurationsDelta DELTA1 = RecordTestUtils.replayAll(
        new ConfigurationsDelta(IMAGE1, SupportedConfigChecker.TRUE),
        DELTA1_RECORDS
    );

    public static final ConfigurationsImage IMAGE2 = new ConfigurationsImage(Map.of(
        new ConfigResource(BROKER, "1"), new ConfigurationImage(new ConfigResource(BROKER, "1"), Map.of("barfoo", "bazfoo")),
        new ConfigResource(BROKER, "2"), new ConfigurationImage(new ConfigResource(BROKER, "2"), Map.of("foo", "bar"))
    ));

    private ConfigurationsImageFixtures() {
    }
}
