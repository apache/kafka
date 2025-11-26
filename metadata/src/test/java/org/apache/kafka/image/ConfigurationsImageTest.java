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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ConfigurationsImageTest {
    public static final ConfigurationsImage IMAGE1;

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final ConfigurationsDelta DELTA1;

    static final ConfigurationsImage IMAGE2;

    static {
        Map<ConfigResource, ConfigurationImage> map1 = new HashMap<>();
        Map<String, String> broker0Map = new HashMap<>();
        broker0Map.put("foo", "bar");
        broker0Map.put("baz", "quux");
        map1.put(new ConfigResource(BROKER, "0"),
            new ConfigurationImage(new ConfigResource(BROKER, "0"), broker0Map));
        Map<String, String> broker1Map = new HashMap<>();
        broker1Map.put("foobar", "foobaz");
        map1.put(new ConfigResource(BROKER, "1"),
            new ConfigurationImage(new ConfigResource(BROKER, "1"), broker1Map));
        IMAGE1 = new ConfigurationsImage(map1);

        DELTA1_RECORDS = new ArrayList<>();
        // remove configs
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("0").setName("foo").setValue(null),
            CONFIG_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("0").setName("baz").setValue(null),
            CONFIG_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("1").setName("foobar").setValue(null),
            CONFIG_RECORD.highestSupportedVersion()));
        // add new config to b1
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("1").setName("barfoo").setValue("bazfoo"),
            CONFIG_RECORD.highestSupportedVersion()));
        // add new config to b2
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("2").setName("foo").setValue("bar"),
            CONFIG_RECORD.highestSupportedVersion()));

        DELTA1 = new ConfigurationsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<ConfigResource, ConfigurationImage> map2 = new HashMap<>();
        Map<String, String> broker1Map2 = Map.of("barfoo", "bazfoo");
        map2.put(new ConfigResource(BROKER, "1"),
            new ConfigurationImage(new ConfigResource(BROKER, "1"), broker1Map2));
        Map<String, String> broker2Map = Map.of("foo", "bar");
        map2.put(new ConfigResource(BROKER, "2"), new ConfigurationImage(new ConfigResource(BROKER, "2"), broker2Map));
        IMAGE2 = new ConfigurationsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(ConfigurationsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1);
        records.addAll(DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    private static void testToImage(ConfigurationsImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(ConfigurationsImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(ConfigurationsImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper<>(
            () -> ConfigurationsImage.EMPTY,
            ConfigurationsDelta::new
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(ConfigurationsImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer);
        return writer.records();
    }

    @Test
    public void testRemovedConfigFiltered() {
        // Create a schema that doesn't include removed configs
        Map<ConfigResource.Type, ConfigDef> configDefs = new HashMap<>();
        ConfigDef topicConfigDef = new ConfigDef();
        topicConfigDef.define("valid.config", ConfigDef.Type.LONG, ConfigDef.Importance.MEDIUM, "doc");
        configDefs.put(TOPIC, topicConfigDef);
        KafkaConfigSchema testSchema = new KafkaConfigSchema(configDefs, Collections.emptyMap());
        ConfigurationsDelta.setConfigSchemaSupplier(() -> testSchema);

        try {
            String testTopic = "test-topic";
            String removedConfig = "message.format.version";
            String validConfig = "valid.config";
            String validConfigValue = "604800000";

            // Test 1: Filter removed configs from base image
            Map<ConfigResource, ConfigurationImage> initialData = new HashMap<>();
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(removedConfig, "0.10.0");
            topicConfigs.put(validConfig, validConfigValue);
            initialData.put(new ConfigResource(TOPIC, testTopic),
                new ConfigurationImage(new ConfigResource(TOPIC, testTopic), topicConfigs));

            ConfigurationsImage initialImage = new ConfigurationsImage(initialData);
            ConfigurationsDelta delta = new ConfigurationsDelta(initialImage);
            ConfigurationsImage finalImage = delta.apply();

            ConfigResource topicResource = new ConfigResource(TOPIC, testTopic);
            ConfigurationImage topicConfig = finalImage.resourceData().get(topicResource);
            assertNotNull(topicConfig);
            assertFalse(topicConfig.data().containsKey(removedConfig), "Removed config should be filtered from base image");
            assertTrue(topicConfig.data().containsKey(validConfig), "Valid config should be present");
            assertEquals(validConfigValue, topicConfig.data().get(validConfig));

            // Test 2: Filter removed configs when replaying records
            ConfigurationsDelta delta2 = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
            List<ApiMessageAndVersion> records = new ArrayList<>();
            records.add(new ApiMessageAndVersion(
                new ConfigRecord()
                    .setResourceType(TOPIC.id())
                    .setResourceName(testTopic)
                    .setName(removedConfig)
                    .setValue("0.10.0"),
                CONFIG_RECORD.highestSupportedVersion()));
            records.add(new ApiMessageAndVersion(
                new ConfigRecord()
                    .setResourceType(TOPIC.id())
                    .setResourceName(testTopic)
                    .setName(validConfig)
                    .setValue(validConfigValue),
                CONFIG_RECORD.highestSupportedVersion()));

            RecordTestUtils.replayAll(delta2, records);
            ConfigurationsImage finalImage2 = delta2.apply();

            ConfigurationImage topicConfig2 = finalImage2.resourceData().get(topicResource);
            assertNotNull(topicConfig2);
            assertFalse(topicConfig2.data().containsKey(removedConfig), "Removed config should be filtered on replay");
            assertTrue(topicConfig2.data().containsKey(validConfig), "Valid config should be present");
            assertEquals(1, topicConfig2.data().size(), "Only valid config should remain");
        } finally {
            ConfigurationsDelta.setConfigSchemaSupplier(null);
        }
    }
}
