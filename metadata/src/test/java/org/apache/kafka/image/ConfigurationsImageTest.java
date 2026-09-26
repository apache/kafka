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
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.SupportedConfigChecker;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ConfigurationsImageTest {
    public static final ConfigurationsImage IMAGE1 = ConfigurationsImageFixtures.IMAGE1;

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = ConfigurationsImageFixtures.DELTA1_RECORDS;

    static final ConfigurationsDelta DELTA1 = ConfigurationsImageFixtures.DELTA1;

    static final ConfigurationsImage IMAGE2 = ConfigurationsImageFixtures.IMAGE2;

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

    @Test
    public void testConfigurationDeltaFiltering() {
        Set<String> validConfigs = Set.of("foo", "bar");
        SupportedConfigChecker supportedConfigChecker = (resourceType, configName) -> validConfigs.contains(configName);

        Map<String, String> initialConfigs = Map.of("foo", "value1"); // valid
        ConfigurationImage image = new ConfigurationImage(new ConfigResource(BROKER, "0"), initialConfigs);

        ConfigurationDelta delta = new ConfigurationDelta(image, supportedConfigChecker);
        delta.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0")
            .setName("bar").setValue("value2"));
        delta.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0")
            .setName("qux").setValue("value3"));

        ConfigurationImage result = delta.apply();

        assertTrue(result.data().containsKey("foo"));
        assertTrue(result.data().containsKey("bar"));
        assertFalse(result.data().containsKey("qux"));
    }

    @Test
    public void testConfigurationDeltaWithoutFiltering() {
        Map<String, String> initialConfigs = Map.of("foo", "value1", "bar", "value2");
        ConfigurationImage image = new ConfigurationImage(new ConfigResource(BROKER, "0"), initialConfigs);

        ConfigurationDelta delta = new ConfigurationDelta(image, SupportedConfigChecker.TRUE);
        delta.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0")
            .setName("baz").setValue("value3"));

        ConfigurationImage result = delta.apply();

        assertTrue(result.data().containsKey("foo"));
        assertTrue(result.data().containsKey("bar"));
        assertTrue(result.data().containsKey("baz"));
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
            img -> new ConfigurationsDelta(img, SupportedConfigChecker.TRUE)
        ).test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(ConfigurationsImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer);
        return writer.records();
    }
}
