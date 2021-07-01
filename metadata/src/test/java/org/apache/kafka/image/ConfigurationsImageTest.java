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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ConfigurationsImageTest {
    final static ConfigurationsImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static ConfigurationsDelta DELTA1;

    final static ConfigurationsImage IMAGE2;

    static {
        Map<ConfigResource, ConfigurationImage> map1 = new HashMap<>();
        Map<String, String> broker0Map = new HashMap<>();
        broker0Map.put("foo", "bar");
        broker0Map.put("baz", "quux");
        map1.put(new ConfigResource(BROKER, "0"),
            new ConfigurationImage(broker0Map));
        Map<String, String> broker1Map = new HashMap<>();
        broker1Map.put("foobar", "foobaz");
        map1.put(new ConfigResource(BROKER, "1"),
            new ConfigurationImage(broker1Map));
        IMAGE1 = new ConfigurationsImage(map1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("0").setName("foo").setValue(null),
            CONFIG_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(BROKER.id()).
            setResourceName("1").setName("barfoo").setValue("bazfoo"),
            CONFIG_RECORD.highestSupportedVersion()));

        DELTA1 = new ConfigurationsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<ConfigResource, ConfigurationImage> map2 = new HashMap<>();
        Map<String, String> broker0Map2 = new HashMap<>();
        broker0Map2.put("baz", "quux");
        map2.put(new ConfigResource(BROKER, "0"),
            new ConfigurationImage(broker0Map2));
        Map<String, String> broker1Map2 = new HashMap<>();
        broker1Map2.put("foobar", "foobaz");
        broker1Map2.put("barfoo", "bazfoo");
        map2.put(new ConfigResource(BROKER, "1"),
            new ConfigurationImage(broker1Map2));
        IMAGE2 = new ConfigurationsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(ConfigurationsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(ConfigurationsImage image) throws Throwable {
        MockSnapshotConsumer writer = new MockSnapshotConsumer();
        image.write(writer);
        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        RecordTestUtils.replayAllBatches(delta, writer.batches());
        ConfigurationsImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
