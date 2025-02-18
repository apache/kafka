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

import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class MetadataVersionChangeTest {

    private static final MetadataVersionChange CHANGE_MINIMUM_TO_LATEST =
        new MetadataVersionChange(MetadataVersion.MINIMUM_VERSION, MetadataVersion.latestProduction());

    private static final MetadataVersionChange CHANGE_LATEST_TO_MINIMUM =
        new MetadataVersionChange(MetadataVersion.latestProduction(), MetadataVersion.MINIMUM_VERSION);

    @Test
    public void testIsUpgrade() {
        assertTrue(CHANGE_MINIMUM_TO_LATEST.isUpgrade());
        assertFalse(CHANGE_LATEST_TO_MINIMUM.isUpgrade());
    }

    @Test
    public void testIsDowngrade() {
        assertFalse(CHANGE_MINIMUM_TO_LATEST.isDowngrade());
        assertTrue(CHANGE_LATEST_TO_MINIMUM.isDowngrade());
    }

    @Test
    public void testMetadataVersionChangeExceptionToString() {
        assertEquals("org.apache.kafka.image.MetadataVersionChangeException: The metadata.version " +
            "is changing from " + MetadataVersion.MINIMUM_VERSION + " to " + MetadataVersion.latestProduction(),
                new MetadataVersionChangeException(CHANGE_MINIMUM_TO_LATEST).toString());
        assertEquals("org.apache.kafka.image.MetadataVersionChangeException: The metadata.version " +
            "is changing from " + MetadataVersion.latestProduction() + " to " + MetadataVersion.MINIMUM_VERSION,
                new MetadataVersionChangeException(CHANGE_LATEST_TO_MINIMUM).toString());
    }
}
