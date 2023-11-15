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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class MetadataVersionChangeTest {

    private final static MetadataVersionChange CHANGE_3_0_IV1_TO_3_3_IV0 =
        new MetadataVersionChange(IBP_3_0_IV1, IBP_3_3_IV0);

    private final static MetadataVersionChange CHANGE_3_3_IV0_TO_3_0_IV1 =
        new MetadataVersionChange(IBP_3_3_IV0, IBP_3_0_IV1);

    @Test
    public void testIsUpgrade() {
        assertTrue(CHANGE_3_0_IV1_TO_3_3_IV0.isUpgrade());
        assertFalse(CHANGE_3_3_IV0_TO_3_0_IV1.isUpgrade());
    }

    @Test
    public void testIsDowngrade() {
        assertFalse(CHANGE_3_0_IV1_TO_3_3_IV0.isDowngrade());
        assertTrue(CHANGE_3_3_IV0_TO_3_0_IV1.isDowngrade());
    }

    @Test
    public void testMetadataVersionChangeExceptionToString() {
        assertEquals("org.apache.kafka.image.MetadataVersionChangeException: The metadata " +
            "version is changing from 3.0-IV1 to 3.3-IV0",
                new MetadataVersionChangeException(CHANGE_3_0_IV1_TO_3_3_IV0).toString());
        assertEquals("org.apache.kafka.image.MetadataVersionChangeException: The metadata " +
            "version is changing from 3.3-IV0 to 3.0-IV1",
                new MetadataVersionChangeException(CHANGE_3_3_IV0_TO_3_0_IV1).toString());
    }
}
