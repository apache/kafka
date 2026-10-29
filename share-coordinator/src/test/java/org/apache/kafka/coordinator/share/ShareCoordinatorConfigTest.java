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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.config.AbstractConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShareCoordinatorConfigTest {

    @Test
    public void testAppendLingerMs() {
        ShareCoordinatorConfig config = createConfig(Map.of(ShareCoordinatorConfig.APPEND_LINGER_MS_CONFIG, -1));
        assertEquals(OptionalInt.empty(), config.shareCoordinatorAppendLingerMs());

        config = createConfig(Map.of(ShareCoordinatorConfig.APPEND_LINGER_MS_CONFIG, 0));
        assertEquals(OptionalInt.of(0), config.shareCoordinatorAppendLingerMs());

        config = createConfig(Map.of(ShareCoordinatorConfig.APPEND_LINGER_MS_CONFIG, 5));
        assertEquals(OptionalInt.of(5), config.shareCoordinatorAppendLingerMs());
    }

    public static ShareCoordinatorConfig createConfig(Map<String, Object> configs) {
        return new ShareCoordinatorConfig(new AbstractConfig(
            ShareCoordinatorConfig.CONFIG_DEF,
            configs,
            false
        ));
    }
}
