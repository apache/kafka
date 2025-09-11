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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.apache.kafka.server.config.DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG;
import static org.apache.kafka.server.config.DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG;
import static org.apache.kafka.server.config.DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG;
import static org.apache.kafka.server.config.DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelegationTokenManagerConfigsTest {
    @Test
    void testDefaults() {
        DelegationTokenManagerConfigs config = new DelegationTokenManagerConfigs(new AbstractConfig(DelegationTokenManagerConfigs.CONFIG_DEF, Map.of()));
        assertNull(config.delegationTokenSecretKey());
        assertFalse(config.tokenAuthEnabled());
        assertEquals(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFE_TIME_MS_DEFAULT, config.delegationTokenMaxLifeMs());
        assertEquals(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_DEFAULT, config.delegationTokenExpiryTimeMs());
        assertEquals(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_DEFAULT, config.delegationTokenExpiryCheckIntervalMs());
    }

    @Test
    void testOverride() {
        DelegationTokenManagerConfigs config = new DelegationTokenManagerConfigs(
            new AbstractConfig(DelegationTokenManagerConfigs.CONFIG_DEF,
                Map.of(
                    DELEGATION_TOKEN_SECRET_KEY_CONFIG, "test",
                    DELEGATION_TOKEN_MAX_LIFETIME_CONFIG, "500",
                    DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG, "200",
                    DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG, "100"
                )
            )
        );
        assertEquals(new Password("test"), config.delegationTokenSecretKey());
        assertTrue(config.tokenAuthEnabled());
        assertEquals(500L, config.delegationTokenMaxLifeMs());
        assertEquals(200L, config.delegationTokenExpiryTimeMs());
        assertEquals(100L, config.delegationTokenExpiryCheckIntervalMs());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        DELEGATION_TOKEN_MAX_LIFETIME_CONFIG,
        DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG,
        DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG
    })
    void testInvalidProperty(String field) {
        assertThrows(Exception.class, () -> new DelegationTokenManagerConfigs(
            new AbstractConfig(DelegationTokenManagerConfigs.CONFIG_DEF, Map.of(field, "not_a_number"))));
    }
}
