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
package org.apache.kafka.common.security.oauthbearer.internals.expiring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.Test;

public class ExpiringCredentialRefreshConfigTest {
    @Test
    public void fromGoodConfig() {
        ExpiringCredentialRefreshConfig expiringCredentialRefreshConfig = new ExpiringCredentialRefreshConfig(
                new ConfigDef().withClientSaslSupport().parse(Collections.emptyMap()), true);
        assertEquals(Double.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR),
                Double.valueOf(expiringCredentialRefreshConfig.loginRefreshWindowFactor()));
        assertEquals(Double.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER),
                Double.valueOf(expiringCredentialRefreshConfig.loginRefreshWindowJitter()));
        assertEquals(Short.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS),
                Short.valueOf(expiringCredentialRefreshConfig.loginRefreshMinPeriodSeconds()));
        assertEquals(Short.valueOf(SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS),
                Short.valueOf(expiringCredentialRefreshConfig.loginRefreshBufferSeconds()));
        assertTrue(expiringCredentialRefreshConfig.loginRefreshReloginAllowedBeforeLogout());
    }
}
