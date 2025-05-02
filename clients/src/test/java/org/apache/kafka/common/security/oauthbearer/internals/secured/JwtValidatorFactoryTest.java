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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class JwtValidatorFactoryTest extends OAuthBearerTest {

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorInit() throws IOException {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        JwtRetriever jwtRetriever = new JwtRetriever() {
            @Override
            public void init() throws IOException {
                throw new IOException("My init had an error!");
            }

            @Override
            public String retrieve() {
                return "dummy";
            }
        };

        Map<String, ?> configs = getSaslConfigs();

        try (JwtValidator jwtValidator = new DefaultJwtValidator(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)) {
            assertThrowsWithMessage(
                KafkaException.class, () -> handler.init(jwtRetriever, jwtValidator), "encountered an error when initializing");
        }
    }

    @Test
    public void testConfigureThrowsExceptionOnJwtValidatorClose() throws IOException {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        JwtRetriever jwtRetriever = new JwtRetriever() {
            @Override
            public void close() throws IOException {
                throw new IOException("My close had an error!");
            }
            @Override
            public String retrieve() {
                return "dummy";
            }
        };

        Map<String, ?> configs = getSaslConfigs();
        try (JwtValidator jwtValidator = new DefaultJwtValidator(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)) {
            handler.init(jwtRetriever, jwtValidator);

            // Basically asserting this doesn't throw an exception :(
            handler.close();
        }
    }

}
