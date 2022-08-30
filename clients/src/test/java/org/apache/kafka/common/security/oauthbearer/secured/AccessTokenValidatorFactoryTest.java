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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

public class AccessTokenValidatorFactoryTest extends OAuthBearerTest {

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorInit() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever() {
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
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs);

        assertThrowsWithMessage(
            KafkaException.class, () -> handler.init(accessTokenRetriever, accessTokenValidator), "encountered an error when initializing");
    }

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorClose() {
        OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
        AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever() {
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
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs);
        handler.init(accessTokenRetriever, accessTokenValidator);

        // Basically asserting this doesn't throw an exception :(
        handler.close();
    }

}
