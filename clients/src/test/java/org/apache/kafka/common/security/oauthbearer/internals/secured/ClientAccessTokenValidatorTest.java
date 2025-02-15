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
import org.apache.kafka.common.security.oauthbearer.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.AccessTokenValidatorTest;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTestableLoginCallbackHandler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ClientAccessTokenValidatorTest extends AccessTokenValidatorTest {

    @Override
    protected AccessTokenValidator createAccessTokenValidator(AccessTokenBuilder builder) {
        return new ClientAccessTokenValidator();
    }

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorConfigure() {
        try (OAuthBearerTestableLoginCallbackHandler handler = new OAuthBearerTestableLoginCallbackHandler();
             AccessTokenRetriever accessTokenRetriever = mock(AccessTokenRetriever.class);
             AccessTokenValidator accessTokenValidator = mock(AccessTokenValidator.class)) {

            doThrow(new KafkaException("Forced failure")).when(accessTokenValidator).configure(any(), any(), any());

            assertThrowsWithMessage(
                KafkaException.class,
                () -> handler.init(accessTokenRetriever, accessTokenValidator),
                "encountered an error during configuration"
            );
        }
    }

    @Test
    public void testConfigureThrowsExceptionOnAccessTokenValidatorClose() {
        try (OAuthBearerTestableLoginCallbackHandler handler = new OAuthBearerTestableLoginCallbackHandler();
             AccessTokenRetriever accessTokenRetriever = mock(AccessTokenRetriever.class);
             AccessTokenValidator accessTokenValidator = mock(AccessTokenValidator.class)) {
            doThrow(new KafkaException("Forced failure")).when(accessTokenValidator).close();
            assertDoesNotThrow(() -> handler.init(accessTokenRetriever, accessTokenValidator));
            assertThrows(KafkaException.class, handler::close);
        }
    }
}
