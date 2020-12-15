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
package org.apache.kafka.common.security.oauthbearer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

public class OAuthBearerTokenCallbackTest {
    private static final OAuthBearerToken TOKEN = new OAuthBearerToken() {
        @Override
        public String value() {
            return "value";
        }

        @Override
        public Long startTimeMs() {
            return null;
        }

        @Override
        public Set<String> scope() {
            return Collections.emptySet();
        }

        @Override
        public String principalName() {
            return "principalName";
        }

        @Override
        public long lifetimeMs() {
            return 0;
        }
    };

    @Test
    public void testError() {
        String errorCode = "errorCode";
        String errorDescription = "errorDescription";
        String errorUri = "errorUri";
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        callback.error(errorCode, errorDescription, errorUri);
        assertEquals(errorCode, callback.errorCode());
        assertEquals(errorDescription, callback.errorDescription());
        assertEquals(errorUri, callback.errorUri());
        assertNull(callback.token());
    }

    @Test
    public void testToken() {
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        callback.token(TOKEN);
        assertSame(TOKEN, callback.token());
        assertNull(callback.errorCode());
        assertNull(callback.errorDescription());
        assertNull(callback.errorUri());
    }
}
