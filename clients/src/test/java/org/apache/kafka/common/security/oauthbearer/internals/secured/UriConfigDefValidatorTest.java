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

import static org.apache.kafka.common.security.oauthbearer.internals.secured.ValidatorCallbackHandlerConfiguration.JWKS_ENDPOINT_URI_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class UriConfigDefValidatorTest {

    @Test
    public void smoke() {
        UriConfigDefValidator validator = new UriConfigDefValidator();
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "http://www.example.com");
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "HTTPS://WWW.EXAMPLE.COM");
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "https://myidp.example.com/oauth2/default/v1/token");
    }

    @Test
    public void missingValues() {
        UriConfigDefValidator validator = new UriConfigDefValidator();
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, null);
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "");
        validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "    ");
    }

    @Test
    public void missingScheme() {
        UriConfigDefValidator validator = new UriConfigDefValidator();
        assertThrowsWithMessage(() -> validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "www.example.com"),
            "missing the scheme");
    }

    @Test
    public void invalidScheme() {
        UriConfigDefValidator validator = new UriConfigDefValidator();
        assertThrowsWithMessage(() -> validator.ensureValid(JWKS_ENDPOINT_URI_CONFIG, "ftp://www.example.com"),
            "invalid scheme");
    }

    protected void assertThrowsWithMessage(Executable validatorExecutable, String substring) {
        assertThrows(ConfigException.class, validatorExecutable);

        try {
            validatorExecutable.execute();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains(substring), String.format("Expected exception message (\"%s\") to contain substring (\"%s\")", e.getMessage(), substring));
        }
    }

}
