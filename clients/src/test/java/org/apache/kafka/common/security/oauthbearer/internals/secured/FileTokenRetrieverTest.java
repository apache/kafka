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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class FileTokenRetrieverTest extends OAuthBearerTest {

    @Test
    public void testFileTokenRetrieverHandlesNewline() throws IOException {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        long cur = cal.getTimeInMillis() / 1000;
        String exp = "" + (cur + 60 * 60);  // 1 hour in future
        String iat = "" + cur;

        String expected = createAccessKey("{}", String.format("{\"exp\":%s, \"iat\":%s, \"sub\":\"subj\"}", exp, iat), "sign");
        String withNewline = expected + "\n";

        File tmpDir = createTempDir("access-token");
        File accessTokenFile = createTempFile(tmpDir, "access-token-", ".json", withNewline);

        List<AppConfigurationEntry> jaasConfigEntries = new ArrayList<>();
        jaasConfigEntries.add(new AppConfigurationEntry("dummy", OPTIONAL, Collections.emptyMap()));

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, accessTokenFile.toURI().toString());
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, accessTokenFile.toURI().toString());

        try (OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler()) {
            handler.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries);
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            handler.handle(new Callback[]{callback});
            assertEquals(callback.token().value(), expected);
        } catch (Exception e) {
            fail(e);
        }
    }
}
