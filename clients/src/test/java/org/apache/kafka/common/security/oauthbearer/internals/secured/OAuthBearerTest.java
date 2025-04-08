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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class OAuthBearerTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Time time = new MockTime();

    protected ObjectMapper mapper = new ObjectMapper();

    protected void assertThrowsWithMessage(Class<? extends Exception> clazz,
        Executable executable,
        String substring) {
        assertErrorMessageContains(assertThrows(clazz, executable).getMessage(), substring);
    }

    protected void assertErrorMessageContains(String actual, String expectedSubstring) {
        assertTrue(actual.contains(expectedSubstring),
            String.format("Expected exception message (\"%s\") to contain substring (\"%s\")",
                actual,
                expectedSubstring));
    }

    protected void configureHandler(AuthenticateCallbackHandler handler,
        Map<String, ?> configs,
        Map<String, Object> jaasConfig) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry("KafkaClient", OAuthBearerLoginModule.class.getName(), jaasConfig);
        AppConfigurationEntry kafkaClient = config.getAppConfigurationEntry("KafkaClient")[0];

        handler.configure(configs,
            OAUTHBEARER_MECHANISM,
            Collections.singletonList(kafkaClient));
    }

    protected String createAccessKey(String header, String payload, String signature) {
        Base64.Encoder enc = Base64.getEncoder();
        header = enc.encodeToString(Utils.utf8(header));
        payload = enc.encodeToString(Utils.utf8(payload));
        signature = enc.encodeToString(Utils.utf8(signature));
        return String.format("%s.%s.%s", header, payload, signature);
    }

    protected String createBase64JsonJwtSection(Consumer<ObjectNode> c) {
        String json = createJsonJwtSection(c);

        try {
            return Utils.utf8(Base64.getEncoder().encode(Utils.utf8(json)));
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

    protected String createJsonJwtSection(Consumer<ObjectNode> c) {
        ObjectNode node = mapper.createObjectNode();
        c.accept(node);

        try {
            return mapper.writeValueAsString(node);
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

    protected File createTempFile(String prefix, String suffix, String contents) throws IOException {
        File file = TestUtils.tempFile(prefix, suffix);
        Files.writeString(file.toPath(), contents);
        log.debug("Created new temp file {}", file);
        return file;
    }

    protected Map<String, ?> getSaslConfigs(Map<String, ?> configs) {
        ConfigDef configDef = new ConfigDef();
        configDef.withClientSaslSupport();
        AbstractConfig sslClientConfig = new AbstractConfig(configDef, configs);
        return sslClientConfig.values();
    }

    protected Map<String, ?> getSaslConfigs(String name, Object value) {
        return getSaslConfigs(Collections.singletonMap(name, value));
    }

    protected Map<String, ?> getSaslConfigs() {
        return getSaslConfigs(Collections.emptyMap());
    }

    protected PublicJsonWebKey createRsaJwk() throws JoseException {
        RsaJsonWebKey jwk = RsaJwkGenerator.generateJwk(2048);
        jwk.setKeyId("key-1");
        return jwk;
    }

    protected PublicJsonWebKey createEcJwk() throws JoseException {
        PublicJsonWebKey jwk = PublicJsonWebKey.Factory.newPublicJwk("{" +
            "  \"kty\": \"EC\"," +
            "  \"d\": \"Tk7qzHNnSBMioAU7NwZ9JugFWmWbUCyzeBRjVcTp_so\"," +
            "  \"use\": \"sig\"," +
            "  \"crv\": \"P-256\"," +
            "  \"kid\": \"key-1\"," +
            "  \"x\": \"qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I\"," +
            "  \"y\": \"wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc\"" +
            "}");
        jwk.setKeyId("key-1");
        return jwk;
    }
}