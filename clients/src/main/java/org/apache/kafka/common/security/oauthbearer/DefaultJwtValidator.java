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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * This {@link JwtValidator} uses the delegation approach, instantiating and delegating calls to a
 * more concrete implementation. The underlying implementation is determined by the configuration:
 * if a JWKS endpoint URL is configured or a verification key resolver is provided,
 * a {@link BrokerJwtValidator} is created, otherwise a {@link ClientJwtValidator} is created.
 *
 * <p>Note: {@link BrokerJwtValidator} and its jose4j dependency are loaded lazily via reflection
 * to avoid {@link ClassNotFoundException} in client-only environments where jose4j is not
 * on the classpath.
 */
public class DefaultJwtValidator implements JwtValidator {

    private static final String BROKER_JWT_VALIDATOR_CLASS =
        "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator";

    private static final String CLOSEABLE_VERIFICATION_KEY_RESOLVER_CLASS =
        "org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver";

    private final Optional<Object> verificationKeyResolver;

    private JwtValidator delegate;

    public DefaultJwtValidator() {
        this.verificationKeyResolver = Optional.empty();
    }

    /**
     * @param verificationKeyResolver The resolver (typed as Object to avoid
     *        importing CloseableVerificationKeyResolver, which extends jose4j's
     *        VerificationKeyResolver and would trigger class loading)
     */
    public DefaultJwtValidator(Object verificationKeyResolver) {
        this.verificationKeyResolver = Optional.of(verificationKeyResolver);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (verificationKeyResolver.isPresent()) {
            delegate = createBrokerJwtValidator(verificationKeyResolver.get());
        } else {
            ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);

            if (cu.containsKey(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)) {
                delegate = createBrokerJwtValidator();
            } else {
                delegate = new ClientJwtValidator();
            }
        }

        delegate.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
        if (delegate == null)
            throw new IllegalStateException("JWT validator delegate is null; please call configure() first");

        return delegate.validate(accessToken);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(delegate, "JWT validator delegate");
    }

    JwtValidator delegate() {
        return delegate;
    }

    private static JwtValidator createBrokerJwtValidator() {
        try {
            Class<?> clazz = Class.forName(BROKER_JWT_VALIDATOR_CLASS);
            return (JwtValidator) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new KafkaException(
                BROKER_JWT_VALIDATOR_CLASS + " requires the jose4j library. Please add org.bitbucket.b_c:jose4j to your classpath.", e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new KafkaException("Failed to create " + BROKER_JWT_VALIDATOR_CLASS, e);
        }
    }

    private static JwtValidator createBrokerJwtValidator(Object verificationKeyResolver) {
        try {
            Class<?> clazz = Class.forName(BROKER_JWT_VALIDATOR_CLASS);
            Class<?> resolverClass = Class.forName(CLOSEABLE_VERIFICATION_KEY_RESOLVER_CLASS);
            Constructor<?> ctor = clazz.getDeclaredConstructor(resolverClass);
            return (JwtValidator) ctor.newInstance(verificationKeyResolver);
        } catch (ClassNotFoundException e) {
            throw new KafkaException(
                BROKER_JWT_VALIDATOR_CLASS + " requires the jose4j library. Please add org.bitbucket.b_c:jose4j to your classpath.", e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new KafkaException("Failed to create " + BROKER_JWT_VALIDATOR_CLASS, e);
        }
    }
}
