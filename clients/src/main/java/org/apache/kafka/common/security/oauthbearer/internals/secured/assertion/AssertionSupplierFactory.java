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

package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.layeredAssertionJwtTemplate;

/**
 * Factory class for creating JWT assertion suppliers used in OAuth 2.0 client authentication.
 *
 * <p>
 * This factory supports two methods of obtaining JWT assertions for client authentication:
 * <ul>
 *   <li><b>File-based assertions:</b> Pre-generated JWT assertions read from a file specified by
 *       {@link SaslConfigs#SASL_OAUTHBEARER_ASSERTION_FILE}.
 *       This is useful for testing or when assertions are managed externally.</li>
 *   <li><b>Dynamically-generated assertions:</b> JWTs dynamically created and signed using a private key
 *       specified by {@link SaslConfigs#SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE}.
 *       This is the recommended approach for production use.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The created supplier can be invoked repeatedly to obtain assertions as needed (for example, when
 * refreshing tokens). For file-based assertions, the assertion is cached and reloaded automatically when the file changes on disk.
 * For dynamically-generated assertions, a new assertion with updated timestamps is created on each invocation.
 * </p>
 */
public class AssertionSupplierFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AssertionSupplierFactory.class);

    /**
     * Creates a closeable supplier that provides JWT assertions based on the provided configuration.
     *
     * <p>
     * If {@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_ASSERTION_FILE} is configured,
     * the supplier will read assertions from that file. Otherwise, it will create assertions dynamically
     * using the configured private key and algorithm.
     * </p>
     *
     * <p>
     * <b>Important:</b> The returned {@link CloseableSupplier} must be closed when no longer needed
     * to properly release resources.
     * </p>
     *
     * @param cu   The configuration utilities containing assertion configuration
     * @param time The time source for generating timestamps in dynamically-created assertions
     * @return A closeable supplier that provides JWT assertion strings when invoked
     * @throws ConfigException if required configuration is missing or invalid
     * @throws JwtRetrieverException if assertion creation fails (wrapped in the returned supplier)
     */
    public static CloseableSupplier<String> create(ConfigurationUtils cu, Time time) {
        AssertionJwtTemplate assertionJwtTemplate;
        AssertionCreator assertionCreator;

        if (cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false) != null) {
            File assertionFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE);
            LOG.info("Configuring File based assertion using file: {}", assertionFile.getAbsolutePath());
            assertionCreator = new FileAssertionCreator(assertionFile);
            assertionJwtTemplate = new StaticAssertionJwtTemplate();
        } else {
            String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
            File privateKeyFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE);
            Optional<String> passphrase = cu.containsKey(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE) ?
                Optional.of(cu.validatePassword(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)) :
                Optional.empty();
            LOG.debug("Configuring dynamic assertion creation using algorithm: {} and private key file: {}",
                algorithm, privateKeyFile.getAbsolutePath());
            assertionCreator = new DefaultAssertionCreator(algorithm, privateKeyFile, passphrase);
            assertionJwtTemplate = layeredAssertionJwtTemplate(cu, time);
        }

        return new AssertionSupplier(assertionCreator, assertionJwtTemplate);
    }

    /**
     * Implementation of {@link CloseableSupplier} that wraps an {@link AssertionCreator} and
     * {@link AssertionJwtTemplate}, providing proper resource cleanup.
     */
    private static class AssertionSupplier implements CloseableSupplier<String> {
        private final AssertionCreator assertionCreator;
        private final AssertionJwtTemplate assertionJwtTemplate;

        AssertionSupplier(AssertionCreator assertionCreator, AssertionJwtTemplate assertionJwtTemplate) {
            this.assertionCreator = assertionCreator;
            this.assertionJwtTemplate = assertionJwtTemplate;
        }

        @Override
        public String get() {
            try {
                return assertionCreator.create(assertionJwtTemplate);
            } catch (Exception e) {
                throw new JwtRetrieverException(e);
            }
        }

        @Override
        public void close() throws IOException {
            Utils.closeQuietly(assertionCreator, "JWT assertion creator");
            Utils.closeQuietly(assertionJwtTemplate, "JWT assertion template");
        }
    }
}
