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

package org.apache.kafka.connect.rest.basic.auth.extension;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provides the ability to authenticate incoming BasicAuth credentials using the configured JAAS {@link
 * javax.security.auth.spi.LoginModule}. An entry with the name {@code KafkaConnect} is expected in the JAAS config file configured in the
 * JVM. An implementation of {@link javax.security.auth.spi.LoginModule} needs to be provided in the JAAS config file. The {@code
 * LoginModule} implementation should configure the {@link javax.security.auth.callback.CallbackHandler} with only {@link
 * javax.security.auth.callback.NameCallback} and {@link javax.security.auth.callback.PasswordCallback}.
 *
 * <p>To use this extension, one needs to add the following config in the {@code worker.properties}
 * <pre>
 *     rest.extension.classes = org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension
 * </pre>
 *
 * <p> An example JAAS config would look as below
 * <Pre>
 *         KafkaConnect {
 *              org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule required
 *              file="/mnt/secret/credentials.properties";
 *         };
 *</Pre>
 *
 * <p>This is a reference implementation of the {@link ConnectRestExtension} interface. It registers an implementation of {@link
 * javax.ws.rs.container.ContainerRequestFilter} that does JAAS based authentication of incoming Basic Auth credentials. {@link
 * ConnectRestExtension} implementations are loaded via the plugin class loader using {@link java.util.ServiceLoader} mechanism and hence
 * the packaged jar includes {@code META-INF/services/org.apache.kafka.connect.rest.extension.ConnectRestExtension} with the entry
 * {@code org.apache.kafka.connect.extension.auth.jaas.BasicAuthSecurityRestExtension}
 *
 * <p><b>NOTE: The implementation ships with a default {@link PropertyFileLoginModule} that helps authenticate the request against a
 * property file. {@link PropertyFileLoginModule} is NOT intended to be used in production since the credentials are stored in PLAINTEXT. One can use
 * this extension in production by using their own implementation of {@link javax.security.auth.spi.LoginModule} that authenticates against
 * stores like LDAP, DB, etc.</b>
 */
public class BasicAuthSecurityRestExtension implements ConnectRestExtension {

    private static final Logger log = LoggerFactory.getLogger(BasicAuthSecurityRestExtension.class);

    private static final Supplier<Configuration> CONFIGURATION = initializeConfiguration(Configuration::getConfiguration);

    // Capture the JVM's global JAAS configuration as soon as possible, as it may be altered later
    // by connectors, converters, other REST extensions, etc.
    static Supplier<Configuration> initializeConfiguration(Supplier<Configuration> configurationSupplier) {
        try {
            Configuration configuration = configurationSupplier.get();
            return () -> configuration;
        } catch (Exception e) {
            // We have to be careful not to throw anything here as this static block gets executed during plugin scanning and any exceptions will
            // cause the worker to fail during startup, even if it's not configured to use the basic auth extension.
            return () -> {
                throw new ConnectException("Failed to retrieve JAAS configuration", e);
            };
        }
    }

    private final Supplier<Configuration> configuration;

    public BasicAuthSecurityRestExtension() {
        this(CONFIGURATION);
    }

    // For testing
    BasicAuthSecurityRestExtension(Supplier<Configuration> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        log.trace("Registering JAAS basic auth filter");
        restPluginContext.configurable().register(new JaasBasicAuthFilter(configuration.get()));
        log.trace("Finished registering JAAS basic auth filter");
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // If we failed to retrieve a JAAS configuration during startup, throw that exception now
        configuration.get();
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
