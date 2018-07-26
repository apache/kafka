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

package org.apache.kafka.connect.rest.basic.auth.extenstion;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import java.io.IOException;
import java.util.Map;

/**
 * Provides the ability to authenticate incoming BasicAuth credentials using the configured JAAS {@link
 * javax.security.auth.spi.LoginModule}. An entry with the name {@code KafkaConnect} is expected in the JAAS config file configured in the
 * JVM. An implementation of {@link javax.security.auth.spi.LoginModule} needs to be provided in the JAAS config file. The {@code
 * LoginModule} implementation should configure the {@link javax.security.auth.callback.CallbackHandler} with only {@link
 * javax.security.auth.callback.NameCallback} and {@link javax.security.auth.callback.PasswordCallback}.
 *
 * <p>To use this extension, one needs to add the following config in the {@code worker.properties}
 * <pre>
 *     rest.extension.classes = org.apache.kafka.connect.rest.basic.auth.extenstion.BasicAuthSecurityRestExtension
 * </pre>
 *
 * <p> An example JAAS config would look as below
 * <Pre>
 *         KafkaConnect {
 *              org.apache.kafka.connect.rest.basic.auth.extenstion.PropertyFileLoginModule required
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

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        restPluginContext.configurable().register(JaasBasicAuthFilter.class);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
