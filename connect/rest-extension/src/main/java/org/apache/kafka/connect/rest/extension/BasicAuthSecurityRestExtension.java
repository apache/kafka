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

package org.apache.kafka.connect.rest.extension;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import java.io.IOException;
import java.util.Map;

/**
 * {@link BasicAuthSecurityRestExtension} provides the ability to authenticate incoming BasicAuth
 * credentials using the configured JAAS {@link javax.security.auth.spi.LoginModule}.
 * An entry with the name {@code KafkaConnect} is expected in the JAAS config file configured in
 * the JVM. An implementation of {@link javax.security.auth.spi.LoginModule} needs to be provided
 * in the JAAS config file. The {@code LoginModule} implementation should configure the
 * {@link javax.security.auth.callback.CallbackHandler} with only
 * {@link javax.security.auth.callback.NameCallback} and {@link javax.security.auth.callback.PasswordCallback}
 *
 * <p>To use this extension, one needs to add the following config in the {@code worker.properties}
 * <br>
 * {@code rest.extension.classes = org.apache.kafka.connect.rest.extension.BasicAuthSecurityRestExtension}
 *
 * <p> An example JAAS config would look as below
 * <br>
 * <code>
 *     KafkaConnect {
 *          com.example.PropertyLoginModule required
 *          filename="/mnt/secret/credentials.properties";
 *     };
 * </code>
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
