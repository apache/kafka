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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import javax.net.ssl.SSLContext;

/**
 * {@code SslResource} couples the {@link SslFactory} and {@link SSLContext} so that
 * {@link #sslFactory} can be properly {@link SslFactory#close() closed} during closing of the overall
 * OAuth login/validation module. The {@link SSLContext} API is what the HTTP clients use, so the two
 * need to be kept closely together.
 */
public class SslResource implements Closeable {

    private final SslFactory sslFactory;

    private final SSLContext sslContext;

    public SslResource(SslFactory sslFactory, SSLContext sslContext) {
        this.sslFactory = sslFactory;
        this.sslContext = sslContext;
    }

    public static SslResource create(Map<String, ?> configs) {
        SslFactory sslFactory = new SslFactory(ConnectionMode.CLIENT);
        sslFactory.configure(configs);

        if (!((sslFactory.sslEngineFactory()) instanceof DefaultSslEngineFactory)) {
            String message = String.format(
                "The OAuth %s configuration includes a custom SSL factory class (%s) which is not a supported JAAS option for OAuth",
                SaslConfigs.SASL_JAAS_CONFIG,
                SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG
            );
            throw new ConfigException(message);
        }

        SSLContext sslContext = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext();
        return new SslResource(sslFactory, sslContext);
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(sslFactory, "sslFactory");
    }
}
