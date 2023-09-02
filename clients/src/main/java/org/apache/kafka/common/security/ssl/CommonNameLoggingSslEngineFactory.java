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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class CommonNameLoggingSslEngineFactory extends DefaultSslEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(CommonNameLoggingSslEngineFactory.class);

    @Override
    protected SSLContext createSSLContext(SecurityStore keystore, SecurityStore truststore) {
        try {
            SSLContext sslContext;
            if (provider != null)
                sslContext = SSLContext.getInstance(protocol, provider);
            else
                sslContext = SSLContext.getInstance(protocol);

            KeyManager[] keyManagers = null;
            if (keystore != null || kmfAlgorithm != null) {
                String kmfAlgorithm = this.kmfAlgorithm != null ?
                        this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
                if (keystore != null) {
                    kmf.init(keystore.get(), keystore.keyPassword());
                } else {
                    kmf.init(null, null);
                }
                keyManagers = kmf.getKeyManagers();
            }

            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            CommonNameLoggingTrustManagerFactoryWrapper tmf = CommonNameLoggingTrustManagerFactoryWrapper.getInstance(tmfAlgorithm);
            KeyStore ts = truststore == null ? null : truststore.get();
            tmf.init(ts);

            sslContext.init(keyManagers, tmf.getTrustManagers(), this.secureRandomImplementation);
            log.debug("Created SSL context with keystore {}, truststore {}, provider {}.",
                    keystore, truststore, sslContext.getProvider().getName());
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }
}
