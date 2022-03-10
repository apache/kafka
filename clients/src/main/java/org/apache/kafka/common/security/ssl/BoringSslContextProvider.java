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

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.config.SslConfigs;
import org.conscrypt.Conscrypt;

/**
 * A BoringSslContext provider based on Conscrypt library.
 */
public class BoringSslContextProvider implements SslContextProvider {
    private String protocol;
    private Provider provider;

    @Override
    public void configure(Map<String, ?> configs) {
        protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        provider = Conscrypt.newProvider();
    }

    @Override
    public SSLContext getSSLContext()  throws NoSuchAlgorithmException {
        return SSLContext.getInstance(protocol, provider);
    }
}
