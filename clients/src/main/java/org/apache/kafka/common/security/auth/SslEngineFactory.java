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
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.Configurable;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.security.KeyStore;
import java.util.Map;
import java.util.Set;

/**
 * Plugin interface for allowing creation of SSLEngine object in a custom way.
 * Example: You want to use custom way to load your key material and trust material needed for SSLContext.
 * However, keep in mind that this is complementary to the existing Java Security Provider's mechanism and not a competing
 * solution.
 */
public interface SslEngineFactory extends Configurable, Closeable {

    /**
     * Create a new SSLEngine object to be used by the client.
     *
     * @param peerHost               The peer host to use. This is used in client mode if endpoint validation is enabled.
     * @param peerPort               The peer port to use. This is a hint and not used for validation.
     * @param endpointIdentification Endpoint identification algorithm for client mode.
     * @return The new SSLEngine.
     */
    SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification);

    /**
     * Create a new SSLEngine object to be used by the server.
     *
     * @param peerHost               The peer host to use. This is a hint and not used for validation.
     * @param peerPort               The peer port to use. This is a hint and not used for validation.
     * @return The new SSLEngine.
     */
    SSLEngine createServerSslEngine(String peerHost, int peerPort);

    /**
     * Returns true if SSLEngine needs to be rebuilt. This method will be called when reconfiguration is triggered on
     * {@link org.apache.kafka.common.security.ssl.SslFactory}. Based on the <i>nextConfigs</i>, this method will
     * decide whether underlying SSLEngine object needs to be rebuilt. If this method returns true, the
     * {@link org.apache.kafka.common.security.ssl.SslFactory} will re-create instance of this object and run other
     * checks before deciding to use the new object for the <i>new incoming connection</i> requests.The existing connections
     * are not impacted by this and will not see any changes done as part of reconfiguration.
     *
     * <pre>
     *     Example: If the implementation depends on the file based key material it can check if the file is updated
     *     compared to the previous/last-loaded timestamp and return true.
     * </pre>
     *
     * @param nextConfigs       The configuration we want to use.
     * @return                  True only if the underlying SSLEngine object should be rebuilt.
     */
    boolean shouldBeRebuilt(Map<String, Object> nextConfigs);

    /**
     * Returns the names of configs that may be reconfigured.
     */
    Set<String> reconfigurableConfigs();

    /**
     * Returns keystore.
     * @return
     */
    KeyStore keystore();

    /**
     * Returns truststore.
     * @return
     */
    KeyStore truststore();
}