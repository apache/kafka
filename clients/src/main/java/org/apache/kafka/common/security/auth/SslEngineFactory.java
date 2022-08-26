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
 * Plugin interface for allowing creation of <code>SSLEngine</code> object in a custom way.
 * For example, you can use this to customize loading your key material and trust material needed for <code>SSLContext</code>.
 * This is complementary to the existing Java Security Provider mechanism which allows the entire provider
 * to be replaced with a custom provider. In scenarios where only the configuration mechanism for SSL engines
 * need to be updated, this interface provides a convenient method for overriding the default implementation.
 */
public interface SslEngineFactory extends Configurable, Closeable {

    /**
     * Creates a new <code>SSLEngine</code> object to be used by the client.
     *
     * @param peerHost               The peer host to use. This is used in client mode if endpoint validation is enabled.
     * @param peerPort               The peer port to use. This is a hint and not used for validation.
     * @param endpointIdentification Endpoint identification algorithm for client mode.
     * @return The new <code>SSLEngine</code>.
     */
    SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification);

    /**
     * Creates a new <code>SSLEngine</code> object to be used by the server.
     *
     * @param peerHost               The peer host to use. This is a hint and not used for validation.
     * @param peerPort               The peer port to use. This is a hint and not used for validation.
     * @return The new <code>SSLEngine</code>.
     */
    SSLEngine createServerSslEngine(String peerHost, int peerPort);

    /**
     * Returns true if <code>SSLEngine</code> needs to be rebuilt. This method will be called when reconfiguration is triggered on
     * the <code>SslFactory</code> used to create SSL engines. Based on the new configs provided in <i>nextConfigs</i>, this method
     * will decide whether underlying <code>SSLEngine</code> object needs to be rebuilt. If this method returns true, the
     * <code>SslFactory</code> will create a new instance of this object with <i>nextConfigs</i> and run other
     * checks before deciding to use the new object for <i>new incoming connection</i> requests. Existing connections
     * are not impacted by this and will not see any changes done as part of reconfiguration.
     * <p>
     * For example, if the implementation depends on file-based key material, it can check if the file was updated
     * compared to the previous/last-loaded timestamp and return true.
     * </p>
     *
     * @param nextConfigs       The new configuration we want to use.
     * @return                  True only if the underlying <code>SSLEngine</code> object should be rebuilt.
     */
    boolean shouldBeRebuilt(Map<String, Object> nextConfigs);

    /**
     * Returns the names of configs that may be reconfigured.
     * @return Names of configuration options that are dynamically reconfigurable.
     */
    Set<String> reconfigurableConfigs();

    /**
     * Returns keystore configured for this factory.
     * @return The keystore for this factory or null if a keystore is not configured.
     */
    KeyStore keystore();

    /**
     * Returns truststore configured for this factory.
     * @return The truststore for this factory or null if a truststore is not configured.
     */
    KeyStore truststore();
}