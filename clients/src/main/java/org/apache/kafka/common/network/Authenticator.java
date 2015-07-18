/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;

/**
 * Authentication for Channel
 */

import java.io.IOException;
import java.security.Principal;

import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.KafkaException;

public interface Authenticator {

    /**
     * configures Authenticator using principalbuilder and transportLayer.
     * @param TransportLayer transportLayer
     * @param PrincipalBuilder principalBuilder
     */
    void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder);

    /**
     * Implements any authentication mechanism. Use transportLayer to read or write tokens.
     * If no further authentication needs to be done returns.
     */
    void authenticate() throws IOException;

    /**
     * Returns Principal using PrincipalBuilder
     */
    Principal principal() throws KafkaException;

    /**
     * returns true if authentication is complete otherwise returns false;
     */
    boolean complete();

    /**
     * Closes this Authenticator
     *
     * @throws IOException if any I/O error occurs
     */
    void close() throws IOException;

}
