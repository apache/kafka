/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.plain;

import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.SaslPasswordVerifier;

import javax.security.sasl.SaslException;
import java.io.IOException;

/**
 * JAAS based username/password verification for SASL/PLAIN authentication.
 */
public class JaasPlainPasswordVerifier implements SaslPasswordVerifier {

    private static final String JAAS_USER_PREFIX = "user_";

    /**
     * Verifies username and password sent by client against username and password in JAAS config file.
     * @param username Principal name sent by client.
     * @param password Password sent by client.
     * @return True if username and password are valid.
     * @throws SaslException
     */
    @Override
    public boolean verifyPassword(String username, String password) throws SaslException {
        String expectedPassword;
        try {
            expectedPassword = JaasUtils.jaasConfig(LoginType.SERVER.contextName(), JAAS_USER_PREFIX + username);
        } catch (IOException e) {
            throw new SaslException("Authentication failed: Invalid JAAS configuration", e);
        }
        return password.equals(expectedPassword);
    }
}
