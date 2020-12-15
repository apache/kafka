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

package org.apache.kafka.common.security.plain;

import javax.security.auth.callback.Callback;

/*
 * Authentication callback for SASL/PLAIN authentication. Callback handler must
 * set authenticated flag to true if the client provided password in the callback
 * matches the expected password.
 */
public class PlainAuthenticateCallback implements Callback {
    private final char[] password;
    private boolean authenticated;

    /**
     * Creates a callback with the password provided by the client
     * @param password The password provided by the client during SASL/PLAIN authentication
     */
    public PlainAuthenticateCallback(char[] password) {
        this.password = password;
    }

    /**
     * Returns the password provided by the client during SASL/PLAIN authentication
     */
    public char[] password() {
        return password;
    }

    /**
     * Returns true if client password matches expected password, false otherwise.
     * This state is set the server-side callback handler.
     */
    public boolean authenticated() {
        return this.authenticated;
    }

    /**
     * Sets the authenticated state. This is set by the server-side callback handler
     * by matching the client provided password with the expected password.
     *
     * @param authenticated true indicates successful authentication
     */
    public void authenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
}
