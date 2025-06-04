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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * {@code AssertionCreator} is used to create a client-signed OAuth assertion that can be used with different
 * grant types. See <a href="https://datatracker.ietf.org/doc/html/rfc7521">RFC 7521</a> for specifics.
 *
 * <p/>
 *
 * The assertion creator has three main steps:
 *
 * <ol>
 *     <li>Create the JWT header</li>
 *     <li>Create the JWT payload</li>
 *     <li>Sign</li>
 * </ol>
 *
 * <p/>
 *
 * Step 1 is to dynamically create the JWT header. The implementation may add whatever values it needs, but
 * the {@code alg} (algorithm), {@code kid} (key ID), and {@code type} (type) are usually present. Here is
 * an example of the JSON version of the JWT header:
 *
 * <pre>
 * {
 *   "kid": "9d82418e64e0541066637ca8592d459c",
 *   "alg": RS256,
 *   "typ": "JWT",
 * }
 * </pre>
 *
 * <p/>
 *
 * Step 2 is to create the JWT payload from the claims provided to {@link #create(AssertionJwtTemplate)}. Depending on the
 * implementation, other claims may be dynamically generated and added to the JWT payload. Or, some of the
 * claims in the incoming map could be ignored or modified. Here's an example where the implementation has
 * added the {@code iat} (initialized at) and {@code exp} (expires) claims:
 *
 * <pre>
 * {
 *   "iat": 1741121401,
 *   "exp": 1741125001,
 *   "sub": "some-service-account",
 *   "aud": "my_audience",
 *   "iss": "https://example.com",
 *   "...": "...",
 * }
 * </pre>
 *
 * <p/>
 *
 * Step 3 is to use the configured private key to sign the header and payload and serialize in the compact
 * JWT format. The means by which the private key (if any) is made available for use is up to the
 * implementation. The private key could be loaded from a file, downloaded from a trusted resource,
 * embedded in the configuration, etc.
 */
public interface AssertionCreator extends Closeable {

    /**
     * Creates and signs an OAuth assertion by converting the given claims into JWT and then signing it using
     * the configured algorithm.
     *
     * <p/>
     *
     * @param template {@link AssertionJwtTemplate} with optional header and/or claims to include in the JWT
     */
    String create(AssertionJwtTemplate template) throws GeneralSecurityException, IOException;

    /**
     * Closes any resources used by this implementation. The default implementation of
     * this method is a no op, for convenience to implementors.
     */
    @Override
    default void close() throws IOException {
        // Do nothing...
    }
}
